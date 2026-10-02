"""Tests for priority scheduling: fair shares, preemption, aging, retries."""

import shutil
import tempfile
import unittest

from backend.common import constants as C
from backend.common.config import ClusterConfig
from backend.common.jsonutil import now_ms
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master.fault_tolerance import FaultTolerance
from backend.master.job_manager import JobManager
from backend.master.metrics import Metrics
from backend.master.registry import WorkerRegistry
from backend.master.scheduler import Scheduler, fair_shares
from backend.master.shuffle import ShuffleCoordinator


class FakeResponse:
    def __init__(self, ok=True, data=None):
        self.ok = ok
        self.data = data or {}


class FakeClient:
    """Stands in for the worker-facing HTTP client; accepts every dispatch."""

    def __init__(self, accept=True):
        self.accept = accept
        self.posts = []

    def post(self, url, payload, timeout=3.0):
        self.posts.append((url, payload))
        if "/task/execute" in url:
            return FakeResponse(True, {"accepted": self.accept})
        return FakeResponse(True, {})


class TestFairShares(unittest.TestCase):
    def test_equal_split(self):
        self.assertEqual(fair_shares({"a": 10, "b": 10}, 10), {"a": 5, "b": 5})

    def test_demand_capped_and_redistributed(self):
        self.assertEqual(fair_shares({"a": 2, "b": 10}, 10), {"a": 2, "b": 8})

    def test_remainder_is_deterministic(self):
        shares = fair_shares({"a": 5, "b": 5, "c": 5}, 4)
        self.assertEqual(shares, {"a": 2, "b": 1, "c": 1})
        self.assertEqual(sum(shares.values()), 4)

    def test_zero_capacity(self):
        self.assertEqual(fair_shares({"a": 3, "b": 3}, 0), {"a": 0, "b": 0})

    def test_zero_demand_excluded(self):
        self.assertEqual(fair_shares({"a": 0, "b": 4}, 2), {"a": 0, "b": 2})


class PriorityClusterTestCase(unittest.TestCase):
    """A Master with fake workers and a fake dispatch client (no real HTTP)."""

    workers = 1
    cores = 2
    config_kwargs: dict = {}

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.config = ClusterConfig(**self.config_kwargs)
        self.logbus = LogBus(self.storage)
        self.jm = JobManager(self.storage, self.config, self.logbus)
        self.registry = WorkerRegistry(self.storage, self.config)
        self.metrics = Metrics(self.storage)
        self.shuffle = ShuffleCoordinator(self.storage, self.jm, self.registry, self.logbus)
        self.ft = FaultTolerance(self.storage, self.jm, self.config, self.logbus)
        self.sched = Scheduler(
            self.storage, self.jm, self.registry, self.shuffle,
            self.ft, self.metrics, self.config, self.logbus,
        )
        self.client = FakeClient()
        self.sched.client = self.client
        for i in range(self.workers):
            self.add_worker(f"w{i}", cores=self.cores)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- helpers ---------------------------------------------------------
    def add_worker(self, worker_id, cores=2):
        self.registry.register({
            "worker_id": worker_id, "name": worker_id, "host": "127.0.0.1",
            "port": 9000 + len(self.registry.all()), "cpu_cores": cores,
            "mem_total_mb": 1024,
        })

    def submit(self, name="job", priority=5, num_map=4, num_reduce=1):
        return self.jm.submit({
            "name": name, "mapper": "wordcount_mapper", "reducer": "count_reducer",
            "num_map_tasks": num_map, "num_reduce_tasks": num_reduce,
            "input_rows": 200, "priority": priority, "params": {},
        })

    def assigned(self, job, kind="map"):
        return [t for t in self.jm.tasks_for(job.job_id, kind)
                if t.status in (C.TASK_ASSIGNED, C.TASK_RUNNING)]

    def pending(self, job, kind="map"):
        return [t for t in self.jm.tasks_for(job.job_id, kind)
                if t.status == C.TASK_PENDING]


class TestPriorityDispatch(PriorityClusterTestCase):
    def test_high_priority_gets_slots_first(self):
        low = self.submit("low", priority=3, num_map=4)
        high = self.submit("high", priority=8, num_map=2)
        self.sched.tick()
        # Capacity is 2 slots; the high-priority job takes both.
        self.assertEqual(len(self.assigned(high)), 2)
        self.assertEqual(len(self.assigned(low)), 0)
        self.assertEqual(len(self.pending(low)), 4)

    def test_same_priority_splits_capacity_fairly(self):
        a = self.submit("a", priority=5, num_map=4)
        b = self.submit("b", priority=5, num_map=4)
        self.sched.tick()
        self.assertEqual(len(self.assigned(a)), 1)
        self.assertEqual(len(self.assigned(b)), 1)

    def test_capacity_follows_worker_registration(self):
        job = self.submit("j", priority=5, num_map=4)
        self.sched.tick()
        self.assertEqual(len(self.assigned(job)), 2)
        # A new node joins -> its slots are used on the next tick.
        self.add_worker("w-extra", cores=2)
        self.sched.tick()
        self.assertEqual(len(self.assigned(job)), 4)

    def test_submit_clamps_priority(self):
        job = self.submit("j", priority=99)
        self.assertEqual(job.priority, C.PRIORITY_MAX)
        job2 = self.submit("j2", priority=-5)
        self.assertEqual(job2.priority, C.PRIORITY_MIN)


class TestPreemption(PriorityClusterTestCase):
    def test_high_priority_preempts_running_low_priority(self):
        low = self.submit("low", priority=3, num_map=2)
        self.sched.tick()
        self.assertEqual(len(self.assigned(low)), 2)

        high = self.submit("high", priority=9, num_map=2)
        self.sched.tick()  # no free slots -> preemption pass
        low_tasks = self.jm.tasks_for(low.job_id, "map")
        self.assertTrue(all(t.status == C.TASK_RETRYING for t in low_tasks))
        # Preemption is not a failure: retry budget untouched, eviction counted.
        self.assertTrue(all(t.attempts == 0 for t in low_tasks))
        self.assertTrue(all((t.stats or {}).get("preemptions") == 1 for t in low_tasks))
        self.assertTrue(all(t.retry_after_ms == 0 for t in low_tasks))
        faults = self.ft.list_faults(low.job_id)
        self.assertEqual(len([f for f in faults if f["kind"] == "preempted"]), 2)

        # Next tick: the freed slots go to the high-priority job.
        self.sched.tick()
        self.assertEqual(len(self.assigned(high)), 2)
        self.assertEqual(len(self.assigned(low)), 0)

    def test_preemption_skips_nearly_finished_tasks(self):
        low = self.submit("low", priority=3, num_map=2)
        self.sched.tick()
        # One task is 90% done (above the 0.8 guard) and must not be preempted.
        almost_done = self.assigned(low)[0]
        self.jm.update_task(low.job_id, almost_done.task_id,
                            status=C.TASK_RUNNING, progress=0.9)
        self.submit("high", priority=9, num_map=2)
        self.sched.tick()
        kept = self.jm.get_task(low.job_id, almost_done.task_id)
        self.assertEqual(kept.status, C.TASK_RUNNING)
        victim = [t for t in self.jm.tasks_for(low.job_id, "map")
                  if t.task_id != almost_done.task_id][0]
        self.assertEqual(victim.status, C.TASK_RETRYING)

    def test_preemption_never_hits_same_or_higher_priority(self):
        high = self.submit("high", priority=9, num_map=2)
        self.sched.tick()
        self.assertEqual(len(self.assigned(high)), 2)
        self.submit("mid", priority=5, num_map=2)
        self.sched.tick()
        # The mid-priority job waits; the high-priority job keeps its slots.
        self.assertEqual(len(self.assigned(high)), 2)
        self.assertTrue(all(t.status != C.TASK_RETRYING
                            for t in self.jm.tasks_for(high.job_id, "map")))

    def test_preemption_disabled_by_config(self):
        self.config.preemption_enabled = False
        low = self.submit("low", priority=3, num_map=2)
        self.sched.tick()
        self.submit("high", priority=9, num_map=2)
        self.sched.tick()
        self.assertEqual(len(self.assigned(low)), 2)
        self.assertEqual(self.ft.list_faults(low.job_id), [])

    def test_late_failure_report_from_preempted_task_is_ignored(self):
        low = self.submit("low", priority=3, num_map=2)
        self.sched.tick()
        self.submit("high", priority=9, num_map=2)
        self.sched.tick()  # preempts both low tasks
        task = self.jm.tasks_for(low.job_id, "map")[0]
        self.assertEqual(task.status, C.TASK_RETRYING)
        # The worker's "cancelled" failure report arrives late.
        self.sched.on_task_complete({
            "job_id": low.job_id, "task_id": task.task_id,
            "worker_id": "w0", "status": C.TASK_FAILED, "error": "cancelled",
        })
        task = self.jm.get_task(low.job_id, task.task_id)
        self.assertEqual(task.status, C.TASK_RETRYING)
        self.assertEqual(task.attempts, 0)  # retry budget not consumed


class TestPreemptionBudget(PriorityClusterTestCase):
    workers = 1
    cores = 3
    config_kwargs = {"preemption_max_per_tick": 1}

    def test_per_tick_limit_bounds_churn(self):
        low = self.submit("low", priority=3, num_map=3)
        self.sched.tick()
        self.assertEqual(len(self.assigned(low)), 3)
        self.submit("high", priority=9, num_map=3)
        self.sched.tick()
        preempted = [t for t in self.jm.tasks_for(low.job_id, "map")
                     if t.status == C.TASK_RETRYING]
        self.assertEqual(len(preempted), 1)  # bounded by preemption_max_per_tick


class TestAging(PriorityClusterTestCase):
    config_kwargs = {"priority_aging_sec": 10.0}

    def test_waiting_job_gains_effective_priority(self):
        low = self.submit("low", priority=3, num_map=2)
        high = self.submit("high", priority=9, num_map=2)
        self.sched.tick()  # high takes both slots; low starts waiting
        self.assertEqual(len(self.assigned(low)), 0)
        job = self.jm.get_job(low.job_id)
        self.assertTrue(job.stats.get("waiting_since_ms"))

        # Simulate 35s of starvation -> boost of 3 -> effective 6.
        self.jm.apply_job(low.job_id, lambda j: j.stats.__setitem__(
            "waiting_since_ms", now_ms() - 35000))
        # Effective priority is recomputed from the wait clock every tick.
        eff = self.sched._effective_priority(self.jm.get_job(low.job_id), now_ms())
        self.assertEqual(eff, 6)

    def test_aging_eventually_beats_higher_priority(self):
        high = self.submit("high", priority=9, num_map=2)
        self.sched.tick()
        self.assertEqual(len(self.assigned(high)), 2)
        low = self.submit("low", priority=3, num_map=2)
        # Low job has been starved "forever" -> effective priority caps at 10.
        self.jm.apply_job(low.job_id, lambda j: j.stats.__setitem__(
            "waiting_since_ms", now_ms() - 10_000_000))
        self.sched.tick()  # aged low job now outranks and preempts the high job
        high_tasks = self.jm.tasks_for(high.job_id, "map")
        self.assertTrue(any(t.status == C.TASK_RETRYING for t in high_tasks))
        queue = {e["job_id"]: e for e in self.sched.queue_status()["jobs"]}
        self.assertEqual(queue[low.job_id]["effective_priority"], C.PRIORITY_MAX)

    def test_wait_clock_clears_once_served(self):
        low = self.submit("low", priority=3, num_map=2)
        high = self.submit("high", priority=9, num_map=2)
        self.sched.tick()
        self.assertTrue(self.jm.get_job(low.job_id).stats.get("waiting_since_ms"))
        # High job finishes -> its tasks succeed -> low job gets the slots.
        for t in self.jm.tasks_for(high.job_id, "map"):
            self.sched.on_task_complete({
                "job_id": high.job_id, "task_id": t.task_id, "worker_id": "w0",
                "status": C.TASK_SUCCEEDED, "records_processed": 1,
                "records_emitted": 1, "duration_ms": 1,
            })
        self.sched.tick()
        self.assertEqual(len(self.assigned(low)), 2)
        self.assertFalse(self.jm.get_job(low.job_id).stats.get("waiting_since_ms"))


class TestRetriesAndPriorityChange(PriorityClusterTestCase):
    def test_failed_task_rejoins_queue_after_backoff(self):
        job = self.submit("j", priority=5, num_map=2)
        self.sched.tick()
        task = self.assigned(job)[0]
        self.ft.handle_task_failure(job, task, "boom", "w0")
        task = self.jm.get_task(job.job_id, task.task_id)
        self.assertEqual(task.status, C.TASK_RETRYING)
        self.assertGreater(task.retry_after_ms, now_ms())
        # Backoff not elapsed -> the slot stays free, task not redispatched.
        self.sched.tick()
        task = self.jm.get_task(job.job_id, task.task_id)
        self.assertEqual(task.status, C.TASK_RETRYING)
        # Backoff elapsed -> rejoins the queue at the job's priority.
        self.jm.update_task(job.job_id, task.task_id, retry_after_ms=0)
        self.sched.tick()
        task = self.jm.get_task(job.job_id, task.task_id)
        self.assertEqual(task.status, C.TASK_ASSIGNED)

    def test_priority_change_takes_effect_next_tick(self):
        low = self.submit("low", priority=3, num_map=2)
        high = self.submit("high", priority=9, num_map=2)
        self.sched.tick()
        self.assertEqual(len(self.assigned(high)), 2)
        # Demote the running job below the waiting one -> it becomes the victim.
        self.jm.set_priority(self.jm.get_job(high.job_id), 1)
        self.sched.tick()
        high_tasks = self.jm.tasks_for(high.job_id, "map")
        self.assertTrue(any(t.status == C.TASK_RETRYING for t in high_tasks))

    def test_set_priority_clamps_and_resets_wait_clock(self):
        job = self.submit("j", priority=5, num_map=2)
        self.jm.apply_job(job.job_id, lambda j: j.stats.__setitem__(
            "waiting_since_ms", now_ms() - 5000))
        self.jm.set_priority(self.jm.get_job(job.job_id), 99)
        job = self.jm.get_job(job.job_id)
        self.assertEqual(job.priority, C.PRIORITY_MAX)
        self.assertFalse(job.stats.get("waiting_since_ms"))


class TestQueueStatus(PriorityClusterTestCase):
    def test_queue_view_orders_by_priority_and_reports_state(self):
        low = self.submit("low", priority=3, num_map=4)
        high = self.submit("high", priority=8, num_map=2)
        self.sched.tick()
        queue = self.sched.queue_status()
        self.assertEqual(queue["capacity"], 2)
        self.assertEqual(queue["free_slots"], 0)
        entries = queue["jobs"]
        self.assertEqual(entries[0]["job_id"], high.job_id)  # highest priority first
        self.assertEqual(entries[0]["queue_state"], C.QUEUE_RUNNING)
        self.assertEqual(entries[0]["queue_position"], 1)
        self.assertEqual(entries[1]["job_id"], low.job_id)
        self.assertEqual(entries[1]["queue_state"], C.QUEUE_QUEUED)
        self.assertEqual(entries[1]["pending_tasks"], 4)
        self.assertEqual(entries[1]["fair_share"], 0)


class TestFullLifecycle(PriorityClusterTestCase):
    """Drive two priority-differentiated jobs to completion through the
    scheduler, simulating workers by completing whatever gets dispatched."""

    workers = 2
    cores = 2  # 4 slots total

    def setUp(self):
        super().setUp()
        # The shuffle stage normally lingers for one observable beat; collapse
        # it so the lifecycle runs in milliseconds.
        import backend.master.scheduler as sched_mod
        self._hold = sched_mod.SHUFFLE_HOLD_MS
        sched_mod.SHUFFLE_HOLD_MS = 0

    def tearDown(self):
        import backend.master.scheduler as sched_mod
        sched_mod.SHUFFLE_HOLD_MS = self._hold
        super().tearDown()

    def _complete_assigned(self):
        for job in self.jm.list_jobs():
            if job.is_terminal:
                continue
            for t in self.jm.tasks_for(job.job_id):
                if t.status == C.TASK_ASSIGNED:
                    self.sched.on_task_complete({
                        "job_id": job.job_id, "task_id": t.task_id,
                        "worker_id": t.worker_id or "w0",
                        "status": C.TASK_SUCCEEDED,
                        "records_processed": 10, "records_emitted": 5,
                        "duration_ms": 1, "results": [],
                    })

    def test_two_jobs_run_to_success_with_priority_order(self):
        low = self.submit("low", priority=3, num_map=4, num_reduce=2)
        high = self.submit("high", priority=8, num_map=2, num_reduce=1)

        first_dispatch_order = []
        for _ in range(60):
            self.sched.tick()
            for url, payload in self.client.posts:
                if "/task/execute" in url:
                    first_dispatch_order.append(payload["job_id"])
            self.client.posts.clear()
            self._complete_assigned()
            if (self.jm.get_job(low.job_id).is_terminal
                    and self.jm.get_job(high.job_id).is_terminal):
                break

        self.assertEqual(self.jm.get_job(high.job_id).status, C.JOB_SUCCEEDED)
        self.assertEqual(self.jm.get_job(low.job_id).status, C.JOB_SUCCEEDED)
        # The high-priority job was dispatched first and finished first.
        self.assertEqual(first_dispatch_order[0], high.job_id)
        self.assertLessEqual(
            self.jm.get_job(high.job_id).finished_ms,
            self.jm.get_job(low.job_id).finished_ms,
        )
        # Queue empties out once everything is done.
        self.assertEqual(self.sched.queue_status()["jobs"], [])


if __name__ == "__main__":
    unittest.main()
