"""Tests for priority ordering, conservative preemption and fair rebalancing."""

import shutil
import tempfile
import unittest

from backend.common import constants as C
from backend.common.config import ClusterConfig
from backend.common.logbus import LogBus
from backend.common.storage import Storage
from backend.master.fault_tolerance import FaultTolerance
from backend.master.job_manager import JobManager
from backend.master.metrics import Metrics
from backend.master.registry import WorkerRegistry
from backend.master.scheduler import Scheduler
from backend.master.shuffle import ShuffleCoordinator


class FakeResponse:
    def __init__(self, ok=True, data=None):
        self.ok = ok
        self.data = data or {}


class FakeHttpClient:
    def __init__(self):
        self.cancelled = []

    def post(self, url, payload, timeout=3.0):
        if url.endswith("/task/cancel"):
            self.cancelled.append(payload)
            return FakeResponse(True, {"cancelled": True})
        if url.endswith("/task/execute"):
            return FakeResponse(True, {"accepted": True})
        return FakeResponse(False)


class PrioritySchedulerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.storage = Storage(self.tmp)
        self.config = ClusterConfig(
            preemption_grace_sec=0.0,
            preemption_min_progress=1.0,
            priority_aging_sec=0.0,
            fair_preemption_grace_sec=0.0,
        )
        self.logbus = LogBus(self.storage)
        self.jm = JobManager(self.storage, self.config, self.logbus)
        self.registry = WorkerRegistry(self.storage, self.config)
        for idx in range(2):
            wid = f"worker-{idx}"
            self.registry.register({
                "worker_id": wid,
                "name": wid,
                "host": "127.0.0.1",
                "port": 9000 + idx,
                "cpu_cores": 1,
                "mem_total_mb": 1024,
                "exec_mode": "thread",
            })
        self.shuffle = ShuffleCoordinator(self.storage, self.jm, self.registry, self.logbus)
        self.fault = FaultTolerance(self.storage, self.jm, self.config, self.logbus)
        self.scheduler = Scheduler(
            self.storage, self.jm, self.registry, self.shuffle, self.fault,
            Metrics(self.storage), self.config, self.logbus,
        )
        self.client = FakeHttpClient()
        self.scheduler.client = self.client

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _submit(self, name, priority, tasks=2):
        return self.jm.submit({
            "name": name,
            "mapper": "wordcount_mapper",
            "reducer": "count_reducer",
            "num_map_tasks": tasks,
            "num_reduce_tasks": 1,
            "input_rows": max(20, tasks * 20),
            "priority": priority,
            "params": {},
        })

    def _active_tasks(self, job_id):
        return [t for t in self.jm.tasks_for(job_id, C.TASK_MAP)
                if t.status in (C.TASK_ASSIGNED, C.TASK_RUNNING)]

    def test_high_priority_preempts_only_required_low_priority_slot(self):
        low = self._submit("low", C.PRIORITY_LOW, tasks=2)
        self.scheduler.tick()
        self.assertEqual(len(self._active_tasks(low.job_id)), 2)

        high = self._submit("high", C.PRIORITY_HIGH, tasks=1)
        self.scheduler.tick()

        # Exactly one lower-priority task was stopped, not both.
        self.assertEqual(len(self.client.cancelled), 1)
        low_tasks = self.jm.tasks_for(low.job_id, C.TASK_MAP)
        self.assertEqual(sum(t.status == C.TASK_CANCELING for t in low_tasks), 1)
        self.assertEqual(len(self._active_tasks(low.job_id)), 1)

        canceled = next(t for t in low_tasks if t.status == C.TASK_CANCELING)
        # The worker's terminal report releases the draining reservation and
        # immediately admits the high-priority task.
        self.scheduler.on_task_complete({
            "job_id": low.job_id,
            "task_id": canceled.task_id,
            "worker_id": canceled.stats.get("last_preempted_worker", ""),
            "status": C.TASK_CANCELLED,
            "dispatch_token": canceled.dispatch_token - 1,
        })
        self.assertEqual(len(self._active_tasks(high.job_id)), 1)
        self.assertEqual(len(self._active_tasks(low.job_id)), 1)

    def test_same_priority_jobs_are_fairly_rebalanced(self):
        first = self._submit("first", C.PRIORITY_NORMAL, tasks=2)
        self.scheduler.tick()
        self.assertEqual(len(self._active_tasks(first.job_id)), 2)

        second = self._submit("second", C.PRIORITY_NORMAL, tasks=1)
        self.scheduler.tick()
        self.assertEqual(len(self.client.cancelled), 1)

        tasks = self.jm.tasks_for(first.job_id, C.TASK_MAP)
        canceled = next(t for t in tasks if t.status == C.TASK_CANCELING)
        self.scheduler.on_task_complete({
            "job_id": first.job_id,
            "task_id": canceled.task_id,
            "worker_id": canceled.stats.get("last_preempted_worker", ""),
            "status": C.TASK_CANCELLED,
            "dispatch_token": canceled.dispatch_token - 1,
        })
        self.assertEqual(len(self._active_tasks(first.job_id)), 1)
        self.assertEqual(len(self._active_tasks(second.job_id)), 1)

    def test_waiting_job_aging_raises_effective_priority(self):
        self.config.priority_preemption = False
        self.config.priority_aging_sec = 60.0
        low = self._submit("low", C.PRIORITY_LOW, tasks=2)
        self.scheduler.tick()
        waiter = self._submit("aging", C.PRIORITY_LOW, tasks=1)
        waiter.waiting_since_ms -= 61_000
        self.jm.save_job(waiter)
        self.scheduler.tick()
        refreshed = self.jm.get_job(waiter.job_id)
        self.assertEqual(refreshed.priority, C.PRIORITY_LOW)
        self.assertEqual(refreshed.effective_priority, 2)

    def test_disabled_preemption_leaves_lower_priority_running(self):
        self.config.priority_preemption = False
        low = self._submit("low", C.PRIORITY_LOW, tasks=2)
        self.scheduler.tick()
        high = self._submit("high", C.PRIORITY_HIGH, tasks=1)
        self.scheduler.tick()
        self.assertEqual(len(self._active_tasks(low.job_id)), 2)
        self.assertEqual(len(self._active_tasks(high.job_id)), 0)


if __name__ == "__main__":
    unittest.main()
