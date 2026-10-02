"""The Master scheduler: priority dispatch, fair sharing, preemption, speculation.

A single background thread runs a ``tick`` loop that:

1. reaps workers whose heartbeat timed out (delegating reassignment to
   ``FaultTolerance``);
2. advances each job's stage state machine (MAP -> SHUFFLE -> REDUCE);
3. runs one **global dispatch pass** across all active jobs:
   jobs are grouped into priority bands (highest first), free worker slots are
   split *max-min fairly* between jobs of the same band, and — when a band's
   demand cannot be satisfied — running tasks of strictly-lower-priority jobs
   are preempted under an explicit, bounded rule;
4. checks for stragglers and launches speculative duplicates.

Fairness and predictability rules (all recomputed every tick, so priority
edits, node joins/leaves and retries are picked up automatically):

* **Priority** — a higher-priority job is always served before a lower one;
  leftover capacity flows down to the next band.
* **Fair share** — within one band, capacity to each job is allocated max-min
  fairly (``fair_shares``): equal shares, capped at demand, leftovers
  redistributed.  No same-priority job can starve another.
* **Aging** — a job that has runnable work but gets zero slots for
  ``priority_aging_sec`` gains +1 effective priority per interval, so
  low-priority jobs cannot starve under a constant high-priority load.
* **Preemption** — only when a band still has waiting tasks and no free
  capacity: cancel running tasks of *strictly lower* priority jobs, least
  progress first, never a task at/above ``preemption_guard_progress``, at most
  ``preemption_max_per_tick`` per tick.  A preempted task is re-queued
  immediately and does **not** consume its failure-retry budget.

Task *completions* arrive asynchronously over HTTP (from workers) and are
handled by ``on_task_complete`` / ``on_task_status``, which mutate state through
the JobManager's locked helpers so the Flask threads and the scheduler thread
never race.
"""

from __future__ import annotations

import threading
import traceback
from typing import Optional

from backend.common import constants as C
from backend.common.http_client import HttpClient
from backend.common.ids import partition_name
from backend.common.jsonutil import now_ms
from backend.common.logbus import LogBus
from backend.common.models import Job, Task, WorkerRecord
from backend.common.storage import Storage
from backend.master.fault_tolerance import FaultTolerance
from backend.master.job_manager import JobManager
from backend.master.metrics import Metrics
from backend.master.registry import WorkerRegistry
from backend.master.shuffle import ShuffleCoordinator

SHUFFLE_HOLD_MS = 400          # keep the SHUFFLE stage observable for one beat
MAX_TASKS_PER_WORKER = 3       # concurrency cap per worker


def fair_shares(demands: dict[str, int], capacity: int) -> dict[str, int]:
    """Max-min fair allocation of ``capacity`` slots across jobs.

    Every job receives an equal share of the remaining capacity; jobs whose
    demand is smaller are capped at their demand and the leftover is
    redistributed among the rest.  When capacity does not divide evenly, the
    spare slots go to the jobs with the smallest ids so the result is fully
    deterministic (and reproducible tick = predictable scheduling).
    """
    shares = {j: 0 for j in demands}
    remaining = max(0, int(capacity))
    active = {j for j, d in demands.items() if d > 0}
    while remaining > 0 and active:
        per = remaining // len(active)
        if per == 0:
            for j in sorted(active):
                if remaining == 0:
                    break
                shares[j] += 1
                remaining -= 1
            break
        for j in sorted(active):
            give = min(per, demands[j] - shares[j])
            shares[j] += give
            remaining -= give
            if shares[j] >= demands[j]:
                active.discard(j)
    return shares


class Scheduler:
    def __init__(
        self,
        storage: Storage,
        job_manager: JobManager,
        registry: WorkerRegistry,
        shuffle: ShuffleCoordinator,
        fault_tolerance: FaultTolerance,
        metrics: Metrics,
        config,
        logbus: LogBus,
    ) -> None:
        self.storage = storage
        self.job_manager = job_manager
        self.registry = registry
        self.shuffle = shuffle
        self.fault_tolerance = fault_tolerance
        self.metrics = metrics
        self.config = config
        self.logbus = logbus
        self.client = HttpClient(timeout=3.0, retries=1)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="scheduler")

    # ------------------------------------------------------------------
    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.tick()
            except Exception:  # noqa: BLE001 - a scheduler crash must not kill the Master
                traceback.print_exc()
            self._stop.wait(self.config.metric_interval_sec)

    # ------------------------------------------------------------------
    def tick(self) -> None:
        # 1. Reap dead workers and reassign their tasks (on a coarser cadence).
        self._tick_count = getattr(self, "_tick_count", 0) + 1
        if self._tick_count % 5 == 0:
            for worker in self.registry.reap():
                count = self.fault_tolerance.handle_worker_death(worker)
                if count:
                    self.logbus.warn("", f"worker {worker.name} reaped; {count} tasks reassigned",
                                     task_id="cluster")

        # 2. Advance each job's stage state machine.
        for job in self.job_manager.list_jobs():
            if job.is_terminal:
                continue
            try:
                self._advance_stage(job)
            except Exception:  # noqa: BLE001
                traceback.print_exc()

        # 3. Global priority/fair-share dispatch pass across all jobs.
        try:
            self._dispatch_pass()
        except Exception:  # noqa: BLE001
            traceback.print_exc()

        # 4. Speculative duplicates for stragglers.
        for job in self.job_manager.list_jobs():
            if job.is_terminal:
                continue
            try:
                self._maybe_speculate(job)
            except Exception:  # noqa: BLE001
                traceback.print_exc()

    # ------------------------------------------------------------------
    # Stage state machine (dispatch itself happens in the global pass)
    # ------------------------------------------------------------------
    def _advance_stage(self, job: Job) -> None:
        status = job.status
        if status == C.JOB_MAP:
            map_tasks = self.job_manager.tasks_for(job.job_id, C.TASK_MAP)
            if map_tasks and all(t.status == C.TASK_SUCCEEDED for t in map_tasks):
                self.shuffle.build(job)
                self.job_manager.apply_job(job.job_id, lambda j: (
                    setattr(j, "status", C.JOB_SHUFFLE),
                    j.stats.__setitem__("shuffle_started_ms", now_ms()),
                ))
                self.logbus.info(job.job_id, "all map tasks finished; shuffle built",
                                 task_id="shuffle")
        elif status == C.JOB_SHUFFLE:
            started = job.stats.get("shuffle_started_ms", 0)
            if now_ms() - started >= SHUFFLE_HOLD_MS:
                self.job_manager.set_job_status(job, C.JOB_REDUCE)
                self.logbus.info(job.job_id, "shuffle complete; reduce stage started",
                                 task_id="shuffle")
        elif status == C.JOB_REDUCE:
            reduce_tasks = self.job_manager.tasks_for(job.job_id, C.TASK_REDUCE)
            if reduce_tasks and all(t.status == C.TASK_SUCCEEDED for t in reduce_tasks):
                self._finish_success(job)

    # ------------------------------------------------------------------
    # Global priority + fair-share dispatch
    # ------------------------------------------------------------------
    def _stage_kind(self, job: Job) -> str:
        return C.TASK_MAP if job.status == C.JOB_MAP else C.TASK_REDUCE

    def _runnable_tasks(self, job: Job, now: int) -> list[Task]:
        """Pending tasks of the job's current stage whose backoff has elapsed."""
        if job.status not in (C.JOB_MAP, C.JOB_REDUCE):
            return []
        out: list[Task] = []
        for t in self.job_manager.tasks_for(job.job_id, self._stage_kind(job)):
            if t.status == C.TASK_PENDING:
                out.append(t)
            elif t.status == C.TASK_RETRYING and t.retry_after_ms <= now:
                out.append(t)
        return out

    def _served_count(self, job: Job) -> int:
        """Tasks of the job's current stage currently occupying worker slots."""
        if job.status not in (C.JOB_MAP, C.JOB_REDUCE):
            return 0
        return sum(
            1 for t in self.job_manager.tasks_for(job.job_id, self._stage_kind(job))
            if t.status in (C.TASK_ASSIGNED, C.TASK_RUNNING)
        )

    def _effective_priority(self, job: Job, now: int) -> int:
        """Base priority plus the aging boost earned by waiting without slots."""
        aging_sec = float(getattr(self.config, "priority_aging_sec", 0.0))
        waiting_since = int(job.stats.get("waiting_since_ms", 0) or 0)
        if aging_sec <= 0 or not waiting_since:
            return job.priority
        boost = int((now - waiting_since) // (aging_sec * 1000))
        return min(C.PRIORITY_MAX, job.priority + boost)

    def _update_wait_clock(self, job: Job, runnable: int, served: int, now: int) -> None:
        """Track how long a job has had runnable work but received zero slots."""
        waiting_since = int(job.stats.get("waiting_since_ms", 0) or 0)
        if runnable > 0 and served == 0 and not waiting_since:
            self.job_manager.apply_job(
                job.job_id, lambda j: j.stats.__setitem__("waiting_since_ms", now))
        elif (runnable == 0 or served > 0) and waiting_since:
            self.job_manager.apply_job(
                job.job_id, lambda j: j.stats.pop("waiting_since_ms", None))

    def _capacity_pool(self) -> dict[str, int]:
        """Free task slots per alive worker (capacity minus running tasks)."""
        pool: dict[str, int] = {}
        for worker in self.registry.alive():
            capacity = max(1, min(worker.cpu_cores or 2, MAX_TASKS_PER_WORKER))
            free = capacity - self._count_running_on(worker.worker_id)
            if free > 0:
                pool[worker.worker_id] = free
        return pool

    def _total_capacity(self) -> int:
        return sum(
            max(1, min(w.cpu_cores or 2, MAX_TASKS_PER_WORKER))
            for w in self.registry.alive()
        )

    def _pick_worker(self, pool: dict[str, int]) -> Optional[WorkerRecord]:
        candidates = [
            self.registry.get(wid) for wid, free in pool.items() if free > 0
        ]
        candidates = [w for w in candidates if w is not None]
        if not candidates:
            return None

        def load(w: WorkerRecord) -> float:
            return w.load1 * 2.0 + w.cpu_percent * 0.01

        return min(candidates, key=load)

    def _dispatch_pass(self) -> None:
        now = now_ms()
        jobs = [j for j in self.job_manager.list_jobs()
                if j.status in (C.JOB_MAP, C.JOB_REDUCE)]
        if not jobs:
            return
        by_id = {j.job_id: j for j in jobs}
        pool = self._capacity_pool()
        free = sum(pool.values())

        # Effective priorities decide the bands; highest band is served first.
        eff = {j.job_id: self._effective_priority(j, now) for j in jobs}
        bands: dict[int, list[Job]] = {}
        for j in jobs:
            bands.setdefault(eff[j.job_id], []).append(j)

        # Fair shares are entitlements against the *total* cluster capacity:
        # a job's demand is the slots it holds plus the slots it still wants,
        # and its grant is the shortfall between its share and what it already
        # runs.  Capacity left over by a band flows down to the next one.
        remaining = self._total_capacity()
        for prio in sorted(bands, reverse=True):
            band = bands[prio]
            demands = {}
            for j in band:
                want = self._served_count(j) + len(self._runnable_tasks(j, now))
                if want > 0:
                    demands[j.job_id] = want
            if not demands:
                continue
            shares = fair_shares(demands, remaining)
            # Most under-served job first; ties broken by submission order so
            # equal-priority jobs rotate fairly through the capacity.
            order = sorted(
                demands,
                key=lambda jid: (self._served_count(by_id[jid]) - shares[jid],
                                 by_id[jid].created_ms),
            )
            usage = 0
            for jid in order:
                job = by_id[jid]
                granted = shares[jid] - self._served_count(job)
                while granted > 0 and free > 0:
                    runnable = self._runnable_tasks(job, now)
                    if not runnable:
                        break
                    worker = self._pick_worker(pool)
                    if worker is None:
                        free = 0
                        break
                    if self._dispatch(job, runnable[0], worker):
                        pool[worker.worker_id] -= 1
                        free -= 1
                        granted -= 1
                    else:
                        # Worker refused the task; drop it from this tick's pool.
                        pool[worker.worker_id] = 0
                usage += max(self._served_count(job), shares[jid])
            remaining = max(0, remaining - usage)

        # Aging bookkeeping: remember which jobs wanted slots but got none.
        for job in jobs:
            self._update_wait_clock(
                job, len(self._runnable_tasks(job, now)), self._served_count(job), now)

        # Preemption: reclaim slots for higher-priority work still waiting.
        if free <= 0 and bool(getattr(self.config, "preemption_enabled", True)):
            self._preempt_pass(jobs, eff)

    # ------------------------------------------------------------------
    # Preemption (explicit rule, see module docstring)
    # ------------------------------------------------------------------
    def _preempt_pass(self, jobs: list[Job], eff: dict[str, int]) -> None:
        now = now_ms()
        waiting: dict[int, int] = {}
        for job in jobs:
            runnable = len(self._runnable_tasks(job, now))
            if runnable > 0:
                prio = eff[job.job_id]
                waiting[prio] = waiting.get(prio, 0) + runnable
        if not waiting:
            return

        budget = int(getattr(self.config, "preemption_max_per_tick", 4))
        guard = float(getattr(self.config, "preemption_guard_progress", 0.8))
        for prio in sorted(waiting, reverse=True):
            need = waiting[prio]
            # Victims: running tasks of strictly-lower-priority jobs, lowest
            # priority first, then least progress first (least work lost).
            victims: list[tuple[int, float, Job, Task]] = []
            for job in jobs:
                if eff[job.job_id] >= prio:
                    continue
                for t in self.job_manager.tasks_for(job.job_id):
                    if t.status in (C.TASK_ASSIGNED, C.TASK_RUNNING) and t.progress < guard:
                        victims.append((eff[job.job_id], t.progress, job, t))
            victims.sort(key=lambda v: (v[0], v[1], v[3].task_id))
            for _, _, job, task in victims:
                if need <= 0 or budget <= 0:
                    break
                if self._preempt(job, task):
                    need -= 1
                    budget -= 1
            if budget <= 0:
                return

    def _preempt(self, job: Job, task: Task) -> bool:
        """Re-queue a running task so a higher-priority job can take its slot."""
        worker_id = task.worker_id or ""
        stats = dict(task.stats or {})
        stats["preemptions"] = int(stats.get("preemptions", 0)) + 1
        # Mark first: a late "cancelled" report from the worker then finds the
        # task already RETRYING and is ignored (see on_task_complete).
        # Attempts are NOT incremented — preemption is not a failure.
        self.job_manager.update_task(
            job.job_id, task.task_id,
            status=C.TASK_RETRYING, worker_id=None,
            error="preempted by a higher-priority job",
            retry_after_ms=0, progress=0.0,
            records_processed=0, records_emitted=0,
            stats=stats,
        )
        worker = self.registry.get(worker_id) if worker_id else None
        if worker is not None:
            try:
                self.client.post(f"{worker.address}/task/cancel",
                                 {"task_id": task.task_id}, timeout=2.0)
            except Exception:  # noqa: BLE001 - reaping will reconcile a lost worker
                pass
        self.fault_tolerance.record_preemption(job, task, worker_id)
        self.logbus.warn(
            job.job_id,
            f"task {task.task_id} preempted on {worker.name if worker else '?'} "
            f"(priority {job.priority})",
            task_id=task.task_id, worker_id=worker_id,
        )
        return True

    # ------------------------------------------------------------------
    # Queue introspection for the UI
    # ------------------------------------------------------------------
    def queue_status(self) -> dict:
        """Snapshot of the scheduling queue: capacity and per-job queue state."""
        now = now_ms()
        jobs = [j for j in self.job_manager.list_jobs() if not j.is_terminal]
        pool = self._capacity_pool()
        free = sum(pool.values())
        eff = {j.job_id: self._effective_priority(j, now) for j in jobs}

        # Fair-share entitlements per band, computed against total capacity —
        # exactly what the dispatch pass would try to grant on the next tick.
        bands: dict[int, list[Job]] = {}
        for j in jobs:
            bands.setdefault(eff[j.job_id], []).append(j)
        share_of: dict[str, int] = {}
        remaining = self._total_capacity()
        for prio in sorted(bands, reverse=True):
            demands = {}
            for j in bands[prio]:
                want = self._served_count(j) + len(self._runnable_tasks(j, now))
                if want > 0:
                    demands[j.job_id] = want
            shares = fair_shares(demands, remaining)
            share_of.update(shares)
            usage = sum(max(self._served_count(j), shares.get(j.job_id, 0))
                        for j in bands[prio])
            remaining = max(0, remaining - usage)

        entries = []
        for position, job in enumerate(
                sorted(jobs, key=lambda j: (-eff[j.job_id], j.created_ms))):
            runnable = len(self._runnable_tasks(job, now))
            served = self._served_count(job)
            waiting_since = int(job.stats.get("waiting_since_ms", 0) or 0)
            if served > 0 or job.status == C.JOB_SHUFFLE:
                state = C.QUEUE_RUNNING
            elif eff[job.job_id] > job.priority:
                state = C.QUEUE_STARVED
            else:
                state = C.QUEUE_QUEUED
            entries.append({
                "job_id": job.job_id,
                "name": job.name,
                "status": job.status,
                "priority": job.priority,
                "effective_priority": eff[job.job_id],
                "queue_position": position + 1,
                "queue_state": state,
                "queue_state_label": C.queue_state_label(state),
                "fair_share": share_of.get(job.job_id, 0),
                "running_tasks": served,
                "pending_tasks": runnable,
                "waiting_ms": (now - waiting_since) if waiting_since else 0,
            })
        return {
            "capacity": self._total_capacity(),
            "free_slots": free,
            "preemption_enabled": bool(getattr(self.config, "preemption_enabled", True)),
            "jobs": entries,
        }

    # ------------------------------------------------------------------
    def _available_workers(self) -> list[WorkerRecord]:
        out: list[WorkerRecord] = []
        for worker in self.registry.alive():
            capacity = max(1, min(worker.cpu_cores or 2, MAX_TASKS_PER_WORKER))
            running = self._count_running_on(worker.worker_id)
            if running < capacity:
                out.append(worker)
        return out

    def _count_running_on(self, worker_id: str) -> int:
        count = 0
        for job in self.job_manager.list_jobs():
            if job.is_terminal:
                continue
            for task in self.job_manager.tasks_for(job.job_id):
                if task.worker_id == worker_id and task.status in C.TASK_ACTIVE_STATES:
                    count += 1
        return count

    def _least_loaded(self, workers: list[WorkerRecord],
                      exclude: Optional[str] = None) -> Optional[WorkerRecord]:
        candidates = [w for w in workers if w.worker_id != exclude] or workers
        if not candidates:
            return None

        def load(w: WorkerRecord) -> float:
            return w.load1 * 2.0 + w.cpu_percent * 0.01

        return min(candidates, key=load)

    # ------------------------------------------------------------------
    def _dispatch(self, job: Job, task: Task, worker: WorkerRecord,
                  speculative: bool = False) -> bool:
        spec = self._build_spec(job, task)
        if speculative:
            spec["speculative"] = True
        url = f"{worker.address}/task/execute"
        try:
            resp = self.client.post(url, spec, timeout=4.0)
            accepted = bool(resp.ok and resp.data and resp.data.get("accepted"))
        except Exception as exc:  # noqa: BLE001
            accepted = False
            self.logbus.warn(job.job_id, f"dispatch to {worker.name} failed: {exc}",
                             task_id=task.task_id)
        if not accepted:
            return False

        def mark_dispatched(t: Task) -> None:
            t.status = C.TASK_ASSIGNED
            t.assigned_ms = now_ms()
            if not speculative:
                t.worker_id = worker.worker_id
            else:
                stats = dict(t.stats or {})
                stats.setdefault("speculative_workers", []).append(worker.worker_id)
                t.stats = stats

        self.job_manager.apply_task(job.job_id, task.task_id, mark_dispatched)
        self.logbus.info(
            job.job_id,
            f"task {task.task_id} dispatched to {worker.name}" + (" (speculative)" if speculative else ""),
            task_id=task.task_id, worker_id=worker.worker_id,
        )
        return True

    def _build_spec(self, job: Job, task: Task) -> dict:
        spec: dict = {
            "task_id": task.task_id,
            "job_id": job.job_id,
            "kind": task.kind,
            "index": task.index,
            "mapper": job.mapper,
            "reducer": job.reducer,
            "params": job.params,
            "attempt": 0,
            "simulate_failure": bool(job.params.get("simulate_failure", False)),
        }
        if task.kind == C.TASK_MAP:
            spec["partition_count"] = job.num_reduce_tasks
            spec["records"] = self.job_manager.planner.load_input_shard(job.job_id, task.input_shard)
        else:
            spec["partition"] = task.partition
            spec["fetch_plan"] = (task.stats or {}).get("fetch_plan", [])
            spec["num_map_tasks"] = job.num_map_tasks
        return spec

    # ------------------------------------------------------------------
    # Completion / progress handling (invoked from Flask routes)
    # ------------------------------------------------------------------
    def on_task_status(self, payload: dict) -> None:
        job = self.job_manager.get_job(payload.get("job_id", ""))
        if job is None:
            return
        task = self.job_manager.get_task(job.job_id, payload.get("task_id", ""))
        if task is None or task.status == C.TASK_SUCCEEDED:
            return

        def apply(t: Task) -> None:
            if t.status in (C.TASK_PENDING, C.TASK_RETRYING, C.TASK_ASSIGNED):
                t.status = C.TASK_RUNNING
                t.worker_id = payload.get("worker_id", t.worker_id)
            if not t.started_ms:
                t.started_ms = now_ms()
            t.progress = float(payload.get("progress", t.progress))
            t.records_processed = int(payload.get("records_processed", t.records_processed))
            t.records_emitted = int(payload.get("records_emitted", t.records_emitted))

        self.job_manager.apply_task(job.job_id, task.task_id, apply)

    def on_task_complete(self, payload: dict) -> None:
        job = self.job_manager.get_job(payload.get("job_id", ""))
        if job is None:
            return
        task = self.job_manager.get_task(job.job_id, payload.get("task_id", ""))
        if task is None or task.status == C.TASK_SUCCEEDED:
            return  # duplicate completion from a speculative loser

        worker_id = payload.get("worker_id", "")
        status = payload.get("status", C.TASK_FAILED)

        if status != C.TASK_SUCCEEDED:
            if task.status in (C.TASK_PENDING, C.TASK_RETRYING):
                # Late failure report from a preempted or reassigned attempt;
                # the task has already been re-queued, so ignore it.
                return
            self.registry.task_finished(worker_id, success=False)
            self.fault_tolerance.handle_task_failure(job, task, payload.get("error", ""), worker_id)
            return

        # Success path.
        def apply(t: Task) -> None:
            t.status = C.TASK_SUCCEEDED
            t.progress = 1.0
            t.records_processed = int(payload.get("records_processed", 0))
            t.records_emitted = int(payload.get("records_emitted", 0))
            t.duration_ms = int(payload.get("duration_ms", 0)) * 1000
            t.finished_ms = now_ms()
            t.error = ""
            stats = dict(t.stats or {})
            stats["partition_size_entries"] = payload.get("partition_sizes", {})
            stats["results"] = payload.get("results", [])
            stats["winning_worker"] = worker_id
            t.stats = stats

        self.job_manager.apply_task(job.job_id, task.task_id, apply)
        self.registry.task_finished(worker_id, success=True)
        self.metrics.record_task(job, task, int(payload.get("duration_ms", 0)))

        if task.kind == C.TASK_REDUCE:
            self._store_results(job, task, payload.get("results", []))
            self.shuffle.mark_partition_done(job, task.partition,
                                             task.stats.get("shuffle_bytes", 0))

        self.logbus.info(
            job.job_id,
            f"task {task.task_id} succeeded ({payload.get('records_processed', 0)} records, "
            f"{payload.get('duration_ms', 0)} ms)",
            task_id=task.task_id, worker_id=worker_id,
        )
        self._cancel_speculative_losers(job, task, worker_id)

    def _store_results(self, job: Job, task: Task, results: list) -> None:
        pname = partition_name(task.partition)
        self.storage.write({
            "job_id": job.job_id,
            "partition": task.partition,
            "partition_name": pname,
            "task_id": task.task_id,
            "records": list(reversed(results)),
            "count": len(results),
            "written_ms": now_ms(),
        }, "jobs", job.job_id, "results", C.STAGE_REDUCE, f"{pname}.json")

    def _cancel_speculative_losers(self, job: Job, task: Task, winner_worker_id: str) -> None:
        losers = list((task.stats or {}).get("speculative_workers", []))
        if winner_worker_id != task.worker_id and task.worker_id:
            losers.append(task.worker_id)
        for wid in losers:
            if wid == winner_worker_id:
                continue
            worker = self.registry.get(wid)
            if worker is not None:
                try:
                    self.client.post(f"{worker.address}/task/cancel",
                                     {"task_id": task.task_id}, timeout=2.0)
                except Exception:  # noqa: BLE001
                    pass

    # ------------------------------------------------------------------
    def _finish_success(self, job: Job) -> None:
        map_tasks = self.job_manager.tasks_for(job.job_id, C.TASK_MAP)
        reduce_tasks = self.job_manager.tasks_for(job.job_id, C.TASK_REDUCE)

        def apply(j: Job) -> None:
            j.status = C.JOB_SUCCEEDED
            j.finished_ms = now_ms()
            j.stats["map_records_processed"] = sum(t.records_processed for t in map_tasks)
            j.stats["map_records_emitted"] = sum(t.records_emitted for t in map_tasks)
            j.stats["reduce_records_emitted"] = sum(t.records_emitted for t in reduce_tasks) + sum(t.records_emitted for t in map_tasks)
            j.stats["total_task_attempts"] = sum(t.attempts for t in map_tasks + reduce_tasks)

        self.job_manager.apply_job(job.job_id, apply)
        self.logbus.info(job.job_id, "job succeeded", task_id="job")
        self._cleanup_worker_shuffle(job)

    def _cleanup_worker_shuffle(self, job: Job) -> None:
        for worker in self.registry.alive():
            try:
                self.client.post(f"{worker.address}/shuffle/cleanup/{job.job_id}", timeout=2.0)
            except Exception:  # noqa: BLE001
                pass

    # ------------------------------------------------------------------
    def _maybe_speculate(self, job: Job) -> None:
        for task in self.fault_tolerance.find_stragglers(job):
            workers = self._available_workers()
            worker = self._least_loaded(workers, exclude=task.worker_id)
            if worker is None:
                continue
            self.fault_tolerance.mark_speculated(job, task)
            self._dispatch(job, task, worker, speculative=True)
            self.logbus.warn(job.job_id, f"speculative copy of {task.task_id} -> {worker.name}",
                             task_id=task.task_id)
