"""Priority-aware Master scheduler.

The scheduler owns one global view of cluster slots.  On every tick it:

1. reaps dead workers and reassigns their tasks;
2. ages queued jobs so a permanently busy high-priority pool cannot starve them;
3. accounts weighted fair runtime for jobs at the same effective priority;
4. optionally preempts lower-priority (or over-allocated same-priority) tasks;
5. dispatches pending tasks in strict ``(priority, fair runtime, submission)``
   order;
6. advances the map -> shuffle -> reduce state machine and handles speculation.

Preemption is deliberately conservative: only the slots actually needed by
waiting work are reclaimed, tasks in their short grace period or nearly complete
are protected, and a preempted task is retried without consuming its failure
budget.
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
from backend.master.job_manager import JobManager, priority_label
from backend.master.metrics import Metrics
from backend.master.registry import WorkerRegistry
from backend.master.shuffle import ShuffleCoordinator

SHUFFLE_HOLD_MS = 400          # keep the SHUFFLE stage observable for one beat
MAX_TASKS_PER_WORKER = 3       # concurrency cap per worker
DISPATCH_CREDIT_MS = 1000.0    # nominal virtual cost that spreads fresh slots
DRAIN_HOLD_MS = 5_000           # reservation while a preempted task is stopping


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
        self._tick_lock = threading.RLock()
        self._draining: dict[tuple[str, str, str], int] = {}
        self._last_fairness_ms = now_ms()

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
        # The priority API can request an immediate tick while the scheduler
        # thread is also firing; serialize the whole allocation decision.
        with self._tick_lock:
            self._tick_locked()

    def _tick_locked(self) -> None:
        # 1. Reap dead workers and reassign their tasks (on a coarser cadence).
        self._tick_count = getattr(self, "_tick_count", 0) + 1
        if self._tick_count % 5 == 0:
            for worker in self.registry.reap():
                count = self.fault_tolerance.handle_worker_death(worker)
                if count:
                    self.logbus.warn("", f"worker {worker.name} reaped; {count} tasks reassigned",
                                     task_id="cluster")

        now = now_ms()
        expired_draining = self._expire_draining(now)
        self._apply_aging_and_fairness(now)
        self._preempt_for_priority(now)
        self._dispatch_runnable_tasks(now)
        if expired_draining:
            # Reap a lost cancellation report and admit its task without
            # waiting for the next scheduler interval.
            self._dispatch_runnable_tasks(now)

        # 2. Advance each active job's stage state machine.
        for job in self.job_manager.list_jobs():
            if job.is_terminal:
                continue
            try:
                self._advance(job)
            except Exception:  # noqa: BLE001
                traceback.print_exc()

        # 3. Spare capacity can be used for speculative duplicates, but never
        #    ahead of ordinary pending work.
        for job in self.job_manager.list_jobs():
            if job.is_terminal:
                continue
            self._maybe_speculate(job)

    # ------------------------------------------------------------------
    # Fair accounting / aging
    # ------------------------------------------------------------------
    def _weight(self, effective_priority: int) -> float:
        # Ordering remains strict priority first.  Within a priority class this
        # weight normalizes fair runtime, giving low-priority classes a modest
        # share when later aging/configuration permits admission.
        p = max(C.PRIORITY_MIN, min(C.PRIORITY_MAX, int(effective_priority)))
        return float(max(1, 2 ** (p - C.PRIORITY_LOW)))

    def _current_kind(self, job: Job) -> Optional[str]:
        if job.status == C.JOB_MAP:
            return C.TASK_MAP
        if job.status == C.JOB_REDUCE:
            return C.TASK_REDUCE
        return None

    def _task_index(self, tasks: list[Task]) -> dict:
        running = []
        ready = []
        blocked_retry = []
        now = now_ms()
        for task in tasks:
            if task.status in (C.TASK_ASSIGNED, C.TASK_RUNNING):
                running.append(task)
            elif task.status == C.TASK_PENDING:
                ready.append(task)
            elif task.status == C.TASK_RETRYING:
                if task.retry_after_ms <= now:
                    ready.append(task)
                else:
                    blocked_retry.append(task)
        ready.sort(key=lambda t: (t.index, t.task_id))
        return {"running": running, "ready": ready, "blocked_retry": blocked_retry}

    def _scheduling_view(self) -> dict:
        now = now_ms()
        capacities = self._worker_capacities(now)
        jobs = []
        for job in self.job_manager.list_jobs():
            kind = self._current_kind(job)
            if kind is None or job.is_terminal:
                continue
            idx = self._task_index(self.job_manager.tasks_for(job.job_id, kind))
            effective_priority = self._effective_priority(job, idx, now, persist=False)
            jobs.append({"job": job, "kind": kind, **idx, "effective_priority": effective_priority})
        return {"now": now, "capacities": capacities, "jobs": jobs}

    def _apply_aging_and_fairness(self, now: int) -> None:
        elapsed_ms = max(0.0, float(now - self._last_fairness_ms))
        self._last_fairness_ms = now
        if elapsed_ms <= 0:
            return

        def update(job: Job) -> None:
            kind = self._current_kind(job)
            if kind is None:
                return
            idx = self._task_index(self.job_manager.tasks_for(job.job_id, kind))
            job.effective_priority = self._effective_priority(job, idx, now, persist=True)
            active = len(idx["running"])
            if active:
                job.fair_vruntime_ms += elapsed_ms / self._weight(job.effective_priority) * active
                job.waiting_since_ms = 0
            elif idx["ready"] and not job.waiting_since_ms:
                job.waiting_since_ms = now

        self.job_manager.apply_all_jobs(update)

    def _effective_priority(self, job: Job, idx: dict, now: int, persist: bool) -> int:
        aging_sec = float(getattr(self.config, "priority_aging_sec", 0.0) or 0.0)
        base = max(C.PRIORITY_MIN, min(C.PRIORITY_MAX, int(job.priority)))
        if aging_sec <= 0 or idx["running"] or not idx["ready"] or not job.waiting_since_ms:
            value = base
        else:
            waited_sec = max(0.0, (now - job.waiting_since_ms) / 1000.0)
            value = min(C.PRIORITY_MAX, base + int(waited_sec // aging_sec))
        if persist:
            job.effective_priority = value
        return value

    # ------------------------------------------------------------------
    # Capacity and global dispatch
    # ------------------------------------------------------------------
    def _worker_capacities(self, now: int) -> dict[str, dict]:
        capacities: dict[str, dict] = {}
        active_by_worker = self._active_tasks_by_worker()
        for worker in self.registry.alive():
            wid = worker.worker_id
            capacity = max(1, min(worker.cpu_cores or 2, MAX_TASKS_PER_WORKER))
            occupied = len(active_by_worker.get(wid, []))
            draining = sum(1 for (j, t, w), expires in self._draining.items()
                           if w == wid and expires > now)
            capacities[wid] = {
                "worker": worker,
                "capacity": capacity,
                "occupied": occupied,
                "draining": draining,
                "free": max(0, capacity - occupied - draining),
            }
        return capacities

    def _active_tasks_by_worker(self) -> dict[str, list[tuple[Job, Task]]]:
        out: dict[str, list[tuple[Job, Task]]] = {}
        for job in self.job_manager.list_jobs():
            if job.is_terminal:
                continue
            for task in self.job_manager.tasks_for(job.job_id):
                wid = task.worker_id
                if wid and task.status in (C.TASK_ASSIGNED, C.TASK_RUNNING):
                    out.setdefault(wid, []).append((job, task))
        return out

    def _expire_draining(self, now: int) -> int:
        expired_count = 0
        for key, expires in list(self._draining.items()):
            if expires > now:
                continue
            job_id, task_id, _worker_id = key
            task = self.job_manager.get_task(job_id, task_id)
            if task is not None and task.status == C.TASK_CANCELING:
                expired_count += 1
                # The stop report was lost; return the task to the queue rather
                # than double-dispatching while the old worker may still hold it.
                self.job_manager.update_task(
                    job_id, task_id, status=C.TASK_RETRYING, worker_id=None,
                    retry_after_ms=now, progress=0.0,
                    records_processed=0, records_emitted=0,
                    dispatch_token=task.dispatch_token + 1,
                )
            self._draining.pop(key, None)
        return expired_count

    def _reserve_draining(self, job: Job, task: Task, worker_id: str, now: int) -> None:
        self._draining[(job.job_id, task.task_id, worker_id)] = now + DRAIN_HOLD_MS



    def _release_draining(self, job_id: str, task_id: str, worker_id: str = "") -> None:
        if worker_id:
            self._draining.pop((job_id, task_id, worker_id), None)
            return
        for key in list(self._draining):
            if key[0] == job_id and key[1] == task_id:
                self._draining.pop(key, None)

    def _free_workers(self, capacities: dict[str, dict]) -> list[WorkerRecord]:
        return [info["worker"] for info in capacities.values() if info["free"] > 0]

    def _least_loaded(self, workers: list[WorkerRecord],
                      exclude: Optional[str] = None) -> Optional[WorkerRecord]:
        candidates = [w for w in workers if w.worker_id != exclude] or workers
        if not candidates:
            return None

        def load(w: WorkerRecord) -> float:
            return w.load1 * 2.0 + w.cpu_percent * 0.01

        return min(candidates, key=load)

    def _job_order(self, entries: list[dict]) -> list[dict]:
        return sorted(
            entries,
            key=lambda e: (
                -e["effective_priority"],
                e["job"].fair_vruntime_ms,
                e["job"].created_ms,
                e["job"].job_id,
            ),
        )

    def _dispatch_runnable_tasks(self, now: int) -> None:
        view = self._scheduling_view()
        capacities = view["capacities"]
        if not capacities:
            return

        for entry in self._job_order(view["jobs"]):
            job = entry["job"]
            for task in list(entry["ready"]):
                if task.status == C.TASK_RETRYING and task.retry_after_ms > now:
                    continue
                free_workers = self._free_workers(capacities)
                if not free_workers:
                    return
                worker = self._least_loaded(free_workers)
                if worker is None:
                    return
                if self._dispatch(job, task, worker):
                    info = capacities[worker.worker_id]
                    info["occupied"] += 1
                    info["free"] = max(0, info["free"] - 1)
                    entry["running"].append(task)
                    entry["ready"].remove(task)
                    job.fair_vruntime_ms += DISPATCH_CREDIT_MS / self._weight(entry["effective_priority"])
                    job.waiting_since_ms = 0
                    self.job_manager.save_job(job)
            if not entry["running"] and entry["ready"] and not job.waiting_since_ms:
                job.waiting_since_ms = now

    def _runnable_demand_exists(self, min_effective_priority: int) -> bool:
        view = self._scheduling_view()
        return any(
            e["ready"] and e["effective_priority"] >= min_effective_priority
            for e in view["jobs"]
        )

    # ------------------------------------------------------------------
    # Preemption
    # ------------------------------------------------------------------
    def _preempt_for_priority(self, now: int) -> None:
        view = self._scheduling_view()
        capacities = view["capacities"]
        free_slots = sum(info["free"] for info in capacities.values())
        entries = self._job_order(view["jobs"])

        # Higher effective priority first.  Free slots naturally satisfy this
        # demand; only the remaining unmet demand causes preemption.
        for entry in entries:
            ready = list(entry["ready"])
            if not ready:
                continue
            need = max(0, len(ready) - free_slots)
            if need <= 0:
                free_slots = max(0, free_slots - len(ready))
                continue
            if not getattr(self.config, "priority_preemption", True):
                continue
            victims = self._priority_victims(entries, entry, need, now)
            for victim_job, victim_task, worker_id in victims:
                if self._preempt(victim_job, victim_task, worker_id, entry["job"], now,
                                 reason="higher_priority"):
                    free_slots += 1
                    need -= 1
            free_slots = max(0, free_slots)

        if not getattr(self.config, "priority_preemption", True):
            return
        self._preempt_for_fairness(entries, now)

    def _priority_victims(self, entries: list[dict], waiter: dict, need: int,
                          now: int) -> list[tuple[Job, Task, str]]:
        candidates: list[tuple[float, Job, Task, str]] = []
        waiter_priority = waiter["effective_priority"]
        grace_ms = int(float(self.config.preemption_grace_sec) * 1000)
        min_progress = float(self.config.preemption_min_progress)
        for entry in entries:
            if entry["effective_priority"] >= waiter_priority:
                continue
            for task in entry["running"]:
                wid = task.worker_id or ""
                if not wid or (entry["job"].job_id, task.task_id, wid) in self._draining:
                    continue
                if task.assigned_ms and now - task.assigned_ms < grace_ms:
                    continue
                if task.progress >= min_progress:
                    continue
                # Lower priority first, then reclaim the least invested work.
                score = (entry["effective_priority"], task.progress, task.assigned_ms)
                candidates.append((score, entry["job"], task, wid))
        candidates.sort(key=lambda x: x[0])
        return [(j, t, w) for _, j, t, w in candidates[:need]]

    def _preempt_for_fairness(self, entries: list[dict], now: int) -> None:
        """Rebalance same-priority jobs without letting one job monopolize slots."""
        ready_priorities = {e["effective_priority"] for e in entries if e["ready"]}
        groups: dict[int, list[dict]] = {
            priority: [e for e in entries if e["effective_priority"] == priority]
            for priority in ready_priorities
        }

        grace_ms = int(float(getattr(self.config, "fair_preemption_grace_sec", 2.0)) * 1000)
        for group in groups.values():
            ordered = self._job_order(group)
            starved = next((e for e in ordered if not e["running"] and e["ready"] and
                            e["job"].waiting_since_ms and
                            now - e["job"].waiting_since_ms >= grace_ms), None)
            if starved is None:
                continue
            donors = [e for e in ordered if e is not starved and len(e["running"]) >= 2]
            if not donors:
                continue
            donor = max(donors, key=lambda e: (len(e["running"]), -e["job"].fair_vruntime_ms))
            min_progress = float(self.config.preemption_min_progress)
            grace = int(float(self.config.preemption_grace_sec) * 1000)
            candidates = [
                t for t in donor["running"]
                if t.worker_id and t.progress < min_progress
                and (not t.assigned_ms or now - t.assigned_ms >= grace)
                and (donor["job"].job_id, t.task_id, t.worker_id) not in self._draining
            ]
            if not candidates:
                continue
            victim = min(candidates, key=lambda t: (t.progress, t.assigned_ms))
            wid = victim.worker_id or ""
            self._preempt(donor["job"], victim, wid, starved["job"], now,
                          reason="same_priority_fairness")

    def _preempt(self, victim_job: Job, victim_task: Task, worker_id: str,
                 requester: Job, now: int, reason: str) -> bool:
        worker = self.registry.get(worker_id)
        if worker is None:
            return False
        try:
            resp = self.client.post(f"{worker.address}/task/cancel",
                                    {"job_id": victim_job.job_id, "task_id": victim_task.task_id},
                                    timeout=2.0)
            cancelled = bool(resp.ok and resp.data and resp.data.get("cancelled"))
        except Exception:  # noqa: BLE001
            cancelled = False
        if not cancelled:
            return False

        applied = False

        def reset(task: Task) -> None:
            nonlocal applied
            if task.status == C.TASK_SUCCEEDED:
                return
            applied = True
            task.status = C.TASK_CANCELING
            task.worker_id = None
            task.progress = 0.0
            task.records_processed = 0
            task.records_emitted = 0
            task.retry_after_ms = now
            task.error = ""
            task.dispatch_token += 1
            stats = dict(task.stats or {})
            stats["preemptions"] = int(stats.get("preemptions", 0)) + 1
            stats["last_preempted_by"] = requester.job_id
            stats["last_preempted_worker"] = worker_id
            stats["last_preemption_reason"] = reason
            stats["last_preemption_ms"] = now
            task.stats = stats

        self.job_manager.apply_task(victim_job.job_id, victim_task.task_id, reset)
        if not applied:
            return False
        self._reserve_draining(victim_job, victim_task, worker_id, now)
        preempted = int(victim_job.stats.get("preempted_tasks", 0)) + 1
        self.job_manager.apply_job(victim_job.job_id,
                                   lambda j: j.stats.__setitem__("preempted_tasks", preempted))
        self.logbus.warn(
            victim_job.job_id,
            f"task {victim_task.task_id} preempted for {reason}; requester={requester.job_id}",
            task_id=victim_task.task_id, worker_id=worker_id,
        )
        return True

    # ------------------------------------------------------------------
    def _advance(self, job: Job) -> None:
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
                self.job_manager.apply_job(
                    job.job_id,
                    lambda j: setattr(j, "waiting_since_ms", now_ms()),
                )
                self.logbus.info(job.job_id, "shuffle complete; reduce stage started",
                                 task_id="shuffle")
        elif status == C.JOB_REDUCE:
            reduce_tasks = self.job_manager.tasks_for(job.job_id, C.TASK_REDUCE)
            if reduce_tasks and all(t.status == C.TASK_SUCCEEDED for t in reduce_tasks):
                self._finish_success(job)

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
                t.dispatch_token += 1
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
            "attempt": task.attempts,
            "dispatch_token": task.dispatch_token + (0 if task.status == C.TASK_ASSIGNED else 1),
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
        token = int(payload.get("dispatch_token", task.dispatch_token) or 0)
        if token and token != task.dispatch_token:
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
            return  # duplicate completion from a speculative loser or retry

        worker_id = payload.get("worker_id", "")
        status = payload.get("status", C.TASK_FAILED)
        token = int(payload.get("dispatch_token", task.dispatch_token) or 0)
        if status == C.TASK_CANCELLED:
            self._release_draining(job.job_id, task.task_id, worker_id)
            should_schedule = True
            if not token or token == task.dispatch_token or task.status == C.TASK_CANCELING:
                # Normal preemption marks the task CANCELING before this late
                # report arrives; without a mark, cancellation still means retry.
                self.job_manager.update_task(
                    job.job_id, task.task_id,
                    status=C.TASK_RETRYING, worker_id=None, progress=0.0,
                    records_processed=0, records_emitted=0, retry_after_ms=now_ms(),
                    dispatch_token=task.dispatch_token + 1,
                )
            else:
                should_schedule = task.status == C.TASK_CANCELING
            if should_schedule:
                self.tick()
            return

        self._release_draining(job.job_id, task.task_id, worker_id)
        if token and token != task.dispatch_token:
            return  # response from an attempt that was already replaced/preempted

        if status == C.TASK_SUCCEEDED and task.status == C.TASK_CANCELING:
            # The worker completed just as the scheduler requested cancellation;
            # its late CANCELLED report will reconcile the draining reservation.
            return

        if status != C.TASK_SUCCEEDED:
            self.registry.task_finished(worker_id, success=False)
            self.fault_tolerance.handle_task_failure(job, task, payload.get("error", ""), worker_id)
            return

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
                                     {"job_id": job.job_id, "task_id": task.task_id},
                                     timeout=2.0)
                except Exception:  # noqa: BLE001
                    pass

    # ------------------------------------------------------------------
    def _finish_success(self, job: Job) -> None:
        map_tasks = self.job_manager.tasks_for(job.job_id, C.TASK_MAP)
        reduce_tasks = self.job_manager.tasks_for(job.job_id, C.TASK_REDUCE)

        def apply(j: Job) -> None:
            j.status = C.JOB_SUCCEEDED
            j.finished_ms = now_ms()
            j.waiting_since_ms = 0
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
            if self._runnable_demand_exists(job.effective_priority):
                return
            capacities = self._worker_capacities(now_ms())
            worker = self._least_loaded(
                [info["worker"] for info in capacities.values() if info["free"] > 0],
                exclude=task.worker_id,
            )
            if worker is None:
                continue
            if worker is None:
                continue
            if self._dispatch(job, task, worker, speculative=True):
                self.fault_tolerance.mark_speculated(job, task)
                capacities[worker.worker_id]["occupied"] += 1
                capacities[worker.worker_id]["free"] = max(0, capacities[worker.worker_id]["free"] - 1)
            self.logbus.warn(job.job_id, f"speculative copy of {task.task_id} -> {worker.name}",
                             task_id=task.task_id)

    # ------------------------------------------------------------------
    # API/UI snapshot
    # ------------------------------------------------------------------
    def queue_snapshot(self) -> dict:
        now = now_ms()
        view = self._scheduling_view()
        capacities = view["capacities"]
        total_slots = sum(info["capacity"] for info in capacities.values())
        free_slots = sum(info["free"] for info in capacities.values())

        entries = self._job_order(view["jobs"])
        queued = [e for e in entries if not e["running"] and e["ready"]]
        active_priorities = sorted({
            e["effective_priority"] for e in entries if e["running"] or e["ready"]
        }, reverse=True)

        items = []
        for position, entry in enumerate(queued, start=1):
            job = entry["job"]
            higher = [e for e in entries
                      if e["effective_priority"] > entry["effective_priority"]
                      and (e["running"] or e["ready"])]
            if entry["blocked_retry"] and not entry["ready"]:
                state = C.QUEUE_RETRY_WAIT
                reason = "retry_backoff"
            elif higher:
                state = C.QUEUE_WAITING
                reason = "higher_priority"
            elif free_slots == 0:
                state = C.QUEUE_WAITING
                reason = "fair_share"
            else:
                state = C.QUEUE_READY
                reason = "ready"
            items.append({
                "job_id": job.job_id,
                "queue_state": state,
                "queue_position": position,
                "wait_reason": reason,
                "ready_tasks": len(entry["ready"]),
                "running_tasks": len(entry["running"]),
            })

        job_items = {item["job_id"]: item for item in items}
        for entry in entries:
            job = entry["job"]
            if entry["running"]:
                state = C.QUEUE_RUNNING
                reason = "running"
            elif job.job_id in job_items:
                continue
            elif job.status == C.JOB_SHUFFLE:
                state = C.QUEUE_STAGE_WAIT
                reason = "shuffle"
            elif entry["blocked_retry"]:
                state = C.QUEUE_RETRY_WAIT
                reason = "retry_backoff"
            else:
                state = C.QUEUE_READY
                reason = "ready"
            items.append({
                "job_id": job.job_id,
                "queue_state": state,
                "queue_position": 0,
                "wait_reason": reason,
                "ready_tasks": len(entry["ready"]),
                "running_tasks": len(entry["running"]),
            })

        queue_by_job = {item["job_id"]: item for item in items}
        jobs = []
        for job in self.job_manager.list_jobs():
            summary = self.job_manager.job_summary(job)
            extra = queue_by_job.get(job.job_id, {
                "queue_state": "DONE" if job.is_terminal else C.QUEUE_STAGE_WAIT,
                "queue_position": 0,
                "wait_reason": "done" if job.is_terminal else "stage",
                "ready_tasks": 0,
                "running_tasks": 0,
            })
            summary.update(extra)
            summary["priority_label"] = priority_label(job.priority)
            if summary.get("effective_priority") != job.priority:
                summary["effective_priority_label"] = priority_label(job.effective_priority)
            jobs.append(summary)

        return {
            "generated_ms": now,
            "total_slots": total_slots,
            "free_slots": free_slots,
            "workers": [
                {
                    "worker_id": info["worker"].worker_id,
                    "name": info["worker"].name,
                    "capacity": info["capacity"],
                    "occupied": info["occupied"],
                    "draining": info["draining"],
                    "free": info["free"],
                }
                for info in sorted(capacities.values(), key=lambda x: x["worker"].worker_id)
            ],
            "preemption_enabled": bool(getattr(self.config, "priority_preemption", True)),
            "active_priorities": active_priorities,
            "jobs": jobs,
            "queue": [queue_by_job[e["job"].job_id] for e in queued],
        }
