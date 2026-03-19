from __future__ import annotations

import json
import threading
import time
import uuid
from concurrent.futures import Future
from dataclasses import asdict, dataclass, field
from typing import Any, Optional

from .object_store import CompareAndSwapError, ObjectStore


@dataclass
class BrokerLease:
    broker_id: str
    heartbeat_at: float


@dataclass
class QueueJob:
    job_id: str
    payload: dict[str, Any]
    status: str = "queued"
    claimed_by: Optional[str] = None
    claimed_at: Optional[float] = None
    heartbeat_at: Optional[float] = None
    completed_at: Optional[float] = None
    attempt: int = 0


@dataclass
class QueueState:
    broker: Optional[BrokerLease] = None
    jobs: list[QueueJob] = field(default_factory=list)

    @classmethod
    def from_bytes(cls, raw: bytes) -> "QueueState":
        if not raw:
            return cls()
        data = json.loads(raw.decode("utf-8"))
        broker = data.get("broker")
        return cls(
            broker=BrokerLease(**broker) if broker else None,
            jobs=[QueueJob(**job) for job in data.get("jobs", [])],
        )

    def to_bytes(self) -> bytes:
        payload = {
            "broker": asdict(self.broker) if self.broker else None,
            "jobs": [asdict(job) for job in self.jobs],
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")


@dataclass
class BufferedOperation:
    kind: str
    args: tuple[Any, ...]
    future: Future


class ObjectStorageQueueBroker:
    """Final queue design from the blog, adapted for Python.

    The broker is the only writer to object storage. It batches enqueues, claims,
    heartbeats, and completes into a single CAS loop, records its lease in the queue
    file, and allows workers to reclaim stale in-progress jobs once heartbeats expire.
    """

    def __init__(
        self,
        store: ObjectStore,
        *,
        key: str = "queue.json",
        broker_id: Optional[str] = None,
        flush_interval: float = 0.05,
        broker_timeout: float = 3.0,
        heartbeat_timeout: float = 30.0,
    ):
        self.store = store
        self.key = key
        self.broker_id = broker_id or f"broker-{uuid.uuid4()}"
        self.flush_interval = flush_interval
        self.broker_timeout = broker_timeout
        self.heartbeat_timeout = heartbeat_timeout

        self._condition = threading.Condition()
        self._pending: list[BufferedOperation] = []
        self._stopped = False
        self._thread = threading.Thread(target=self._run, name=f"queue-broker-{self.broker_id}", daemon=True)
        self._thread.start()

    def close(self) -> None:
        with self._condition:
            self._stopped = True
            self._condition.notify_all()
        self._thread.join(timeout=2)

    def enqueue(self, payload: dict[str, Any], *, job_id: Optional[str] = None) -> str:
        job_id = job_id or f"job-{uuid.uuid4()}"
        self._submit("enqueue", job_id, payload).result(timeout=self.broker_timeout * 2)
        return job_id

    def claim(self, worker_id: str) -> Optional[QueueJob]:
        return self._submit("claim", worker_id).result(timeout=self.broker_timeout * 2)

    def heartbeat(self, worker_id: str, job_id: str) -> None:
        self._submit("heartbeat", worker_id, job_id).result(timeout=self.broker_timeout * 2)

    def complete(self, worker_id: str, job_id: str) -> None:
        self._submit("complete", worker_id, job_id).result(timeout=self.broker_timeout * 2)

    def snapshot(self) -> QueueState:
        return QueueState.from_bytes(self.store.get(self.key).data)

    def _submit(self, kind: str, *args: Any) -> Future:
        future: Future = Future()
        operation = BufferedOperation(kind=kind, args=args, future=future)
        with self._condition:
            self._pending.append(operation)
            self._condition.notify_all()
        return future

    def _run(self) -> None:
        while True:
            with self._condition:
                if not self._pending and not self._stopped:
                    self._condition.wait(timeout=self.flush_interval)
                if self._stopped and not self._pending:
                    return
                pending = self._pending
                self._pending = []

            if not pending:
                continue

            try:
                self._flush_pending(pending)
            except Exception as exc:  # pragma: no cover - defensive, surfaced via futures
                for operation in pending:
                    if not operation.future.done():
                        operation.future.set_exception(exc)

    def _flush_pending(self, pending: list[BufferedOperation]) -> None:
        while True:
            record = self.store.get(self.key)
            state = QueueState.from_bytes(record.data)
            self._adopt_broker_lease(state)
            self._requeue_stale_jobs(state)

            results: list[Any] = []
            now = time.time()
            state.broker = BrokerLease(broker_id=self.broker_id, heartbeat_at=now)
            for operation in pending:
                results.append(self._apply_operation(state, operation, now))

            try:
                self.store.compare_and_swap(self.key, record.version, state.to_bytes())
            except CompareAndSwapError:
                continue

            for operation, result in zip(pending, results, strict=True):
                if not operation.future.done():
                    operation.future.set_result(result)
            return

    def _adopt_broker_lease(self, state: QueueState) -> None:
        now = time.time()
        if state.broker is None:
            state.broker = BrokerLease(broker_id=self.broker_id, heartbeat_at=now)
            return
        if state.broker.broker_id == self.broker_id:
            return
        if now - state.broker.heartbeat_at > self.broker_timeout:
            state.broker = BrokerLease(broker_id=self.broker_id, heartbeat_at=now)

    def _requeue_stale_jobs(self, state: QueueState) -> None:
        now = time.time()
        for job in state.jobs:
            if job.status != "claimed":
                continue
            heartbeat_at = job.heartbeat_at or job.claimed_at or 0.0
            if now - heartbeat_at <= self.heartbeat_timeout:
                continue
            job.status = "queued"
            job.claimed_by = None
            job.claimed_at = None
            job.heartbeat_at = None

    def _apply_operation(self, state: QueueState, operation: BufferedOperation, now: float) -> Any:
        if operation.kind == "enqueue":
            job_id, payload = operation.args
            state.jobs.append(QueueJob(job_id=job_id, payload=payload))
            return job_id

        if operation.kind == "claim":
            (worker_id,) = operation.args
            for job in state.jobs:
                if job.status != "queued":
                    continue
                job.status = "claimed"
                job.claimed_by = worker_id
                job.claimed_at = now
                job.heartbeat_at = now
                job.attempt += 1
                return QueueJob(**asdict(job))
            return None

        if operation.kind == "heartbeat":
            worker_id, job_id = operation.args
            job = self._find_job(state, job_id)
            if job is None or job.status != "claimed" or job.claimed_by != worker_id:
                raise ValueError(f"worker {worker_id!r} does not own job {job_id!r}")
            job.heartbeat_at = now
            return None

        if operation.kind == "complete":
            worker_id, job_id = operation.args
            job = self._find_job(state, job_id)
            if job is None or job.status != "claimed" or job.claimed_by != worker_id:
                raise ValueError(f"worker {worker_id!r} does not own job {job_id!r}")
            job.status = "completed"
            job.completed_at = now
            job.heartbeat_at = now
            return None

        raise ValueError(f"unknown operation kind: {operation.kind}")

    @staticmethod
    def _find_job(state: QueueState, job_id: str) -> Optional[QueueJob]:
        for job in state.jobs:
            if job.job_id == job_id:
                return job
        return None
