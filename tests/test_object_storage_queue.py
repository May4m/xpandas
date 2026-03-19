import time
from pathlib import Path

from xpandas import LocalObjectStore, LocalS3MockClient, ObjectStorageQueueBroker, S3ObjectStore


def test_local_object_store_compare_and_swap(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path)

    record = store.compare_and_swap("queue.json", None, b"first")
    assert record.data == b"first"

    updated = store.compare_and_swap("queue.json", record.version, b"second")
    assert updated.data == b"second"
    assert store.get("queue.json").version == updated.version


def test_s3_adapter_works_with_local_mock(tmp_path: Path) -> None:
    client = LocalS3MockClient(tmp_path / "mock-s3")
    store = S3ObjectStore(client, bucket="unit-test")

    first = store.compare_and_swap("queue.json", None, b"hello")
    second = store.compare_and_swap("queue.json", first.version, b"world")

    assert store.get("queue.json").data == b"world"
    assert second.version is not None


def test_broker_batches_queue_lifecycle_with_heartbeat_and_reclaim(tmp_path: Path) -> None:
    store = LocalObjectStore(tmp_path)
    broker = ObjectStorageQueueBroker(store, flush_interval=0.01, broker_timeout=0.2, heartbeat_timeout=0.05)
    try:
        job_id = broker.enqueue({"task": "index", "namespace": "demo"})

        claimed = broker.claim("worker-a")
        assert claimed is not None
        assert claimed.job_id == job_id
        assert claimed.attempt == 1

        broker.heartbeat("worker-a", job_id)
        broker.complete("worker-a", job_id)

        snapshot = broker.snapshot()
        completed_job = next(job for job in snapshot.jobs if job.job_id == job_id)
        assert completed_job.status == "completed"
        assert completed_job.claimed_by == "worker-a"
    finally:
        broker.close()

    reclaim_store = LocalObjectStore(tmp_path / "reclaim")
    broker_one = ObjectStorageQueueBroker(
        reclaim_store,
        key="queue.json",
        broker_id="broker-one",
        flush_interval=0.01,
        broker_timeout=0.2,
        heartbeat_timeout=0.05,
    )
    try:
        stale_job_id = broker_one.enqueue({"task": "index", "namespace": "stale"})
        stale_claim = broker_one.claim("worker-stale")
        assert stale_claim is not None
        assert stale_claim.job_id == stale_job_id
        time.sleep(0.08)
    finally:
        broker_one.close()

    broker_two = ObjectStorageQueueBroker(
        reclaim_store,
        key="queue.json",
        broker_id="broker-two",
        flush_interval=0.01,
        broker_timeout=0.05,
        heartbeat_timeout=0.05,
    )
    try:
        reclaimed = broker_two.claim("worker-fresh")
        assert reclaimed is not None
        assert reclaimed.job_id == stale_job_id
        assert reclaimed.attempt == 2
    finally:
        broker_two.close()
