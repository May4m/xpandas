# XPandas

XPandas is an experimental parallel, distributed pandas project.

This repository now includes a Python implementation of the object-storage queue design
from turbopuffer's "How to build a distributed queue in a single JSON file on object storage".
The implementation follows the final architecture from the post:

- a single `queue.json` object,
- a broker that is the only writer,
- brokered group commit via buffered operations,
- heartbeat-based worker liveness,
- reclaiming stale claimed jobs for at-least-once delivery,
- pluggable object storage backends.

## Storage backends

- `LocalObjectStore` persists objects to the local filesystem while exposing compare-and-swap semantics.
- `S3ObjectStore` adapts an injected S3-compatible client.
- `LocalS3MockClient` provides a local disk-backed S3 mock for development and tests.

## Main queue API

```python
from xpandas import LocalObjectStore, ObjectStorageQueueBroker

store = LocalObjectStore("./data")
broker = ObjectStorageQueueBroker(store)

job_id = broker.enqueue({"task": "index", "dataset": "events"})
job = broker.claim("worker-1")
if job:
    broker.heartbeat("worker-1", job.job_id)
    broker.complete("worker-1", job.job_id)

broker.close()
```
