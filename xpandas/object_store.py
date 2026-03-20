from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Protocol


class CompareAndSwapError(RuntimeError):
    """Raised when an object's version does not match the expected version."""


@dataclass(frozen=True)
class ObjectRecord:
    key: str
    data: bytes
    version: Optional[str]


class ObjectStore(Protocol):
    def get(self, key: str) -> ObjectRecord:
        ...

    def compare_and_swap(self, key: str, expected_version: Optional[str], data: bytes) -> ObjectRecord:
        ...


class LocalObjectStore:
    """Filesystem-backed object store with CAS semantics.

    Each key maps to a file under ``root``. The version token is a SHA256 hash of the
    file bytes, which behaves similarly to an object-store ETag for this use case.
    """

    def __init__(self, root: str | os.PathLike[str]):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)

    def get(self, key: str) -> ObjectRecord:
        path = self._path_for(key)
        if not path.exists():
            return ObjectRecord(key=key, data=b"", version=None)
        data = path.read_bytes()
        return ObjectRecord(key=key, data=data, version=self._version(data))

    def compare_and_swap(self, key: str, expected_version: Optional[str], data: bytes) -> ObjectRecord:
        path = self._path_for(key)
        path.parent.mkdir(parents=True, exist_ok=True)

        current = self.get(key)
        if current.version != expected_version:
            raise CompareAndSwapError(
                f"CAS failed for {key!r}: expected {expected_version!r}, got {current.version!r}"
            )

        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as tmp:
            tmp.write(data)
            tmp.flush()
            os.fsync(tmp.fileno())
            temp_name = tmp.name

        os.replace(temp_name, path)
        return ObjectRecord(key=key, data=data, version=self._version(data))

    def _path_for(self, key: str) -> Path:
        normalized = key.lstrip("/")
        return self.root / normalized

    @staticmethod
    def _version(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()


class S3LikeClient(Protocol):
    def get_object(self, *, Bucket: str, Key: str) -> dict:
        ...

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, IfMatch: Optional[str] = None, IfNoneMatch: Optional[str] = None) -> dict:
        ...


class S3ObjectStore:
    """Adapter around an injected S3-like client.

    The client is intentionally injected so production code can use boto3 while tests can
    provide a local mock implementation without requiring AWS dependencies.
    """

    def __init__(self, client: S3LikeClient, bucket: str):
        self.client = client
        self.bucket = bucket

    def get(self, key: str) -> ObjectRecord:
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
        except FileNotFoundError:
            return ObjectRecord(key=key, data=b"", version=None)

        body = response["Body"]
        if hasattr(body, "read"):
            data = body.read()
        else:
            data = body
        return ObjectRecord(key=key, data=data, version=response.get("ETag"))

    def compare_and_swap(self, key: str, expected_version: Optional[str], data: bytes) -> ObjectRecord:
        kwargs = {"Bucket": self.bucket, "Key": key, "Body": data}
        if expected_version is None:
            kwargs["IfNoneMatch"] = "*"
        else:
            kwargs["IfMatch"] = expected_version

        try:
            response = self.client.put_object(**kwargs)
        except PermissionError as exc:
            raise CompareAndSwapError(str(exc)) from exc
        return ObjectRecord(key=key, data=data, version=response.get("ETag"))


class LocalS3MockClient:
    """A local S3 mock that persists objects to disk and supports CAS preconditions."""

    def __init__(self, root: str | os.PathLike[str]):
        self.store = LocalObjectStore(root)

    def get_object(self, *, Bucket: str, Key: str) -> dict:
        record = self.store.get(f"{Bucket}/{Key}")
        if record.version is None:
            raise FileNotFoundError(Key)
        return {"Body": record.data, "ETag": record.version}

    def put_object(
        self,
        *,
        Bucket: str,
        Key: str,
        Body: bytes,
        IfMatch: Optional[str] = None,
        IfNoneMatch: Optional[str] = None,
    ) -> dict:
        object_key = f"{Bucket}/{Key}"
        current = self.store.get(object_key)
        expected = IfMatch
        if IfNoneMatch == "*":
            if current.version is not None:
                raise PermissionError(f"Precondition failed for {Key!r}: object already exists")
            expected = None
        try:
            record = self.store.compare_and_swap(object_key, expected, Body)
        except CompareAndSwapError as exc:
            raise PermissionError(str(exc)) from exc
        return {"ETag": record.version}
