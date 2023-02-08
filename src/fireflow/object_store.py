"""A simple object store, for storing large binary objects."""
from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import closing
import hashlib
from io import BytesIO
from pathlib import Path
import shutil
import tempfile
from typing import BinaryIO, Iterable, Protocol

COPY_BUFSIZE = 64 * 1024


class BinaryStream(Protocol):
    """A binary stream, that can be read once and only once, optionally in chunks."""

    def read(self, size: int = -1) -> bytes:
        """Read the stream."""


class ObjectStore(ABC):
    """A simple object store.

    Files are stored via their SHA256 hash, to avoid duplicates.
    """

    @abstractmethod
    def count(self) -> int:
        """Count the number of objects in the store."""

    @abstractmethod
    def keys(self) -> Iterable[str]:
        """Iterate over the keys of the objects in the store."""

    @abstractmethod
    def add_from_bytes(self, obj: bytes) -> str:
        """Add an object to the store idempotently and atomically.

        :param obj: the object to store
        :return: the key of the object
        """

    @abstractmethod
    def add_from_io(self, obj: BinaryStream, *, chunks: int = COPY_BUFSIZE) -> str:
        """Add an object to the store idempotently and atomically.

        :param obj: the object to store
        :param chunks: the size of chunks to stream in
        :return: the key of the object
        """

    def add_from_path(self, path: Path | str, *, chunks: int = COPY_BUFSIZE) -> str:
        """Add an object to the store idempotently and atomically.

        :param path: the path to the object
        :param chunks: the size of chunks to stream in
        :return: the key of the object
        """
        _path = Path(path)
        with open(_path, "rb") as obj:
            return self.add_from_io(obj, chunks=chunks)

    def add_from_glob(
        self, path: Path, glob: str, *, chunks: int = COPY_BUFSIZE
    ) -> dict[str, str]:
        """Add objects to the store idempotently and atomically.

        :param path: the path to the objects directory
        :param glob: a glob pattern to match files in the directory
        :param chunks: the size of chunks to stream in
        :return: a mapping from the path to the key of the object
        """
        added = {}
        for glob_path in path.glob(glob):
            added[str(glob_path)] = self.add_from_path(glob_path, chunks=chunks)
        return added

    @abstractmethod
    def __contains__(self, sha256: str) -> bool:
        """Check if the object is in the store."""

    @abstractmethod
    def get_size(self, sha256: str) -> int:
        """Get the size of the object in bytes.

        :raises KeyError: if the object is not in the store.
        """

    @abstractmethod
    def open(self, sha256: str) -> BinaryIO:
        """Open the object for reading.

        :raises KeyError: if the object is not in the store.
        """


class InMemoryObjectStore(ObjectStore):
    def __init__(self) -> None:
        """Initialize the store."""
        self._store: dict[str, bytes] = {}

    def count(self) -> int:
        return len(self._store)

    def keys(self) -> Iterable[str]:
        return self._store.keys()

    def add_from_io(self, obj: BinaryStream, *, chunks: int = COPY_BUFSIZE) -> str:
        return self.add_from_bytes(obj.read())

    def add_from_bytes(self, obj: bytes) -> str:
        sha256 = hashlib.sha256(obj).hexdigest()
        if sha256 not in self._store:
            self._store[sha256] = obj
        return sha256

    def __contains__(self, sha256: str) -> bool:
        return sha256 in self._store

    def get_size(self, sha256: str) -> int:
        return len(self._store[sha256])

    def open(self, sha256: str) -> BinaryIO:
        return closing(BytesIO(self._store[sha256]))  # type: ignore[return-value]


class FileObjectStore(ObjectStore):
    def __init__(self, path: Path | str) -> None:
        """Initialize the store."""
        self._path = Path(path)

    def count(self) -> int:
        return sum(1 for _ in self._path.iterdir())

    def keys(self) -> Iterable[str]:
        return (p.name for p in self._path.iterdir())

    def add_from_bytes(self, obj: bytes) -> str:
        sha256 = hashlib.sha256(obj).hexdigest()

        path = self._path / sha256
        if path.exists():
            return sha256

        try:
            path.write_bytes(obj)
        except Exception:
            if path.exists():
                path.unlink()
            raise
        return sha256

    def add_from_io(self, obj: BinaryStream, *, chunks: int = COPY_BUFSIZE) -> str:
        """Add an object to the store idempotently and atomically.

        To be atomic, the object is first written to a temporary file,
        whilst computing its hash.
        If the object is already in the store, the temporary file is deleted.
        If the object is not in the store, the temporary file is moved to the store.
        """
        hasher = hashlib.sha256()
        with tempfile.NamedTemporaryFile("wb", delete=False) as temp:
            while True:
                chunk = obj.read(chunks)
                if not chunk:
                    break
                hasher.update(chunk)
                temp.write(chunk)

        sha256 = hasher.hexdigest()
        path = self._path / sha256

        if path.exists():
            Path(temp.name).unlink()
            return sha256

        try:
            shutil.move(temp.name, path)
        except Exception:
            if path.exists():
                path.unlink()
            raise
        return sha256

    def __contains__(self, sha256: str) -> bool:
        return (self._path / sha256).exists()

    def _get_path(self, sha256: str) -> Path:
        _path = self._path / sha256
        if not _path.exists():
            raise KeyError(sha256)
        return _path

    def get_size(self, sha256: str) -> int:
        path = self._get_path(sha256)
        return path.stat().st_size

    def open(self, sha256: str) -> BinaryIO:
        path = self._get_path(sha256)
        return path.open("rb")
