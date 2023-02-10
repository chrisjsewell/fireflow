"""A simple object store, for storing large binary objects."""
from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import closing
import hashlib
from io import BytesIO
import os
from pathlib import Path
import tempfile
from typing import Any, BinaryIO, Iterable, Protocol, TypeVar

COPY_BUFSIZE = 64 * 1024


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
        """Return a file reader for the object.

        :raises KeyError: if the object is not in the store.
        """

    def open_for_write(self) -> ObjectWriter:
        """Return a writer to the store, for a single object.

        The writes will be idempotent and atomic,
        such that writing can only happen once, inside the context,
        and the object will be placed in the store only if the context exits successfully.
        """
        # TODO do we want an async version of this? (e.g. using aiofiles)
        return DefaultObjectWriter(self)


class BinaryStream(Protocol):
    """A binary stream, that can be read once and only once, optionally in chunks."""

    def read(self, size: int = -1) -> bytes:
        """Read the stream."""


# TODO replace with Self type for Python 3.10
_Self1 = TypeVar("_Self1", bound="ObjectWriter")
_Self2 = TypeVar("_Self2", bound="DefaultObjectWriter")
_Self3 = TypeVar("_Self3", bound="FileObjectWriter")


class ObjectWriter(ABC):
    """A context for writing objects to the store,
    that can be used to write a single object to the store.
    """

    @abstractmethod
    def __enter__(self: _Self1) -> _Self1:
        """Enter the context for writing.

        :raises ValueError: if the writer has already been closed.
        """

    @abstractmethod
    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: Any,
    ) -> None:
        """Exit the context, and place the object in the object store."""

    @abstractmethod
    def write(self, data: bytes) -> None:
        """Write data to the file.

        :raises ValueError: if the writer is not in the context.
        """

    @property
    @abstractmethod
    def key(self) -> None | str:
        """Get the key of the object that was written."""


_T2 = TypeVar("_T2", bound="DefaultObjectWriter")


class DefaultObjectWriter(ObjectWriter):
    def __init__(self, store: ObjectStore) -> None:
        self._store = store
        self._key: str | None = None
        self._data: bytes = b""
        self._open: None | bool = (
            None  # None before entering, True after, False after exiting
        )

    @property
    def key(self) -> None | str:
        return self._key

    def __enter__(self: _Self2) -> _Self2:
        if self._open is False:
            raise ValueError("Writer already closed")
        self._open = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: Any,
    ) -> None:
        self._open = False
        if exc_type is not None or not self._data:
            return
        self._key = self._store.add_from_bytes(self._data)

    def write(self, data: bytes) -> None:
        if self._open is not True:
            raise ValueError("Writer not in context")
        self._data += data


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
        else:
            os.rename(temp.name, path)
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

    def open_for_write(self) -> ObjectWriter:
        return FileObjectWriter(self._path)


class FileObjectWriter(ObjectWriter):
    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._key: str | None = None
        self._open: None | bool = (
            None  # None before entering, True after, False after exiting
        )
        self._hasher: Any | None = None  # TODO type of hasher?
        self._file: BinaryIO | None = None

    @property
    def key(self) -> str | None:
        return self._sha256

    def __enter__(self: _Self3) -> _Self3:
        if self._open is False:
            raise ValueError("Writer already closed")
        if self._open is True:
            return self
        self._open = True
        self._hasher = hashlib.sha256()
        self._file = tempfile.NamedTemporaryFile("wb", delete=False)  # type: ignore[assignment]
        return self

    def write(self, obj: bytes) -> None:
        assert self._file is not None
        assert self._hasher is not None
        self._hasher.update(obj)
        self._file.write(obj)

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: Any,
    ) -> None:
        self._open = False
        try:
            if exc_type is None:

                assert self._hasher is not None
                sha256: str = self._hasher.hexdigest()
                path = self._path / sha256

                if not path.exists():
                    assert self._file is not None
                    os.rename(self._file.name, path)
        finally:
            if self._file and Path(self._file.name).exists():
                Path(self._file.name).unlink()
            self._sha256 = sha256
            self._file = None
            self._hasher = None
