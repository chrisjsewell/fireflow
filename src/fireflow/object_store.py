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
    Files are also stored with an optional extension,
    to provide a simple way to record their encoding.
    Two files with the same hash but different extensions are not allowed.
    """

    @abstractmethod
    def count(self) -> int:
        """Count the number of objects in the store."""

    @abstractmethod
    def keys(self) -> Iterable[str]:
        """Iterate over the keys of the objects in the store."""

    @abstractmethod
    def add_from_bytes(self, obj: bytes, ext: str = "") -> str:
        """Add an object to the store idempotently and atomically.

        :param obj: the object to store
        :param ext: the file extension of the object, e.g. "json"
        :return: the key of the object
        :raises ValueError: if the object is already in the store with a different extension.
        """

    @abstractmethod
    def add_from_io(
        self, obj: BinaryStream, *, ext: str = "", chunks: int = COPY_BUFSIZE
    ) -> str:
        """Add an object to the store idempotently and atomically.

        :param obj: the object to store
        :param ext: the file extension of the object, e.g. "json"
        :param chunks: the size of chunks to stream in
        :return: the key of the object
        :raises ValueError: if the object is already in the store with a different extension.
        """

    def add_from_path(self, path: Path | str, *, chunks: int = COPY_BUFSIZE) -> str:
        """Add an object to the store idempotently and atomically.

        :param path: the path to the object
        :param chunks: the size of chunks to stream in
        :return: the key of the object
        :raises ValueError: if the object is already in the store with a different extension.
        """
        _path = Path(path)
        with open(_path, "rb") as obj:
            return self.add_from_io(obj, ext=_path.suffix.lstrip("."), chunks=chunks)

    def add_from_glob(
        self, path: Path, glob: str, *, chunks: int = COPY_BUFSIZE
    ) -> dict[str, str]:
        """Add objects to the store idempotently and atomically.

        :param path: the path to the objects directory
        :param glob: a glob pattern to match files in the directory
        :param chunks: the size of chunks to stream in
        :return: a mapping from the path to the key of the object
        :raises ValueError: if an object is already in the store with a different extension.
        """
        added = {}
        for glob_path in path.glob(glob):
            added[str(glob_path)] = self.add_from_path(glob_path, chunks=chunks)
        return added

    @abstractmethod
    def __contains__(self, sha256: str) -> bool:
        """Check if the object is in the store."""

    @abstractmethod
    def get_extension(self, sha256: str) -> str:
        """Get the file extension of the object.

        :raises KeyError: if the object is not in the store.
        """

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
        self._store: dict[str, tuple[str, bytes]] = {}

    def count(self) -> int:
        return len(self._store)

    def keys(self) -> Iterable[str]:
        return self._store.keys()

    def add_from_io(
        self, obj: BinaryStream, *, ext: str = "", chunks: int = COPY_BUFSIZE
    ) -> str:
        return self.add_from_bytes(obj.read(), ext=ext)

    def add_from_bytes(self, obj: bytes, ext: str = "") -> str:
        sha256 = hashlib.sha256(obj).hexdigest()
        if sha256 in self._store:
            if self._store[sha256][0] != ext:
                raise ValueError(
                    f"Object already in store with different extension: {sha256}"
                )
        else:
            self._store[sha256] = (ext, obj)
        return sha256

    def __contains__(self, sha256: str) -> bool:
        return sha256 in self._store

    def get_extension(self, sha256: str) -> str:
        return self._store[sha256][0]

    def get_size(self, sha256: str) -> int:
        return len(self._store[sha256][1])

    def open(self, sha256: str) -> BinaryIO:
        return closing(BytesIO(self._store[sha256][1]))  # type: ignore[return-value]


class FileObjectStore(ObjectStore):
    def __init__(self, path: Path | str) -> None:
        """Initialize the store."""
        self._path = Path(path)

    def count(self) -> int:
        return sum(1 for _ in self._path.iterdir())

    def keys(self) -> Iterable[str]:
        return (p.name for p in self._path.iterdir())

    def add_from_bytes(self, obj: bytes, ext: str = "") -> str:
        sha256 = hashlib.sha256(obj).hexdigest()

        try:
            stored_ext = self.get_extension(sha256)
        except KeyError:
            pass
        else:
            if stored_ext != ext:
                raise ValueError(
                    f"Object already in store with extension {stored_ext}: {sha256}"
                )
            return sha256

        path = self._path / sha256 / f"file.{ext}"
        path.parent.mkdir(exist_ok=True)
        try:
            path.write_bytes(obj)
        except Exception:
            if path.exists():
                path.unlink()
            raise
        return sha256

    def add_from_io(
        self, obj: BinaryStream, *, ext: str = "", chunks: int = COPY_BUFSIZE
    ) -> str:
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

        try:
            stored_ext = self.get_extension(sha256)
        except KeyError:
            pass
        else:
            Path(temp.name).unlink()
            if stored_ext != ext:
                raise ValueError(
                    f"Object already in store with extension {stored_ext}: {sha256}"
                )
            return sha256

        path = self._path / sha256 / f"file.{ext}"
        path.parent.mkdir(exist_ok=True)
        try:
            shutil.move(temp.name, path)
        except Exception:
            if path.exists():
                path.unlink()
            raise
        return sha256

    def _get_path(self, sha256: str) -> Path:
        folder = self._path / sha256
        if not folder.exists():
            raise KeyError(sha256)
        for path in folder.glob("file.*"):
            return path
        raise KeyError(sha256)

    def get_extension(self, sha256: str) -> str:
        path = self._get_path(sha256)
        return path.name.split(".", 1)[-1]

    def get_size(self, sha256: str) -> int:
        path = self._get_path(sha256)
        return path.stat().st_size

    def __contains__(self, sha256: str) -> bool:
        try:
            self._get_path(sha256)
        except KeyError:
            return False
        return True

    def open(self, sha256: str) -> BinaryIO:
        path = self._get_path(sha256)
        return path.open("rb")
