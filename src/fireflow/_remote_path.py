"""Implement a pathlib.Path-like class for remote paths.

That can be used with https://pypi.org/project/virtual_glob/.
See also: https://github.com/eth-cscs/pyfirecrest/pull/43
"""
from __future__ import annotations

from pathlib import PurePath
import typing as t  # import Iterable, Literal, TypedDict

from firecrest import Firecrest
from firecrest.FirecrestException import HeaderException

FType = t.Literal[
    "b",  # block device
    "c",  # character device
    "d",  # directory
    "l",  # Symbolic link
    "s",  # Socket.
    "p",  # FIFO
    "-",  # Regular file
]


class LsFile(t.TypedDict):
    """A file listing record, from `utilities/ls`"""

    group: str
    last_modified: str
    link_target: str
    name: str
    permissions: str
    size: str
    type: FType
    user: str


class StatFile(t.TypedDict):
    """A file stat record, from `utilities/stat`

    Command is `stat {deref} -c '%a %i %d %h %u %g %s %X %Y %Z`

    See also https://docs.python.org/3/library/os.html#os.stat_result
    """

    atime: int
    ctime: int
    dev: int  # device
    gid: int  # group id of owner
    ino: int  # inode number
    mode: int  # protection bits
    mtime: int
    nlink: int  # number of hard links
    size: int  # size of file, in bytes
    uid: int  # user id of owner


RPathType = t.TypeVar("RPathType", bound="RemotePath")


class RemotePath:
    def __init__(
        self,
        client: Firecrest,
        machine: str,
        path: PurePath,
        ftype: FType | t.Literal[False] | None,
        size: int | t.Literal[False] | None,
    ):
        self._client = client
        self._machine = machine
        self._path = path
        self._ftype = ftype  # note does not follow symlinks, False if it doesn't exist, None if unknown
        self._size = size

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._machine}@{self._path})"

    @property
    def name(self) -> str:
        """Return the name of this path."""
        return self._path.name

    @property
    def path(self) -> str:
        """Return the full path as a string."""
        return self._path.__fspath__()

    @property
    def pure_path(self) -> PurePath:
        """Return the path as a PurePath."""
        return self._path

    @property
    def size(self) -> int | None:
        """Return the size of this path in bytes, or None if it doesn't exist."""
        if self._size is False:
            return None
        if self._ftype is None:
            try:
                stat: StatFile = self._client.stat(self._machine, str(self._path))
            except HeaderException as exc:
                for header in exc.responses[-1].headers:
                    if header == "X-Not-Found":
                        self._size = False
                        return None
                raise
            self._size = int(stat["size"])
        return self._size

    def _get_ftype(self) -> FType | None:
        """Return the file type of this path, not following symlinks, or None if it doesn't exist."""
        if self._ftype is False:
            return None
        if self._ftype is None:
            if self.size is None:
                self._ftype = False
                return None
            # TODO unfortunately stat does not actually give us the file type yet
            # see: https://github.com/eth-cscs/firecrest/issues/171
            raise NotImplementedError("stat does not return file type yet")
        return self._ftype

    def exists(self) -> bool:
        """Return True if this path exists."""
        return self.size is not None

    def is_symlink(self) -> bool:
        """Return True if this path is a symbolic link."""
        return self._get_ftype() == "l"

    def is_dir(self) -> bool:
        """Return True if this path is a directory, following symlinks."""
        if self._get_ftype() == "l":
            raise NotImplementedError("is_dir for symlinks")
        return self._get_ftype() == "d"

    def is_file(self) -> bool:
        """Return True if this path is a regular file, following symlinks."""
        if self._get_ftype() == "l":
            raise NotImplementedError("is_file for symlinks")
        return self._get_ftype() == "-"

    def iterdir(self: RPathType) -> t.Iterable[RPathType]:
        """Iterate over the contents of this directory."""
        child: LsFile
        for child in self._client.list_files(
            self._machine, str(self._path), show_hidden=True
        ):
            yield self.__class__(
                self._client,
                self._machine,
                self._path / child["name"],
                child["type"],
                int(child["size"]),
            )

    def joinpath(self: RPathType, *parts: str) -> RPathType:
        """Join this path with the given parts."""
        return self.__class__(
            self._client,
            self._machine,
            self._path.joinpath(*parts),
            None,
            None,
        )
