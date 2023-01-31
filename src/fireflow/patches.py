"""Additions to pyfirecrest functionality"""
from __future__ import annotations

from typing import Iterable, TypedDict, cast

from firecrest import Firecrest


class JobAcct(TypedDict):
    """A job accounting record, from `compute/acct`"""

    jobid: str
    name: str
    nodelist: str
    nodes: str
    partition: str
    start_time: str
    state: str
    time: str
    time_left: str
    user: str


class LsFile(TypedDict):
    """A file listing record, from `utilities/ls`"""

    group: str
    last_modified: str
    link_target: str
    name: str
    permissions: str
    size: str
    type: str
    user: str


class LsFileRecurse(LsFile):
    """A file listing record, from `utilities/ls`, called recursively."""

    path: str
    depth: int


def ls_recurse(
    client: Firecrest,
    machine: str,
    path: str,
    *,
    show_hidden: bool = False,
    delimiter: str = "/",
    max_calls: int | None = None,
    max_depth: int | None = None,
) -> Iterable[LsFileRecurse]:
    """Recursively yield paths, depth first."""
    stack: list[LsFileRecurse] = [
        {"depth": 0, "path": path, "type": "d", "_initial": True}  # type: ignore
    ]
    calls_made = 0
    while stack:
        current_path = stack.pop()
        if not current_path.get("_initial"):
            yield cast(LsFileRecurse, current_path)
        if current_path["type"] == "d" and (
            max_depth is None or current_path["depth"] < max_depth
        ):
            if max_calls and calls_made >= max_calls:
                raise RecursionError("Too many API calls, aborting.")
            calls_made += 1
            for child in client.list_files(
                machine, current_path["path"], show_hidden=show_hidden
            ):
                child["path"] = delimiter.join((current_path["path"], child["name"]))
                child["depth"] = current_path["depth"] + 1
                stack.append(child)
