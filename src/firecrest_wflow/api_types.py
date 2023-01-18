from typing import TypedDict


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
