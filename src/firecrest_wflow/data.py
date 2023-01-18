"""Classes for data handling."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any, Literal
from uuid import uuid4

import firecrest


@dataclass
class Computer:
    """A mock computer (including transport/scheduler)."""

    client_url: str
    # per-user authinfo
    client_id: str
    client_secret: str  # note this would not actually be stored in the database
    token_uri: str
    machine_name: str
    work_dir: str
    fsystem: Literal["posix", "windows"] = "posix"
    # decide whether a file can be uploaded directly,
    # over the REST API, or whether it needs to be uploaded
    small_file_size_mb: int = 5

    @property
    def work_path(self) -> PurePosixPath | PureWindowsPath:
        """Return the work directory path."""
        return (
            PurePosixPath(self.work_dir)
            if self.fsystem == "posix"
            else PureWindowsPath(self.work_dir)
        )

    @property
    def client(self) -> firecrest.Firecrest:
        """Return a FirecREST client.

        Cache the client instance, so that we don't have to re-authenticate
        (it automatically refreshes the token when it expires)
        """
        if not hasattr(self, "_client"):
            self._client = firecrest.Firecrest(
                firecrest_url=self.client_url,
                authorization=firecrest.ClientCredentialsAuth(
                    self.client_id, self.client_secret, self.token_uri
                ),
            )
        return self._client


@dataclass
class Data:
    """A mock data object."""

    uuid: str = field(default_factory=lambda: str(uuid4()))
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class CalcNode(Data):
    """A mock calculation data node."""
