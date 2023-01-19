"""Classes for data handling.

See also: https://docs.sqlalchemy.org/en/14/orm/dataclasses.html
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any, Dict, List, Literal, Optional, get_args
from uuid import uuid4

import firecrest
import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict
import sqlalchemy.orm as orm

mapper_registry = orm.registry()
"""The registry of all SQLAlchemy entities."""


@mapper_registry.mapped
@dataclass
class Computer:
    """Data for a single-user to interact with FirecREST."""

    __tablename__ = "computer"
    __sa_dataclass_metadata_key__ = "sa"

    client_url: str
    # per-user authinfo
    client_id: str
    client_secret: str  # note this would not actually be stored in the database
    token_uri: str
    machine_name: str
    work_dir: str
    """The working directory for the user on the remote machine."""
    fsystem: Literal["posix", "windows"] = field(
        default="posix", metadata={"sa": sa.Column(sa.Enum("posix", "windows"))}
    )
    """The file system type on the remote machine."""
    small_file_size_mb: int = 5
    """The maximum size of a file that can be uploaded directly, in MB."""

    # database populated fields
    pk: Optional[int] = field(
        init=False, metadata={"sa": sa.Column(sa.Integer, primary_key=True)}
    )
    """The primary key set by the database."""
    _calculations: List[Calculation] = field(
        init=False,
        default_factory=list,
        repr=False,
        metadata={"sa": orm.relationship("Calculation")},
    )
    """The calculations that are associated with this computer."""

    @property
    def calculations(self) -> List[Calculation]:
        """Return the outputs of the calculation."""
        return self._calculations

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


StatusType = Literal["created", "uploaded", "submitted", "executed", "finalised"]


@mapper_registry.mapped
@dataclass
class Calculation:
    """Data for a single calculation."""

    __tablename__ = "calculation"
    __sa_dataclass_metadata_key__ = "sa"

    computer: Computer = field(
        repr=False,
        metadata={"sa": orm.relationship("Computer", back_populates="_calculations")},
    )

    attributes: Dict[str, Any] = field(
        default_factory=dict,
        repr=False,
        metadata={"sa": sa.Column(MutableDict.as_mutable(sa.JSON()))},
    )
    """JSONable data to store on the node."""

    status: StatusType = field(
        default="created",
        metadata={"sa": sa.Column(sa.Enum(*get_args(StatusType)))},
    )
    """The status of the calculation."""

    exception: Optional[str] = None
    """The exception that was raised, if any."""

    uuid: str = field(
        default_factory=lambda: str(uuid4()),
        metadata={"sa": sa.Column(sa.String(36))},
    )
    """The unique identifier, for remote folder creation."""

    pk: Optional[int] = field(
        init=False, metadata={"sa": sa.Column(sa.Integer(), primary_key=True)}
    )
    """The primary key set by the database."""
    computer_pk: Optional[int] = field(
        init=False, metadata={"sa": sa.Column(sa.ForeignKey("computer.pk"))}
    )
    """The primary key of the computer that this calculation is associated with."""

    _outputs: List[DataNode] = field(
        default_factory=list,
        repr=False,
        metadata={"sa": orm.relationship("DataNode")},
    )

    @property
    def outputs(self) -> List[DataNode]:
        """Return the outputs of the calculation."""
        return self._outputs

    @property
    def remote_path(self) -> PurePosixPath | PureWindowsPath:
        """Return the remote path for the calculation execution."""
        return self.computer.work_path / "workflows" / self.uuid


@mapper_registry.mapped
@dataclass
class DataNode:
    """Data node to input or output from a calculation."""

    __tablename__ = "data"
    __sa_dataclass_metadata_key__ = "sa"

    pk: Optional[int] = field(
        init=False, metadata={"sa": sa.Column(sa.Integer(), primary_key=True)}
    )
    """The primary key set by the database."""
    attributes: Dict[str, Any] = field(
        default_factory=dict,
        repr=False,
        metadata={"sa": sa.Column(MutableDict.as_mutable(sa.JSON()))},
    )
    """JSONable data to store on the node."""

    creator: Optional[Calculation] = field(
        repr=False,
        default=None,
        metadata={"sa": orm.relationship("Calculation", back_populates="_outputs")},
    )
    creator_pk: Optional[int] = field(
        init=False, metadata={"sa": sa.Column(sa.ForeignKey("calculation.pk"))}
    )
