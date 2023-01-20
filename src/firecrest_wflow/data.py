"""Classes for data handling.

See also: https://docs.sqlalchemy.org/en/20/orm/quickstart.html
"""
from pathlib import PurePosixPath, PureWindowsPath
from typing import Any, Dict, List, Literal, Optional, Union, get_args
from uuid import uuid4

import firecrest
import sqlalchemy as sa
from sqlalchemy.ext.mutable import MutableDict
import sqlalchemy.orm as orm
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    """Base class for all tables."""

    __abstract__ = True

    pk: Mapped[int] = mapped_column(primary_key=True)
    """The primary key set by the database."""

    def __repr__(self) -> str:
        """Return a string representation of the object."""
        return f"{self.__class__.__name__}({self.pk})"

    def __str__(self) -> str:
        """Return a string representation of the object."""
        return f"{self.__class__.__name__}({self.pk})"

    def __eq__(self, other: Any) -> bool:
        """Return True if the objects are equal."""
        # test if same class
        if not isinstance(other, self.__class__):
            return False
        return self.pk == other.pk

    def __hash__(self) -> int:
        """Return the hash of the object."""
        return hash(self.pk)


class Computer(Base):
    """Data for a single-user to interact with FirecREST."""

    __tablename__ = "computer"

    client_url: Mapped[str]
    # per-user authinfo
    client_id: Mapped[str]
    # note this would not actually be stored in the database
    token_uri: Mapped[str]
    client_secret: Mapped[str]
    machine_name: Mapped[str]
    work_dir: Mapped[str]
    """The working directory for the user on the remote machine."""
    fsystem: Mapped[Literal["posix", "windows"]] = mapped_column(
        sa.Enum("posix", "windows"), default="posix"
    )
    """The file system type on the remote machine."""
    small_file_size_mb: Mapped[int] = mapped_column(default=5)
    """The maximum size of a file that can be uploaded directly, in MB."""

    """The primary key set by the database."""
    _codes: Mapped[List["Code"]] = orm.relationship("Code")
    """The codes that are associated with this computer."""

    @property
    def codes(self) -> List["Code"]:
        """Return the outputs of the calculation."""
        return self._codes

    @property
    def work_path(self) -> Union[PurePosixPath, PureWindowsPath]:
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


class Code(Base):
    """Data for a single code."""

    __tablename__ = "code"

    computer_pk: Mapped[int] = mapped_column(sa.ForeignKey("computer.pk"))
    """The primary key of the computer that this calculation is associated with."""
    computer: Mapped[Computer] = orm.relationship("Computer", back_populates="_codes")
    """The computer that this calculation is associated with."""

    script: Mapped[str]
    """The batch script template to submit to the scheduler on the remote machine.

    This can use the `calc` jinja2 placeholders,
    and will be prepended by:
    ```bash
    #!/bin/bash
    #SBATCH --job-name={{calc.uuid}}
    ```
    """

    _calculations: Mapped[List["Calculation"]] = orm.relationship("Calculation")
    """The calculations that are associated with this code."""

    @property
    def calculations(self) -> List["Calculation"]:
        """Return the outputs of the calculation."""
        return self._calculations


StepType = Literal[
    "created", "uploading", "submitting", "running", "retrieving", "finalised"
]


class Calculation(Base):
    """Data for a single calculation."""

    __tablename__ = "calculation"

    code_pk: Mapped[int] = mapped_column(sa.ForeignKey("code.pk"))
    """The primary key of the code that this calculation is associated with."""
    code: Mapped[Code] = orm.relationship("Code", back_populates="_calculations")
    """The code that this calculation is associated with."""

    input_files: Mapped[Dict[str, str]] = mapped_column(
        MutableDict.as_mutable(sa.JSON()), default=dict
    )
    """The text files to upload to the remote machine: {filename: contents}.

    - `filename` should be relative to the process dirctory and use POSIX path separators.
    - `contents` should be UTF-8 encoded.
    """

    attributes: Mapped[Dict[str, Any]] = mapped_column(
        MutableDict.as_mutable(sa.JSON()), default=dict
    )
    """JSONable data to store on the node."""

    step: Mapped[StepType] = mapped_column(
        sa.Enum(*get_args(StepType)), default="created"
    )
    """The step of the calculation."""

    exception: Mapped[Optional[str]]
    """The exception that was raised, if any."""

    uuid: Mapped[str] = mapped_column(sa.String(36), default=lambda: str(uuid4()))
    """The unique identifier, for remote folder creation."""

    outputs: Mapped[List["DataNode"]] = orm.relationship(
        "DataNode", cascade="all, delete-orphan"
    )

    @property
    def remote_path(self) -> Union[PurePosixPath, PureWindowsPath]:
        """Return the remote path for the calculation execution."""
        return self.code.computer.work_path / "workflows" / self.uuid


class DataNode(Base):
    """Data node to input or output from a calculation."""

    __tablename__ = "data"

    attributes: Mapped[Dict[str, Any]] = mapped_column(
        MutableDict.as_mutable(sa.JSON()), default=dict
    )
    """JSONable data to store on the node."""

    # TODO allow for this to not be set?
    creator_pk: Mapped[int] = mapped_column(sa.ForeignKey("calculation.pk"))
    """The primary key of the calculation that created this node."""
    creator: Mapped[Calculation] = orm.relationship(
        "Calculation", back_populates="outputs"
    )
    """The calculation that created this node."""
