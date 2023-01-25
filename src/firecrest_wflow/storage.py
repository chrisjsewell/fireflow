"""Storage for the calculations."""
from __future__ import annotations

import logging
from pathlib import Path
import posixpath
from typing import Iterable, Sequence, TypeVar

import sqlalchemy as sa
import sqlalchemy.orm as orm

from ._object_store import FileObjectStore, InMemoryObjectStore, ObjectStore
from ._orm import Base, Calculation, Code, Computer, Processing

LOGGER = logging.getLogger(__name__)


ORM_TYPE = TypeVar("ORM_TYPE", bound=Base)


class Storage:
    """Persistent storage for the calculations."""

    @classmethod
    def in_memory(cls) -> Storage:
        """Create an in-memory storage."""
        engine = sa.create_engine("sqlite:///:memory:")
        Base.metadata.create_all(engine)
        return cls(engine, InMemoryObjectStore())

    @classmethod
    def on_file(cls, path: Path, *, init: bool = False) -> Storage:
        """Connect to an on-file storage."""
        engine = sa.create_engine(f"sqlite:///{path / 'storage.sqlite'}")
        object_path = path / "objects"
        if init:
            object_path.mkdir(parents=True, exist_ok=True)
            Base.metadata.create_all(engine)
        return cls(
            engine,
            FileObjectStore(object_path),
        )

    def __init__(
        self,
        engine: sa.Engine,
        object_store: ObjectStore,
    ) -> None:
        """Initialize the storage."""

        self._session = orm.sessionmaker(engine)()
        self._object_store = object_store

    @property
    def objects(self) -> ObjectStore:
        """Get the object store."""
        return self._object_store

    def _save_to_db(self, obj: Base) -> None:
        """Save the calculation."""
        LOGGER.debug("Saving row %s", obj)
        self._session.add(obj)
        self._session.commit()

    def save_computer(self, computer: Computer) -> Computer:
        """Add a computer."""
        if (
            computer.pk is not None
            and self._session.get(Computer, computer.pk) is not None
        ):
            raise ValueError(f"{computer} already saved")
        self._save_to_db(computer)
        return computer

    def save_code(self, code: Code) -> Code:
        """Add a code."""
        if code.pk is not None and self._session.get(Code, code.pk) is not None:
            raise ValueError(f"{code} already saved")
        # validate upload paths
        for path, key in (code.upload_paths or {}).items():
            if posixpath.isabs(path):
                raise ValueError(f"Upload path must be relative: {path}")
            if key is not None and key not in self._object_store:
                raise ValueError(f"Upload path key not in object store: {key}")
        self._save_to_db(code)
        return code

    def save_calculation(self, calculation: Calculation) -> Calculation:
        """Add a calculation."""
        if (
            calculation.pk is not None
            and self._session.get(Calculation, calculation.pk) is not None
        ):
            raise ValueError(f"{calculation} already saved")
        # validate download paths
        for path, key in (calculation.upload or {}).items():
            if posixpath.isabs(path):
                raise ValueError(f"Download path must be relative: {path}")
            if key is not None and key not in self._object_store:
                raise ValueError(f"Download path key not in object store: {key}")
        if calculation.status is None:
            calculation.status = Processing()
        self._save_to_db(calculation)
        return calculation

    def update_processing(self, processing: Processing) -> None:
        """Update the processing status."""
        self._save_to_db(processing)

    def all(self, obj_cls: type[ORM_TYPE]) -> Iterable[ORM_TYPE]:
        """Select all computers."""
        for obj in self._session.scalars(sa.select(obj_cls)):
            yield obj

    def get_unfinished(self, limit: None | int = None) -> Sequence[Processing]:
        """Get unfinished calculations, that have not previously excepted."""
        stmt = (
            sa.select(Processing)
            .where(Processing.step != "finalised")
            .where(Processing.exception == None)  # noqa: E711
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        return self._session.scalars(stmt).all()

    def from_yaml(self, path: str) -> None:
        """Load from a yaml file."""
        # TODO this is a bit of a hack, but it's a good way to get started
        # add schema validation of the file
        import yaml

        with open(path) as handle:
            data = yaml.safe_load(handle)

        for computer_data in data["computers"]:
            codes = computer_data.pop("codes")
            computer = self.save_computer(Computer(**computer_data))
            for code_data in codes:
                calculations = code_data.pop("calculations")
                code = self.save_code(Code(**code_data, computer=computer))
                for calculation_data in calculations:
                    self.save_calculation(
                        Calculation(**calculation_data, code=code, status=Processing())
                    )
