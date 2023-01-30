"""Storage for the calculations."""
from __future__ import annotations

import logging
from pathlib import Path
import posixpath
from typing import Iterable, Sequence, TypeVar

import sqlalchemy as sa
import sqlalchemy.orm as orm

from ._object_store import FileObjectStore, InMemoryObjectStore, ObjectStore
from ._orm import Base, CalcJob, Client, Code, Processing

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
    def on_file(cls, path: Path | str, *, init: bool = False) -> Storage:
        """Connect to an on-file storage."""
        path = Path(path)
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
        """Save an ORM object to the database."""
        LOGGER.debug("Saving row %s", obj)
        self._session.add(obj)
        self._session.commit()

    def save_client(self, client: Client) -> Client:
        """Add a client."""
        if client.pk is not None and self._session.get(Client, client.pk) is not None:
            raise ValueError(f"{client} already saved")
        self._save_to_db(client)
        return client

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

    def save_calcjob(self, calcjob: CalcJob) -> CalcJob:
        """Add a calcjob."""
        if (
            calcjob.pk is not None
            and self._session.get(CalcJob, calcjob.pk) is not None
        ):
            raise ValueError(f"{calcjob} already saved")
        # validate download paths
        for path, key in (calcjob.upload or {}).items():
            if posixpath.isabs(path):
                raise ValueError(f"Download path must be relative: {path}")
            if key is not None and key not in self._object_store:
                raise ValueError(f"Download path key not in object store: {key}")
        if calcjob.status is None:
            calcjob.status = Processing()
        self._save_to_db(calcjob)
        return calcjob

    def update_processing(self, processing: Processing) -> None:
        """Update the processing status."""
        self._save_to_db(processing)

    def count_obj(
        self, obj_cls: type[ORM_TYPE], *, filters: Sequence[sa.ColumnElement[bool]] = ()
    ) -> int:
        """Count ORM objects of a particular type

        :param obj_cls: The class of the objects to select
        :param filters: Additional filters to apply (joined with AND)
        """
        selector = sa.select(obj_cls)
        selector = selector.order_by(obj_cls.pk)
        if filters:
            selector = selector.where(sa.and_(*filters))
        return self._session.execute(  # type: ignore
            sa.select(sa.func.count()).select_from(selector.subquery())
        ).scalar_one()

    def iter_obj(
        self,
        obj_cls: type[ORM_TYPE],
        *,
        page_size: int | None = None,
        page: int = 1,
        filters: Sequence[sa.ColumnElement[bool]] = (),
    ) -> Iterable[ORM_TYPE]:
        """Iterate over ORM objects of a particular type

        :param obj_cls: The class of the objects to select
        :param page_size: The number of objects to select per page
        :param page_number: The page number to select
        :param filters: Additional filters to apply (joined with AND)
        """
        selector = sa.select(obj_cls)
        selector = selector.order_by(obj_cls.pk)
        if page_size is not None:
            selector = selector.limit(page_size).offset((page - 1) * page_size)
        if filters:
            selector = selector.where(sa.and_(*filters))
        for obj in self._session.scalars(selector):
            yield obj

    def get_unfinished(self, limit: None | int = None) -> Sequence[Processing]:
        """Get unfinished calcjobs, that have not previously excepted."""
        stmt = (
            sa.select(Processing)
            .where(Processing.step != "finalised")
            .where(Processing.exception == None)  # noqa: E711
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        return self._session.scalars(stmt).all()

    def from_yaml(self, path: str | Path) -> None:
        """Load from a yaml file."""
        # TODO this is a bit of a hack, but it's a good way to get started
        # add schema validation of the file
        import yaml

        with open(path) as handle:
            data = yaml.safe_load(handle)

        for client_data in data["clients"]:
            codes = client_data.pop("codes")
            client = self.save_client(Client(**client_data))
            for code_data in codes:
                calcjobs = code_data.pop("calcjobs")
                code = self.save_code(Code(**code_data, client=client))
                for calcjob_data in calcjobs:
                    self.save_calcjob(CalcJob(**calcjob_data, code=code))
