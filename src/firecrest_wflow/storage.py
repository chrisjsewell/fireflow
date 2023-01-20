"""Storage for the calculations."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Iterable, Protocol, Sequence, TypeVar

import sqlalchemy as sa
import sqlalchemy.orm as orm

from .data import Base, Calculation

LOGGER = logging.getLogger(__name__)


class PersistProtocol(Protocol):
    """A persister for the calculation."""

    def save(self, obj: Base) -> None:
        """Save the calculation."""

    def save_many(self, objs: Iterable[Base]) -> None:
        """Save the calculation."""


class DummyStorage(PersistProtocol):
    """A dummy persister."""

    def save(self, obj: Base) -> None:
        """Save the calculation."""
        LOGGER.debug("Saving %s", obj)

    def save_many(self, objs: Iterable[Base]) -> None:
        """Save the calculation."""
        LOGGER.debug("Saving %s", objs)


OBJ_TYPE = TypeVar("OBJ_TYPE", bound=Base)


class SqliteStorage(PersistProtocol):
    def __init__(
        self,
        path: str | Path | None = None,
        engine_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the storage."""
        url = "sqlite:///:memory:" if path is None else f"sqlite:///{path}"
        self._engine = sa.create_engine(url, **(engine_kwargs or {}))
        Base.metadata.create_all(self._engine)
        self._session = orm.sessionmaker(bind=self._engine)()

    def save(self, obj: Base) -> None:
        """Save the calculation."""
        LOGGER.debug("Saving object %s", obj)
        self._session.add(obj)
        self._session.commit()

    def save_many(self, objs: Iterable[Base]) -> None:
        """Save the calculation."""
        LOGGER.debug("Saving objects %s", objs)
        self._session.add_all(objs)
        self._session.commit()

    def all(self, obj_cls: type[OBJ_TYPE]) -> Iterable[OBJ_TYPE]:
        """Select all computers."""
        for obj in self._session.scalars(sa.select(obj_cls)):
            yield obj

    def get_unfinished(self, limit: None | int = None) -> Sequence[Calculation]:
        """Get unfinished calculations, that have not previously excepted."""
        stmt = (
            sa.select(Calculation)
            .where(Calculation.step != "finalised")
            .where(Calculation.exception == None)  # noqa: E711
        )
        if limit is not None:
            stmt = stmt.limit(limit)
        return self._session.scalars(stmt).all()
