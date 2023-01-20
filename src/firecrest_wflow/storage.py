"""Storage for the calculations."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Iterable, Protocol, Sequence, TypeVar, Union

import sqlalchemy as sa
import sqlalchemy.orm as orm

from firecrest_wflow.data import mapper_registry

from .data import Calculation, Code, Computer

LOGGER = logging.getLogger(__name__)


class PersistProtocol(Protocol):
    """A persister for the calculation."""

    def save(self, calc: Calculation) -> None:
        """Save the calculation."""


class DummyStorage(PersistProtocol):
    """A dummy persister."""

    def save(self, calc: Calculation) -> None:
        """Save the calculation."""
        LOGGER.debug("Saving calculation %s", calc)


OBJ_TYPE = TypeVar("OBJ_TYPE", bound=Union[Computer, Code, Calculation])


class SqliteStorage(PersistProtocol):
    def __init__(
        self,
        path: str | Path | None = None,
        engine_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the storage."""
        url = "sqlite:///:memory:" if path is None else f"sqlite:///{path}"
        self._engine = sa.create_engine(url, **(engine_kwargs or {}))
        mapper_registry.metadata.create_all(self._engine)
        self._session = orm.sessionmaker(bind=self._engine)()

    def save(self, obj: Computer | Code | Calculation) -> None:
        """Save the calculation."""
        LOGGER.info("Saving object %s", obj)
        self._session.add(obj)
        self._session.commit()

    def save_many(self, objs: Iterable[Computer | Code | Calculation]) -> None:
        """Save the calculation."""
        LOGGER.debug("Saving objects %s", objs)
        self._session.add_all(objs)
        self._session.commit()

    def all(self, obj_cls: type[OBJ_TYPE]) -> Iterable[OBJ_TYPE]:
        """Select all computers."""
        for obj in self._session.scalars(sa.select(obj_cls)):
            yield obj

    def get_unfinished(self, max: None | int = None) -> Sequence[Calculation]:
        """Get unfinished calculations."""
        stmt = sa.select(Calculation).where(
            Calculation.status != "finalised"  # type: ignore[arg-type]
        )
        if max is not None:
            stmt = stmt.limit(max)
        return self._session.scalars(stmt).all()
