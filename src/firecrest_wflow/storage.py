"""Storage for the calculations."""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Protocol, Sequence

import sqlalchemy as sa
import sqlalchemy.orm as orm

from firecrest_wflow.data import mapper_registry

from .data import Calculation

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


class SqliteStorage(PersistProtocol):
    def __init__(
        self, path: Path | None = None, engine_kwargs: dict[str, Any] | None = None
    ) -> None:
        """Initialize the storage."""
        url = "sqlite:///:memory:" if path is None else f"sqlite:///{path}"
        self._engine = sa.create_engine(url, **(engine_kwargs or {}))
        mapper_registry.metadata.create_all(self._engine)
        self._session = orm.sessionmaker(bind=self._engine)()

    def save(self, calc: Calculation) -> None:
        """Save the calculation."""
        LOGGER.info("Saving calculation %s", calc)
        self._session.add(calc)
        self._session.commit()

    def save_many(self, calcs: list[Calculation]) -> None:
        """Save the calculation."""
        LOGGER.debug("Saving calculations %s", [calc.uuid for calc in calcs])
        self._session.add_all(calcs)
        self._session.commit()

    def get_unfinished(self, max: None | int = None) -> Sequence[Calculation]:
        """Get unfinished calculations."""
        stmt = sa.select(Calculation).where(
            Calculation.status != "finalised"  # type: ignore[arg-type]
        )
        if max is not None:
            stmt = stmt.limit(max)
        return self._session.scalars(stmt).all()
