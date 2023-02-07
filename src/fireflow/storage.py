"""Storage for the calculations."""
from __future__ import annotations

import logging
from pathlib import Path
from sqlite3 import Connection as SQLite3Connection
import typing as t  # import t.Any, t.Iterable, Sequence, TypedDict, TypeVar

import sqlalchemy as sa
from sqlalchemy import event
from sqlalchemy import orm as sa_orm
from sqlalchemy.exc import IntegrityError as SaIntegrityError
from sqlalchemy.exc import NoResultFound

from . import object_store as ostore
from . import orm

LOGGER = logging.getLogger(__name__)


ORM_TYPE = t.TypeVar("ORM_TYPE", bound=orm.Base)
ANY_TYPE = t.TypeVar("ANY_TYPE")


@event.listens_for(sa.Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record) -> None:  # type: ignore
    """Enable foreign key restrictions for SQLite."""
    if isinstance(dbapi_connection, SQLite3Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()


class UnDeletableError(Exception):
    """Raised when trying to delete an object, but an sa.IntegrityError is raised."""


class Storage:
    """Persistent storage for the calculations."""

    @classmethod
    def from_memory(cls) -> Storage:
        """Create an in-memory storage."""
        engine = sa.create_engine("sqlite:///:memory:")
        orm.Base.metadata.create_all(engine)
        return cls(engine, ostore.InMemoryObjectStore())

    @classmethod
    def from_path(cls, path: Path | str, *, init: bool = False) -> Storage:
        """Connect to an on-file storage."""
        path = Path(path)
        object_path = path / "objects"
        db_path = path / "storage.sqlite"
        if not init:
            if not path.is_dir():
                raise FileNotFoundError(
                    f"Storage path not found (use `fireflow init`): {path}"
                )
            if not object_path.is_dir():
                raise FileNotFoundError(f"Object store path not found: {object_path}")
            if not db_path.is_file():
                raise FileNotFoundError(f"Database path not found: {db_path}")
        engine = sa.create_engine(f"sqlite:///{db_path}")
        if init:
            object_path.mkdir(parents=True, exist_ok=True)
            orm.Base.metadata.create_all(engine)
        return cls(
            engine,
            ostore.FileObjectStore(object_path),
        )

    def __init__(
        self,
        engine: sa.Engine,
        object_store: ostore.ObjectStore,
    ) -> None:
        """Initialize the storage."""

        self._session = sa_orm.sessionmaker(engine)()
        self._object_store = object_store

    @property
    def objects(self) -> ostore.ObjectStore:
        """Get the object store."""
        return self._object_store

    def _save_to_db(self, obj: orm.Base) -> None:
        """Save an ORM object to the database."""
        LOGGER.debug("Saving row %s", obj)
        # TODO check this is the best way to do this
        # with, for example `from_dict` I want to commit at the end
        # but also want to be able to get e.g. the pk of the object
        # hence, `add`` then `flush`
        # otherwise, `add` then `commit`, but we also need to ensure we rollback
        # note sqlachemy always executes queries in a transaction, and doesn't close it
        # (its automatically closed on commit)
        if self._session.in_nested_transaction():
            self._session.add(obj)
            # without the flush, the pk is not set
            self._session.flush()
        elif self._session.in_transaction():
            # see
            # https://docs.sqlalchemy.org/en/20/orm/session_basics.html#framing-out-a-begin-commit-rollback-block
            try:
                self._session.add(obj)
                self._session.commit()
            except Exception:
                self._session.rollback()
                raise
        else:
            with self._session.begin():
                self._session.add(obj)

    def _create_immutable_obj(self, obj: ORM_TYPE) -> ORM_TYPE:
        """Create an immutable object."""
        obj._freeze()
        return obj

    def save_row(self, row: ORM_TYPE) -> ORM_TYPE:
        """Add a row to a database table."""
        if row.is_frozen:
            raise ValueError(f"Cannot save frozen objects: {row}")
        if row.pk is not None and self.has_row(row.__class__, row.pk):
            raise ValueError(f"Cannot save object with existing pk: {row}")

        self._save_to_db(row)
        return self._create_immutable_obj(row)

    def _update_row(self, row: orm.Base) -> None:
        """Update a column of a row.

        This is a private method,
        since it should not generally be called by user.
        """
        # TODO better way to do this?
        self._save_to_db(row)

    def delete_row(self, obj: orm.Base) -> None:
        """Delete a row of a database table."""
        # TODO allow deleting by pk
        if obj.pk is None:
            raise ValueError(f"{obj} not saved")
        # TODO should the delete not be in the try/except?
        self._session.delete(obj)
        try:
            self._session.commit()
        except SaIntegrityError as exc:
            self._session.rollback()
            raise UnDeletableError(
                f"{obj} is likely a dependency for other objects"
            ) from exc
        except Exception:
            self._session.rollback()
            raise

    def count_rows(
        self,
        obj_cls: type[ORM_TYPE],
        *,
        filters: t.Sequence[sa.ColumnElement[bool]] = (),
    ) -> int:
        """Count rows in a database table.

        :param obj_cls: The class of the table to select
        :param filters: Additional filters to apply (joined with AND)
        """
        selector = sa.select(obj_cls)
        selector = selector.order_by(obj_cls.pk)
        if filters:
            selector = selector.where(sa.and_(*filters))
        return self._session.execute(  # type: ignore
            sa.select(sa.func.count()).select_from(selector.subquery())
        ).scalar_one()

    def has_row(self, obj_cls: type[ORM_TYPE], pk: int) -> bool:
        """Check if a row exists in a database table.

        :param obj_cls: The class of the table to select
        :param pk: The primary key of the row to select
        """
        selector = sa.select(obj_cls.pk)
        selector = selector.where(obj_cls.pk == pk)
        return self._session.execute(selector).scalar_one_or_none() is not None

    def get_row(self, obj_cls: type[ORM_TYPE], pk: int) -> ORM_TYPE:
        """Get a row of a database table, represented by an ORM object.

        :param obj_cls: The class of the table to select
        :param pk: The primary key of the row to select
        """
        obj = self._session.get(obj_cls, pk)
        if obj is None:
            raise KeyError(f"{obj_cls.__name__}({pk}) not found")
        return self._create_immutable_obj(obj)

    def get_column(
        self, column: sa_orm.InstrumentedAttribute[ANY_TYPE], pk: int
    ) -> ANY_TYPE:
        """Get a column for a row of a database table, converted to a Python type."""
        selector = sa.select(column)
        selector = selector.where(column.class_.pk == pk)
        try:
            return self._session.execute(selector).scalar_one()
        except NoResultFound:
            raise KeyError(f"{column.class_.__name__}({pk}) not found")

    def iter_rows(
        self,
        obj_cls: type[ORM_TYPE],
        *,
        page_size: int | None = None,
        page: int = 1,
        filters: t.Sequence[sa.ColumnElement[bool]] = (),
    ) -> t.Iterable[ORM_TYPE]:
        """Iterate over rows of a database table, represented by ORM objects.

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
            yield self._create_immutable_obj(obj)

    def save_from_dict(self, data: FromDictConfig) -> dict[str, t.Any]:
        """Load data to the store from a dict representation.

        :return: A dict of data added to the storage
        """
        # TODO load as a jinja template, so we can use variables and do loops etc

        # basic validation
        # TODO could use pydantic os something for this
        if not isinstance(data, dict):
            raise ValueError("Expected a dict at the top level")
        if "objects" in data and not isinstance(data["objects"], dict):
            raise ValueError("Expected a dict for key 'objects'")
        for key in ("clients", "codes", "calcjobs"):
            if key in data:
                if not isinstance(data[key], list):  # type: ignore[literal-required]
                    raise ValueError(f"Expected a list for key '{key}'")
                for idx, item in enumerate(data[key]):  # type: ignore[literal-required]
                    if not isinstance(item, dict):
                        raise ValueError(f"Expected a dict for item '{key}[{idx}]'")

        # add objects and create mapping of label to key
        obj_label_to_key: dict[str, str] = {}
        for obj_label, obj_content in data.get("objects", {}).items():
            if not isinstance(obj_content, dict):
                raise ValueError(f"Expected a dict for object '{obj_label}'")
            if "content" in obj_content:
                if not isinstance(obj_content["content"], str):
                    raise ValueError(f"Expected a string for object '{obj_label}'")
                encoding = obj_content.get("encoding", "utf8")
                extension = obj_content.get("extension", "txt")
                obj_label_to_key[obj_label] = self.objects.add_from_bytes(
                    obj_content["content"].encode(encoding), ext=extension
                )
            elif "path" in obj_content:
                if not isinstance(obj_content["path"], str):
                    raise ValueError(f"Expected a string for object '{obj_label}'")
                obj_label_to_key[obj_label] = self.objects.add_from_path(
                    obj_content["path"]
                )
            else:
                raise ValueError(
                    f"Expected either 'content' or 'path' for object '{obj_label}'"
                )

        # add in single transaction? (so we can rollback if there's an error)
        with self._session.begin_nested():

            added_pks: dict[str, list[int]] = {}

            for client_data in data.get("clients", []):
                try:
                    client = orm.Client(**client_data)
                    self.save_row(client)
                except Exception as exc:
                    raise ValueError(f"clients[{idx}] item is invalid: {exc}") from exc
                added_pks.setdefault("clients", []).append(client.pk)

            for idx, code_data in enumerate(data.get("codes", [])):
                if "client_label" not in code_data:
                    raise ValueError(f"codes[{idx}] item has no 'client_label' key")
                client_label = code_data.pop("client_label")
                client_pk = self._session.scalar(
                    sa.select(orm.Client.pk).where(orm.Client.label == client_label)
                )
                if client_pk is None:
                    raise ValueError(
                        f"codes[{idx}]['client_label'] = {client_label!r} not found"
                    )
                _convert_upload_paths(
                    self.objects, code_data, obj_label_to_key, f"codes[{idx}]"
                )
                try:
                    code = orm.Code(**code_data, client_pk=client_pk)
                    self.save_row(code)
                except Exception as exc:
                    raise ValueError(f"codes[{idx}] item is invalid: {exc}") from exc
                added_pks.setdefault("codes", []).append(code.pk)

            for idx, calcjob_data in enumerate(data.get("calcjobs", [])):
                if "code_label" not in calcjob_data:
                    raise ValueError(f"calcjobs[{idx}] item has no 'code_label' key")
                code_label = calcjob_data.pop("code_label")
                code_pk = self._session.scalar(
                    sa.select(orm.Code.pk).where(orm.Code.label == code_label)
                )
                if code_pk is None:
                    raise ValueError(
                        f"calcjobs[{idx}]['code_label'] = {code_label!r} not found"
                    )
                _convert_upload_paths(
                    self.objects, calcjob_data, obj_label_to_key, f"calcjobs[{idx}]"
                )
                try:
                    calcjob = orm.CalcJob(**calcjob_data, code_pk=code_pk)
                    self.save_row(calcjob)
                except Exception as exc:
                    raise ValueError(f"calcjobs[{idx}] item is invalid: {exc}") from exc
                added_pks.setdefault("calcjobs", []).append(calcjob.pk)

        return added_pks


def _convert_upload_paths(
    store: ostore.ObjectStore,
    item: dict[str, t.Any],
    label_to_key: dict[str, str],
    prefix: str,
) -> None:
    if "upload_paths" not in item:
        return
    upload_paths = item["upload_paths"]
    name = f"{prefix}[upload_paths]"
    if not isinstance(upload_paths, dict):
        raise ValueError(f"Expected a dict for {name}")
    new_upload_paths: dict[str, str] = {}
    for key, value in upload_paths.items():
        if value is None:
            continue
        if not isinstance(value, dict):
            raise ValueError(f"Expected a string for {name}[{key}]")
        if "label" in value:
            if not isinstance(value["label"], str):
                raise ValueError(f"Expected a string for '{name}[{key}]['label']")
            if value["label"] not in label_to_key:
                raise ValueError(
                    f"{name}[{key}]['label'] = {value['label']!r} not found"
                )
            new_upload_paths[key] = label_to_key[value["label"]]
        elif "key" in value:
            if not isinstance(value["key"], str):
                raise ValueError(f"Expected a string for '{name}[{key}]['key']")
            new_upload_paths[key] = value["key"]
        else:
            raise ValueError(f"Expected either 'label' or 'key' for {name}[{key}]")

    for key, value in new_upload_paths.items():
        if value not in store:
            raise KeyError(f"Key {value!r} not found in storage for {name}[{key}]")

    item["upload_paths"] = new_upload_paths


class ObjectDictConfig(t.TypedDict, total=False):
    path: str
    content: str
    encoding: str
    extension: str


class FromDictConfig(t.TypedDict, total=False):
    objects: dict[str, ObjectDictConfig]
    clients: list[dict[str, t.Any]]
    codes: list[dict[str, t.Any]]
    calcjobs: list[dict[str, t.Any]]
