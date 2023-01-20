"""A click based CLI for firecrest-wflow."""
from __future__ import annotations

from dataclasses import fields
import json
from pathlib import Path
from typing import Any

import click
import click_config_file
import yaml

from firecrest_wflow.data import Computer
from firecrest_wflow.storage import SqliteStorage

try:
    import tomllib

    NO_TOMLLIB = False
except ImportError:
    NO_TOMLLIB = True


class StorageContext:
    """The storage context."""

    def __init__(self, path: str | None = None):
        """Initialize the context."""
        self._path = path
        self._storage: None | SqliteStorage = None

    @property
    def storage(self) -> SqliteStorage:
        """Get the storage."""
        if self._storage is None:
            self._storage = SqliteStorage(self._path)
        return self._storage


pass_storage = click.make_pass_decorator(StorageContext, ensure=True)


@click.group()
@click.option("--path", type=click.Path(dir_okay=False), default="wkflow.sqlite")
@click.pass_context
def main(ctx: click.Context, path: str | None) -> None:
    """The firecrest-wflow CLI."""
    ctx.obj = StorageContext(path)


@main.group()
def computer() -> None:
    """Computer related commands."""


def config_provider(path_str: str, _: Any) -> Any:
    """Provide the config."""
    path = Path(path_str)
    if not path.exists():
        raise click.BadParameter(f"Config file {path} does not exist")
    if path.suffix == ".json":
        return json.loads(path.read_text("utf-8"))
    if path.suffix == ".toml":
        if NO_TOMLLIB:
            raise click.BadParameter(
                f"Config file {path} has .toml suffix but tomli is not installed"
            )
        return tomllib.loads(path.read_text("utf-8"))
    if path.suffix in (".yaml", ".yml"):
        return yaml.safe_load(path.read_text("utf-8"))
    raise click.BadParameter(f"Config file {path} has unknown suffix")


@computer.command()
@click.option("--client-url", required=True)
@click.option("--client-id", required=True)
@click.option("--client-secret", required=True)
@click.option("--token-uri", required=True)
@click.option("--machine-name", required=True)
@click.option("--work-dir", required=True)
@click.option("--small-file-size-mb", required=True, type=int)
@click_config_file.configuration_option(implicit=False, provider=config_provider)  # type: ignore
@pass_storage
def create(storage: StorageContext, **kwargs: Any) -> None:
    """Create a computer."""
    computer = Computer(**kwargs)
    storage.storage.save(computer)


@computer.command()
@pass_storage
def list(storage: StorageContext) -> None:
    """List computers."""
    data = []
    for comp in storage.storage.all(Computer):
        data.append(
            {
                "pk": comp.pk,
                "client_url": comp.client_url,
                "client_id": comp.client_id,
                "machine_name": comp.machine_name,
            }
        )
    click.echo(yaml.safe_dump(data, default_flow_style=False, sort_keys=False))


@computer.command()
@click.argument("pk", type=int)
@pass_storage
def show(storage: StorageContext, pk: int) -> None:
    """Show a computer."""
    computer = storage.storage._session.get(Computer, pk)
    data = {}
    for field in fields(computer):
        if field.name.startswith("_"):
            continue
        data[field.name] = getattr(computer, field.name)
    click.echo(yaml.safe_dump(data, default_flow_style=False, sort_keys=False))


if __name__ == "__main__":
    main(help_option_names=["-h", "--help"])
