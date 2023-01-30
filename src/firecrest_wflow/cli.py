"""A click based CLI for firecrest-wflow."""
from __future__ import annotations

from dataclasses import fields
import json
import logging
from pathlib import Path
from typing import Any

import click
import click_config_file
import yaml

from firecrest_wflow._orm import Calculation, Computer
from firecrest_wflow.process import run_unfinished_calculations
from firecrest_wflow.storage import Storage


class StorageContext:
    """The storage context."""

    def __init__(self, storage_dir: str | Path) -> None:
        """Initialize the context."""
        self._storage_dir = Path(storage_dir)
        self._storage: None | Storage = None

    @property
    def storage(self) -> Storage:
        """Get the storage."""
        if self._storage is None:
            self._storage = Storage.on_file(self._storage_dir, init=True)
        return self._storage


pass_storage = click.make_pass_decorator(StorageContext, ensure=True)


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.option(
    "-s",
    "--storage-dir",
    type=click.Path(file_okay=False, dir_okay=True),
    default="wkflow_storage",
)
@click.pass_context
def main(ctx: click.Context, storage_dir: str) -> None:
    """The firecrest-wflow CLI."""
    ctx.obj = StorageContext(storage_dir)


@main.command()
@click.argument("path", type=click.Path(exists=True, dir_okay=False))
@pass_storage
def create(storage: StorageContext, path: str) -> None:
    """Create the storage."""
    storage.storage.from_yaml(path)


@main.command()
@click.argument("number", type=int, default=10)
@click.option(
    "--log-level", type=click.Choice(("DEBUG", "INFO", "WARNING")), default="INFO"
)
@pass_storage
def run(storage: StorageContext, number: int, log_level: str) -> None:
    """Run a maximum number of unfinished calculations."""
    logging.basicConfig(
        format="%(asctime)s:%(name)s:%(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=getattr(logging, log_level),
    )
    run_unfinished_calculations(storage.storage, number)


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
    if path.suffix in (".yaml", ".yml"):
        return yaml.safe_load(path.read_text("utf-8"))
    raise click.BadParameter(f"Config file {path} has unknown suffix")


@computer.command("create")
@click.option("--client-url", required=True)
@click.option("--client-id", required=True)
@click.option("--client-secret", required=True)
@click.option("--token-uri", required=True)
@click.option("--machine-name", required=True)
@click.option("--work-dir", required=True)
@click.option("--small-file-size-mb", required=True, type=int)
@click.option("--label")
@click_config_file.configuration_option(implicit=False, provider=config_provider)  # type: ignore
@pass_storage
def create_computer(storage: StorageContext, **kwargs: Any) -> None:
    """Create a computer."""
    computer = Computer(**kwargs)
    storage.storage.save_computer(computer)


@computer.command()
@pass_storage
def list(storage: StorageContext) -> None:
    """List computers."""
    data = []
    for comp in storage.storage.all(Computer):
        data.append(
            {
                "pk": comp.pk,
                "label": comp.label,
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


@main.group()
def calculation() -> None:
    """Calculation related commands."""


@calculation.command("list")
@pass_storage
def list_calc(storage: StorageContext) -> None:
    """List calculations."""
    data = []
    for calc in storage.storage.all(Calculation):
        data.append(
            {
                "pk": calc.pk,
                "label": calc.label,
                "code": calc.code.label,
                "computer": calc.code.computer.label,
                "status": calc.status.step,
            }
        )
    click.echo(yaml.safe_dump(data, default_flow_style=False, sort_keys=False))
