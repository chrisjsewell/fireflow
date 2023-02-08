"""A CLI for firecrest-wflow."""
from enum import Enum
import logging
from pathlib import Path
import typing as t

from rich import box
from rich.console import Console
from rich.table import Table
from rich.tree import Tree
import typer
from typer.core import TyperGroup
import yaml

from fireflow import __version__, orm
from fireflow._sql_parse import filter_from_string
from fireflow.process import REPORT_LEVEL, run_unfinished_calcjobs
from fireflow.storage import Storage

console = Console()


class OrderedCommandsGroup(TyperGroup):
    """Custom `TyperGroup` to provide commands in the order they are added,
    rather than sorted by name.
    """

    def list_commands(self, ctx: t.Any) -> t.List[str]:
        return list(self.commands)


PANEL_NAME_PROJECT = "Project Commands"

app_main = typer.Typer(
    context_settings={"help_option_names": ["-h", "--help"]},
    rich_markup_mode="rich",
    no_args_is_help=True,
    cls=OrderedCommandsGroup,
)
app_client = typer.Typer(rich_markup_mode="rich")
app_main.add_typer(
    app_client,
    name="client",
    rich_help_panel="Command Groups",
    no_args_is_help=True,
    help="Configure and inspect connections to FirecREST clients.",
)
app_code = typer.Typer(rich_markup_mode="rich")
app_main.add_typer(
    app_code,
    name="code",
    rich_help_panel="Command Groups",
    no_args_is_help=True,
    help="Configure and inspect codes running on a client.",
)
app_calcjob = typer.Typer(rich_markup_mode="rich")
app_main.add_typer(
    app_calcjob,
    name="calcjob",
    rich_help_panel="Command Groups",
    no_args_is_help=True,
    help="Configure and inspect calculation jobs to run a code.",
)

# TODO how to order typers in help panel? (currently alphabetical)
# TODO handle exceptions better, only showing traceback if --debug is set
# TODO doctor command (purge object store of objects not referenced in database)
# TODO move run into a seperate panel
# TODO list calcjobs, and allow changing state


def version_callback(value: bool) -> None:
    """Print the version and exit."""
    if value:
        console.print(f"firecrest_wflow version: {__version__}")
        raise typer.Exit()


def create_table(
    table_title: str,
    data: t.Iterable[t.Dict[str, t.Any]],
    *,
    keys: t.Optional[t.List[str]] = None,
    aliases: t.Optional[t.Dict[str, str]] = None,
) -> Table:
    """Create a table to print

    :param table_title: The title of the table
    :param data: A list of objects to print
    :param keys: A list of keys to print, in order,
        otherwise take from the first object
    :param aliases: A dictionary mapping the keys to the column titles
    """
    aliases = aliases or {}
    table = Table(title=table_title, box=box.ROUNDED)
    for i, obj in enumerate(data):
        if i == 0:
            if keys is None:
                keys = list(obj)
            for key in keys:
                table.add_column(aliases.get(key, key), overflow="fold")
        table.add_row(*(str(obj[key]) for key in (keys or [])))

    return table


def config_callback(
    ctx: typer.Context, param: typer.CallbackParam, value: t.Optional[Path]
) -> t.Optional[Path]:
    if value is not None:
        try:
            with open(value, "r") as f:  # Load config file
                conf = yaml.safe_load(f)
            ctx.default_map = ctx.default_map or {}  # Initialize the default map
            ctx.default_map.update(conf)  # Merge the config dict into default_map
        except Exception as ex:
            raise typer.BadParameter(str(ex))
    return value


class StorageContext:
    """The storage context."""

    def __init__(self, storage_dir: t.Union[None, str, Path] = None) -> None:
        """Initialize the context."""
        if storage_dir is None:
            raise ValueError("storage_dir must be specified")
        self._storage_dir = Path(storage_dir)
        self._storage: t.Optional[Storage] = None

    def __str__(self) -> str:
        return f"StorageContext({str(self.path)!r})"

    @property
    def path(self) -> Path:
        """Get the storage path."""
        return self._storage_dir

    @property
    def storage(self) -> Storage:
        """Get the storage."""
        if self._storage is None:
            self._storage = Storage.from_path(self._storage_dir, init=False)
        return self._storage

    def init(self) -> None:
        """Initialize the storage."""
        self._storage = Storage.from_path(self._storage_dir, init=True)


@app_main.callback()
def main_app(
    ctx: typer.Context,
    storage: Path = typer.Option(
        ".fireflow_project",
        "-p",
        "--project-path",
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
        help="Path to the project directory.",
    ),
    version: t.Optional[bool] = typer.Option(
        None,
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Show the application version and exit.",
    ),
) -> None:
    """[underline]Firecrest workflow manager[/underline]"""
    ctx.obj = StorageContext(storage)


@app_main.command("init", rich_help_panel=PANEL_NAME_PROJECT)
def main_init(
    ctx: typer.Context,
    add: t.Optional[Path] = typer.Option(
        None,
        "-a",
        "--add",
        file_okay=True,
        dir_okay=False,
        resolve_path=True,
        help="Add objects from a YAML configuration file.",
    ),
) -> None:
    """Initialize a project."""
    storage = ctx.ensure_object(StorageContext)
    storage.init()
    console.print(
        f"[green]Storage initialized :white_check_mark:[/green]: {storage.path}"
    )
    if add is not None:
        console.print("Adding objects...")
        with open(add) as handle:
            data = yaml.safe_load(handle)
        added = storage.storage.save_from_dict(data)
        console.print("[green]Added objects :white_check_mark:[/green]", added)


@app_main.command("add", rich_help_panel=PANEL_NAME_PROJECT)
def main_add(
    ctx: typer.Context,
    config: Path = typer.Argument(
        ...,
        file_okay=True,
        dir_okay=False,
        resolve_path=True,
        help="Path to a YAML configuration file.",
    ),
) -> None:
    """Add multiple objects to a project, from a YAML file."""
    storage = ctx.ensure_object(StorageContext)
    with open(config) as handle:
        data = yaml.safe_load(handle)
    console.print("Adding objects...")
    added = storage.storage.save_from_dict(data)
    console.print("[green]Added objects :white_check_mark:[/green]", added)


def _add_plural(count: int, singular: str, plural_suffix: str = "s") -> str:
    if count == 1:
        return f"{count} {singular}"
    else:
        return f"{count} {singular}{plural_suffix}"


@app_main.command("status", rich_help_panel=PANEL_NAME_PROJECT)
def main_status(
    ctx: typer.Context,
) -> None:
    """Show some basic statistics about the project."""
    storage = ctx.ensure_object(StorageContext).storage
    console.print("Object Store:")
    console.print(f"- {_add_plural(storage.objects.count(), 'object')}")
    console.print("Database:")
    console.print(f"- {_add_plural(storage.count_rows(orm.Client), 'client')}")
    console.print(f"- {_add_plural(storage.count_rows(orm.Code), 'code')}")
    console.print(f"- {_add_plural(storage.count_rows(orm.CalcJob), 'calcjob')}")
    for status, color in [
        ("playing", "blue"),
        ("paused", "orange"),
        ("finished", "green"),
        ("excepted", "red"),
    ]:
        filter_ = [orm.Processing.state == status]
        count = storage.count_rows(orm.Processing, where=filter_)
        if count > 0:
            console.print(f"  - {count} [{color}]{status}[/{color}]")


class LogLevel(str, Enum):
    debug = "debug"
    info = "info"
    report = "report"
    warning = "warning"
    error = "error"
    critical = "critical"

    def to_int(self) -> int:
        """Convert the enum value to an integer."""
        return {
            LogLevel.debug: logging.DEBUG,
            LogLevel.info: logging.INFO,
            LogLevel.report: REPORT_LEVEL,
            LogLevel.warning: logging.WARNING,
            LogLevel.error: logging.ERROR,
            LogLevel.critical: logging.CRITICAL,
        }[self]


@app_main.command("run", rich_help_panel=PANEL_NAME_PROJECT)
def main_run(
    ctx: typer.Context,
    number: int = typer.Option(10, help="Maximum number of jobs to run"),
    log_level: LogLevel = typer.Option(
        LogLevel.report, case_sensitive=False, help="Logging level"
    ),
) -> None:
    """Run unfinished calcjobs."""
    storage = ctx.ensure_object(StorageContext).storage
    logging.basicConfig(
        format="%(asctime)s:%(name)s:%(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=log_level.to_int(),
    )
    run_unfinished_calcjobs(storage, number)


@app_client.command("create")
def client_create(
    ctx: typer.Context,
    config: t.Optional[Path] = typer.Option(
        None,
        "-c",
        "--config",
        callback=config_callback,
        is_eager=True,
        show_default=False,
        help="Path to a YAML file, to set defaults.",
    ),
    client_url: str = typer.Option(..., show_default=False, help="URL of the client"),
    client_id: str = typer.Option(..., show_default=False, help="Client ID"),
    client_secret: str = typer.Option(..., show_default=False, help="Client secret"),
    token_uri: str = typer.Option(..., show_default=False, help="Token URI"),
    machine_name: str = typer.Option(..., show_default=False, help="Machine name"),
    work_dir: str = typer.Option(
        ..., show_default=False, help="Work directory (absolute)"
    ),
    small_file_size_mb: int = typer.Option(
        ..., show_default=False, help="Small file size in MB"
    ),
    label: t.Optional[str] = typer.Option(
        None, show_default=False, help="Label for the client"
    ),
) -> None:
    """Create a new client."""
    storage = ctx.ensure_object(StorageContext).storage
    client = orm.Client(
        client_url=client_url,
        client_id=client_id,
        client_secret=client_secret,
        token_uri=token_uri,
        machine_name=machine_name,
        work_dir=work_dir,
        small_file_size_mb=small_file_size_mb,
    )
    if label is not None:
        client.label = label
    storage.save_row(client)
    console.print("[green]Created client:[/green]")
    console.print(client)


@app_client.command("show")
def client_show(
    ctx: typer.Context,
    pk: int = typer.Argument(..., help="Primary key of the client to show"),
) -> None:
    """Show a client."""
    storage = ctx.ensure_object(StorageContext).storage
    client = storage.get_row(orm.Client, pk)
    console.print(client)


@app_client.command("delete")
def client_delete(
    ctx: typer.Context,
    pk: int = typer.Argument(..., help="Primary key of the client to delete"),
) -> None:
    """Delete a client."""
    storage = ctx.ensure_object(StorageContext).storage
    client = storage.get_row(orm.Client, pk)
    typer.confirm(f"Are you sure you want to delete PK={pk}?", abort=True)
    storage.delete_row(client)
    console.print(f"[green]Deleted Client {pk}[/green]")


@app_client.command("list")
def client_list(
    ctx: typer.Context,
    page: int = typer.Option(1, help="The page of results to show"),
    page_size: int = typer.Option(100, help="The number of results per page"),
    where: t.Optional[str] = typer.Option(
        None, "--where", "-w", help="SQL WHERE clause, e.g. 'pk>1'"
    ),
    debug: bool = typer.Option(False, help="Show more information for debugging"),
) -> None:
    """List Clients."""
    storage = ctx.ensure_object(StorageContext).storage
    where_clause = None if where is None else filter_from_string(orm.Client, where)
    if debug and where_clause is not None:
        console.print(f"[blue]WHERE clause: {where_clause}[/blue]")
    count = storage.count_rows(orm.Client, where=where_clause)
    if not count:
        console.print("[green]No Clients to list[/green]")
        return
    table = create_table(
        "Clients {}-{} of {}".format(
            (page - 1) * page_size + 1, min(page * page_size, count), count
        ),
        (
            {
                "PK": client.pk,
                "Label": client.label,
                "Client URL": client.client_url,
                "Client ID": client.client_id,
                "Machine": client.machine_name,
            }
            for client in storage.iter_rows(
                orm.Client, page=page, page_size=page_size, where=where_clause
            )
        ),
    )
    console.print(table)


@app_code.command("show")
def code_show(
    ctx: typer.Context,
    pk: int = typer.Argument(..., help="Primary key of the code to show"),
    client: bool = typer.Option(False, help="Show the client as well"),
) -> None:
    """Show a code."""
    storage = ctx.ensure_object(StorageContext).storage
    code = storage.get_row(orm.Code, pk)
    console.print(code)
    if client:
        console.print(code.client)


@app_code.command("delete")
def code_delete(
    ctx: typer.Context,
    pk: int = typer.Argument(..., help="Primary key of the code to delete"),
) -> None:
    """Delete a client."""
    storage = ctx.ensure_object(StorageContext).storage
    code = storage.get_row(orm.Code, pk)
    typer.confirm(f"Are you sure you want to delete PK={pk}?", abort=True)
    storage.delete_row(code)
    console.print(f"[green]Deleted Code {pk}[/green]")


@app_code.command("tree")
def code_tree(
    ctx: typer.Context,
    page: int = typer.Option(1, help="The page of results to show"),
    page_size: int = typer.Option(100, help="The number of results per page"),
    where: t.Optional[str] = typer.Option(
        None, "--where", "-w", help="SQL WHERE clause, e.g. 'pk>1'"
    ),
    debug: bool = typer.Option(False, help="Show more information for debugging"),
) -> None:
    """Tree of Client :left_arrow_curving_right: Code."""
    storage = ctx.ensure_object(StorageContext).storage
    where_clause = None if where is None else filter_from_string(orm.Code, where)
    if debug and where_clause is not None:
        console.print(f"[blue]WHERE clause: {where_clause}[/blue]")
    count = storage.count_rows(orm.Code, where=where_clause)
    if not count:
        console.print("[green]No Codes to list[/green]")
        return
    tree = Tree(
        "[bold]Codes[/bold] {}-{} of {}".format(
            (page - 1) * page_size + 1, min(page * page_size, count), count
        ),
        highlight=False,
    )
    client_nodes: t.Dict[int, Tree] = {}
    for code in storage.iter_rows(
        orm.Code, page=page, page_size=page_size, where=where_clause
    ):
        if code.client.pk not in client_nodes:
            client_nodes[code.client.pk] = tree.add(
                f"[blue]{code.client.pk}[/blue] - {code.client.label}"
            )
        client_nodes[code.client.pk].add(f"[blue]{code.pk}[/blue] - {code.label}")
    console.print(tree)


@app_code.command("list")
def code_list(
    ctx: typer.Context,
    page: int = typer.Option(1, help="The page of results to show"),
    page_size: int = typer.Option(100, help="The number of results per page"),
    where: t.Optional[str] = typer.Option(
        None, "--where", "-w", help="SQL WHERE clause, e.g. 'pk>1'"
    ),
    debug: bool = typer.Option(False, help="Show more information for debugging"),
) -> None:
    """List Codes."""
    storage = ctx.ensure_object(StorageContext).storage
    where_clause = None if where is None else filter_from_string(orm.Code, where)
    if debug and where_clause is not None:
        console.print(f"[blue]WHERE clause: {where_clause}[/blue]")
    count = storage.count_rows(orm.Code, where=where_clause)
    if not count:
        console.print("[green]No Codes to list[/green]")
        return
    table = create_table(
        "Codes {}-{} of {}".format(
            (page - 1) * page_size + 1, min(page * page_size, count), count
        ),
        (
            {
                "PK": code.pk,
                "Label": code.label,
                "Client": f"{code.client_pk} ({code.client.label})",
            }
            for code in storage.iter_rows(
                orm.Code, page=page, page_size=page_size, where=where_clause
            )
        ),
    )
    console.print(table)


@app_calcjob.command("show")
def calcjob_show(
    ctx: typer.Context,
    pk: int = typer.Argument(..., help="Primary key of the calcjob to show"),
    process: bool = typer.Option(
        False, "--process", "-p", help="Show the process as well"
    ),
) -> None:
    """Show a calcjob."""
    storage = ctx.ensure_object(StorageContext).storage
    calcjob = storage.get_row(orm.CalcJob, pk)
    console.print(calcjob)
    if process:
        console.print(calcjob.process)


@app_calcjob.command("delete")
def calcjob_delete(
    ctx: typer.Context,
    pk: int = typer.Argument(..., help="Primary key of the calcjob to delete"),
) -> None:
    """Delete a calcjob."""
    storage = ctx.ensure_object(StorageContext).storage
    calcjob = storage.get_row(orm.CalcJob, pk)
    typer.confirm(f"Are you sure you want to delete PK={pk}?", abort=True)
    storage.delete_row(calcjob)
    console.print(f"[green]Deleted CalcJob {pk}[/green]")


@app_calcjob.command("list")
def calcjob_list(
    ctx: typer.Context,
    page: int = typer.Option(1, help="The page of results to show"),
    page_size: int = typer.Option(100, help="The number of results per page"),
    where: t.Optional[str] = typer.Option(
        None, "--where", "-w", help="SQL WHERE clause, e.g. 'pk>1'"
    ),
    debug: bool = typer.Option(False, help="Show more information for debugging"),
) -> None:
    """List Codes."""
    storage = ctx.ensure_object(StorageContext).storage
    where_clause = None if where is None else filter_from_string(orm.CalcJob, where)
    if debug and where_clause is not None:
        console.print(f"[blue]WHERE clause: {where_clause}[/blue]")
    count = storage.count_rows(orm.CalcJob, where=where_clause)
    if not count:
        console.print("[green]No CalcJob to list[/green]")
        return
    table = create_table(
        "CalcJob {}-{} of {}".format(
            (page - 1) * page_size + 1, min(page * page_size, count), count
        ),
        (
            {
                "PK": calc.pk,
                # "UUID": str(calc.uuid),
                "Label": calc.label,
                "Code": f"{calc.code_pk} ({calc.code.label})",
                "Client": f"{calc.code.client_pk} ({calc.code.client.label})",
                "State": calc.state,
                "Step": calc.process.step,
            }
            for calc in storage.iter_rows(
                orm.CalcJob, page=page, page_size=page_size, where=where_clause
            )
        ),
    )
    console.print(table)


_STATE_EMOJI = {
    "playing": ":arrow_forward:",
    "paused": ":pause_button:",
    "finished": ":white_check_mark:",
    "excepted": ":cross_mark:",
}


@app_calcjob.command("tree")
def calcjob_tree(
    ctx: typer.Context,
    page: int = typer.Option(1, help="The page of results to show"),
    page_size: int = typer.Option(100, help="The number of results per page"),
    where: t.Optional[str] = typer.Option(
        None, "--where", "-w", help="SQL WHERE clause, e.g. 'pk>1'"
    ),
    debug: bool = typer.Option(False, help="Show more information for debugging"),
) -> None:
    """Tree of Client :left_arrow_curving_right: Code :left_arrow_curving_right: CalcJob."""
    storage = ctx.ensure_object(StorageContext).storage
    where_clause = None if where is None else filter_from_string(orm.CalcJob, where)
    if debug and where_clause is not None:
        console.print(f"[blue]WHERE clause: {where_clause}[/blue]")
    count = storage.count_rows(orm.CalcJob, where=where_clause)
    if not count:
        console.print("[green]No CalcJob to list[/green]")
        return
    tree = Tree(
        "[bold]Calcjobs[/bold] {}-{} of {}".format(
            (page - 1) * page_size + 1, min(page * page_size, count), count
        ),
        highlight=False,
    )
    client_nodes: t.Dict[int, Tree] = {}
    code_nodes: t.Dict[int, Tree] = {}
    for calcjob in storage.iter_rows(
        orm.CalcJob, page=page, page_size=page_size, where=where_clause
    ):
        if calcjob.code.client.pk not in client_nodes:
            client_nodes[calcjob.code.client.pk] = tree.add(
                f"[blue]{calcjob.code.client.pk}[/blue] - {calcjob.code.client.label}"
            )
        if calcjob.code.pk not in code_nodes:
            code_nodes[calcjob.code.pk] = client_nodes[calcjob.code.client.pk].add(
                f"[blue]{calcjob.code.pk}[/blue] - {calcjob.code.label}"
            )
        code_nodes[calcjob.code.pk].add(
            f"[blue]{calcjob.pk}[/blue] - {calcjob.label} {_STATE_EMOJI[calcjob.state]}"
        )

    console.print(tree)


if __name__ == "__main__":
    app_main()
