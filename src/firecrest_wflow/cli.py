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
import yaml

from firecrest_wflow import __version__, orm
from firecrest_wflow.process import run_unfinished_calcjobs
from firecrest_wflow.storage import Storage

console = Console()
app_main = typer.Typer(
    context_settings={"help_option_names": ["-h", "--help"]}, rich_markup_mode="rich"
)
app_client = typer.Typer(rich_markup_mode="rich")
app_main.add_typer(
    app_client,
    name="client",
    rich_help_panel="Command Groups",
    help="Configure and inspect connections to FirecREST clients.",
)
app_code = typer.Typer(rich_markup_mode="rich")
app_main.add_typer(
    app_code,
    name="code",
    rich_help_panel="Command Groups",
    help="Configure and inspect codes running on a client.",
)
app_calcjob = typer.Typer(rich_markup_mode="rich")
app_main.add_typer(
    app_calcjob,
    name="calcjob",
    rich_help_panel="Command Groups",
    help="Configure and inspect calculation jobs to run a code.",
)

# TODO how to order typers in help panel?
# TODO handle exceptions better, only showing traceback if --debug is set


def version_callback(value: bool) -> None:
    """Print the version and exit."""
    if value:
        console.print(f"firecrest_wflow version: {__version__}")
        raise typer.Exit()


def create_table(
    table_title: str, data: t.Iterable[t.Dict[str, t.Any]], *mappings: t.Tuple[str, str]
) -> Table:
    """Create a table to print"""
    table = Table(title=table_title, box=box.ROUNDED)
    for (title, _) in mappings:
        table.add_column(title, overflow="fold")

    for i in data:
        table.add_row(*(str(i[key]) for (_, key) in mappings))

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
            self._storage = Storage.on_file(self._storage_dir, init=False)
        return self._storage

    def init(self) -> None:
        """Initialize the storage."""
        self._storage = Storage.on_file(self._storage_dir, init=True)


@app_main.callback()
def main_app(
    ctx: typer.Context,
    storage: Path = typer.Option(
        "wkflow_storage",
        "-s",
        "--storage-dir",
        file_okay=False,
        dir_okay=True,
        resolve_path=True,
        help="Path to the storage directory.",
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


@app_main.command("init")
def main_init(
    ctx: typer.Context,
    config: t.Optional[Path] = typer.Argument(
        None,
        file_okay=True,
        dir_okay=False,
        resolve_path=True,
        help="Path to a YAML configuration file.",
    ),
) -> None:
    """Initialize the storage."""
    storage = ctx.ensure_object(StorageContext)
    storage.init()
    if config is not None:
        storage.storage.from_yaml(config)
    console.print(
        f"[green]Storage initialized :white_check_mark:[/green]: {storage.path}"
    )


class LogLevel(str, Enum):
    debug = "DEBUG"
    info = "INFO"
    warning = "WARNING"
    error = "ERROR"
    critical = "CRITICAL"


@app_main.command("run")
def main_run(
    ctx: typer.Context,
    number: int = typer.Option(10, help="Maximum number of jobs to run"),
    log_level: LogLevel = typer.Option(
        LogLevel.info, case_sensitive=False, help="Logging level"
    ),
) -> None:
    """Run unfinished calcjobs."""
    storage = ctx.ensure_object(StorageContext).storage
    logging.basicConfig(
        format="%(asctime)s:%(name)s:%(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=getattr(
            logging, log_level.value
        ),  # TODO logging.getLevelNamesMapping (>=3.11)
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
    storage.save_client(client)
    console.print("[green]Created client:[/green]")
    console.print(client)


@app_client.command("show")
def client_show(
    ctx: typer.Context,
    pk: int = typer.Argument(..., help="Primary key of the client to show"),
) -> None:
    """Show a client."""
    storage = ctx.ensure_object(StorageContext).storage
    client = storage.get_obj(orm.Client, pk)
    console.print(client)


@app_client.command("delete")
def client_delete(
    ctx: typer.Context,
    pk: int = typer.Argument(..., help="Primary key of the client to delete"),
) -> None:
    """Delete a client."""
    storage = ctx.ensure_object(StorageContext).storage
    client = storage.get_obj(orm.Client, pk)
    typer.confirm(f"Are you sure you want to delete PK={pk}?", abort=True)
    storage.delete_obj(client)
    console.print(f"[green]Deleted Client {pk}[/green]")


@app_client.command("list")
def client_list(
    ctx: typer.Context,
    page: int = typer.Option(1, help="The page of results to show"),
    page_size: int = typer.Option(100, help="The number of results per page"),
) -> None:
    """List Clients."""
    storage = ctx.ensure_object(StorageContext).storage
    count = storage.count_obj(orm.Client)
    table = create_table(
        "Clients {}-{} of {}".format(
            (page - 1) * page_size + 1, min(page * page_size, count), count
        ),
        (
            {
                "pk": client.pk,
                "label": client.label,
                "client_url": client.client_url,
                "client_id": client.client_id,
                "machine_name": client.machine_name,
            }
            for client in storage.iter_obj(orm.Client, page=page, page_size=page_size)
        ),
        ("PK", "pk"),
        ("Label", "label"),
        ("Client URL", "client_url"),
        ("Client ID", "client_id"),
        ("Machine", "machine_name"),
    )
    console.print(table)


@app_code.command("show")
def code_show(
    ctx: typer.Context,
    pk: int = typer.Argument(..., help="Primary key of the code to show"),
) -> None:
    """Show a code."""
    storage = ctx.ensure_object(StorageContext).storage
    code = storage.get_obj(orm.Code, pk)
    console.print(code)


@app_code.command("delete")
def code_delete(
    ctx: typer.Context,
    pk: int = typer.Argument(..., help="Primary key of the code to delete"),
) -> None:
    """Delete a client."""
    storage = ctx.ensure_object(StorageContext).storage
    code = storage.get_obj(orm.Code, pk)
    typer.confirm(f"Are you sure you want to delete PK={pk}?", abort=True)
    storage.delete_obj(code)
    console.print(f"[green]Deleted Code {pk}[/green]")


@app_code.command("tree")
def code_tree(
    ctx: typer.Context,
    page: int = typer.Option(1, help="The page of results to show"),
    page_size: int = typer.Option(100, help="The number of results per page"),
) -> None:
    """Tree of Client :left_arrow_curving_right: Code."""
    storage = ctx.ensure_object(StorageContext).storage
    count = storage.count_obj(orm.Code)
    tree = Tree(
        "[bold]Codes[/bold] {}-{} of {}".format(
            (page - 1) * page_size + 1, min(page * page_size, count), count
        ),
        highlight=False,
    )
    client_nodes: t.Dict[int, Tree] = {}
    for code in storage.iter_obj(orm.Code, page=page, page_size=page_size):
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
) -> None:
    """List Codes."""
    storage = ctx.ensure_object(StorageContext).storage
    count = storage.count_obj(orm.Code)
    table = create_table(
        "Codes {}-{} of {}".format(
            (page - 1) * page_size + 1, min(page * page_size, count), count
        ),
        (
            {
                "pk": code.pk,
                "label": code.label,
                "client_pk": code.client_pk,
                "client_label": code.client.label,
            }
            for code in storage.iter_obj(orm.Code, page=page, page_size=page_size)
        ),
        ("PK", "pk"),
        ("Label", "label"),
        ("Client PK", "client_pk"),
        ("Client Label", "client_label"),
    )
    console.print(table)


@app_calcjob.command("show")
def calcjob_show(
    ctx: typer.Context,
    pk: int = typer.Argument(..., help="Primary key of the calcjob to show"),
    show_process: bool = typer.Option(
        False, "-p", "--process", help="Show also the process"
    ),
) -> None:
    """Show a calcjob."""
    storage = ctx.ensure_object(StorageContext).storage
    calcjob = storage.get_obj(orm.CalcJob, pk)
    console.print(calcjob)
    if show_process:
        console.print(calcjob.status)


@app_calcjob.command("delete")
def calcjob_delete(
    ctx: typer.Context,
    pk: int = typer.Argument(..., help="Primary key of the calcjob to delete"),
) -> None:
    """Delete a calcjob."""
    storage = ctx.ensure_object(StorageContext).storage
    calcjob = storage.get_obj(orm.CalcJob, pk)
    typer.confirm(f"Are you sure you want to delete PK={pk}?", abort=True)
    storage.delete_obj(calcjob)
    console.print(f"[green]Deleted CalcJob {pk}[/green]")


@app_calcjob.command("tree")
def calcjob_tree(
    ctx: typer.Context,
    page: int = typer.Option(1, help="The page of results to show"),
    page_size: int = typer.Option(100, help="The number of results per page"),
) -> None:
    """Tree of Client :left_arrow_curving_right: Code :left_arrow_curving_right: CalcJob."""
    storage = ctx.ensure_object(StorageContext).storage
    count = storage.count_obj(orm.CalcJob)
    tree = Tree(
        "[bold]Calcjobs[/bold] {}-{} of {}".format(
            (page - 1) * page_size + 1, min(page * page_size, count), count
        ),
        highlight=False,
    )
    client_nodes: t.Dict[int, Tree] = {}
    code_nodes: t.Dict[int, Tree] = {}
    for calcjob in storage.iter_obj(orm.CalcJob, page=page, page_size=page_size):
        if calcjob.code.client.pk not in client_nodes:
            client_nodes[calcjob.code.client.pk] = tree.add(
                f"[blue]{calcjob.code.client.pk}[/blue] - {calcjob.code.client.label}"
            )
        if calcjob.code.pk not in code_nodes:
            code_nodes[calcjob.code.pk] = client_nodes[calcjob.code.client.pk].add(
                f"[blue]{calcjob.code.pk}[/blue] - {calcjob.code.label}"
            )
        code_nodes[calcjob.code.pk].add(f"[blue]{calcjob.pk}[/blue] - {calcjob.label}")
        # TODO include status

    console.print(tree)


if __name__ == "__main__":
    app_main()
