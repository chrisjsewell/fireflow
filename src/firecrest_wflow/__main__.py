"""A mock up how a calculation would be run in AiiDA with FirecREST."""
from __future__ import annotations

import logging

from firecrest_wflow.data import Calculation, Code, Computer
from firecrest_wflow.process import run_unfinished_calculations
from firecrest_wflow.storage import SqliteStorage


def main() -> None:
    """Run the example."""

    logging.basicConfig(
        format="%(asctime)s:%(name)s:%(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

    computer = Computer(
        client_url="http://localhost:8000/",
        client_id="firecrest-sample",
        client_secret="b391e177-fa50-4987-beaf-e6d33ca93571",
        token_uri="http://localhost:8080/auth/realms/kcrealm/protocol/openid-connect/token",
        machine_name="cluster",
        work_dir="/home/service-account-firecrest-sample",
        small_file_size_mb=5,
    )

    code = Code(
        computer=computer,
        script="mkdir -p output\necho 'Hello world!' > output.txt",
    )

    calcs = [Calculation(code=code)] * 2
    storage = SqliteStorage(engine_kwargs={"echo": False})
    storage.save_many(calcs)
    run_unfinished_calculations(storage)

    calc: Calculation
    for calc in storage._session.query(Calculation):  # type: ignore
        print(calc)  # noqa: T201
        print("outputs:")  # noqa: T201
        for node in calc.outputs:
            print(node, node.attributes)  # noqa: T201


if __name__ == "__main__":
    main()
