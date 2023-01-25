"""A mock up how a calculation would be run in AiiDA with FirecREST."""
from __future__ import annotations

import logging
from textwrap import dedent

from firecrest_wflow._orm import Calculation, Code, Computer
from firecrest_wflow.process import run_unfinished_calculations
from firecrest_wflow.storage import Storage


def main() -> None:
    """Run the example."""

    logging.basicConfig(
        format="%(asctime)s:%(name)s:%(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )

    storage = Storage.in_memory()

    computer = Computer(
        client_url="http://localhost:8000/",
        client_id="firecrest-sample",
        client_secret="b391e177-fa50-4987-beaf-e6d33ca93571",
        token_uri="http://localhost:8080/auth/realms/kcrealm/protocol/openid-connect/token",
        machine_name="cluster",
        work_dir="/home/service-account-firecrest-sample",
        small_file_size_mb=5,
    )
    storage.save_computer(computer)

    code = Code(
        computer=computer,
        script=dedent(
            """\
            #!/bin/bash
            #SBATCH --job-name={{calc.uuid}}

            mkdir -p output
            echo 'Hello world!' > output.txt
            """,
        ),
    )
    storage.save_code(code)

    key = storage.objects.add_from_bytes(b"Hello world!", "txt")

    for _ in range(2):
        storage.save_calculation(Calculation(code=code, upload={"input.txt": key}))
    run_unfinished_calculations(storage)

    print("calculations:")  # noqa: T201
    for calc in storage.all(Calculation):
        print(" ", calc, calc.code)  # noqa: T201
        print("  ", "outputs:")  # noqa: T201
        for node in calc.outputs:
            print("  ", node, node.attributes)  # noqa: T201


if __name__ == "__main__":
    main()
