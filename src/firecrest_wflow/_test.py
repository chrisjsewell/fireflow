"""A mock up how a calculation would be run in AiiDA with FirecREST."""
from __future__ import annotations

import logging
from textwrap import dedent

from firecrest_wflow._orm import CalcJob, Client, Code
from firecrest_wflow.process import run_unfinished_calcjobs
from firecrest_wflow.storage import Storage


def main() -> None:
    """Run the example."""

    logging.basicConfig(
        format="%(asctime)s:%(name)s:%(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.INFO,
    )
    # logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

    # storage = Storage.in_memory()
    storage = Storage.on_file("wkflow_storage2", init=True)

    client = Client(
        client_url="http://localhost:8000/",
        client_id="firecrest-sample",
        client_secret="b391e177-fa50-4987-beaf-e6d33ca93571",
        token_uri="http://localhost:8080/auth/realms/kcrealm/protocol/openid-connect/token",
        machine_name="cluster",
        work_dir="/home/service-account-firecrest-sample",
        small_file_size_mb=5,
    )
    storage.save_client(client)

    code = Code(
        client=client,
        script=dedent(
            """\
            #!/bin/bash
            #SBATCH --job-name={{calc.uuid}}

            mkdir -p output
            sleep 30
            echo 'Hello world!' > output.txt
            """,
        ),
    )
    storage.save_code(code)

    key = storage.objects.add_from_bytes(b"Hello world!", "txt")

    for _ in range(2):
        storage.save_calcjob(CalcJob(code=code, upload={"input.txt": key}))
    run_unfinished_calcjobs(storage)

    print("calcjobs:")  # noqa: T201
    for calc in storage.iter_obj(CalcJob):
        print(" ", calc, calc.code)  # noqa: T201
        print("  ", "outputs:")  # noqa: T201
        for node in calc.outputs:
            print("  ", node, node.attributes)  # noqa: T201


if __name__ == "__main__":
    main()
