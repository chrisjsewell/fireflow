"""A mock up how a calculation would be run in AiiDA with FirecREST."""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from tempfile import TemporaryDirectory
import time
from typing import Any, Sequence, TypedDict
from urllib.parse import urlparse

import aiofiles
import aiohttp
import firecrest
import jinja2

from firecrest_wflow.data import Calculation, DataNode
from firecrest_wflow.patches import ls_recurse
from firecrest_wflow.storage import PersistProtocol, SqliteStorage

LOGGER = logging.getLogger(__name__)

JOB_NAME = "job.sh"


def run_unfinished_calculations(storage: SqliteStorage) -> None:
    """Run all unfinished calculations."""
    calcs = storage.get_unfinished()
    asyncio.run(run_multiple_calculations(calcs, storage))


async def run_multiple_calculations(
    calcs: Sequence[Calculation], persist: PersistProtocol
) -> None:
    """Run multiple calculations."""
    await asyncio.gather(*[run_calculation(calc, persist) for calc in calcs])


async def reliquish() -> None:
    """Simple function that relinquishes control to the event loop"""
    await asyncio.sleep(0)


async def run_calculation(calc: Calculation, persist: PersistProtocol) -> None:
    """Run a single calculation."""
    while calc.status != "finalised":
        try:
            await run_step(calc)
        except Exception as exc:
            LOGGER.exception("Error running calculation %s", calc.uuid)
            exc_str = f"{type(exc).__name__}: {exc}"
            calc.exception = exc_str
            calc.status = "finalised"
        persist.save(calc)
        await reliquish()


async def run_step(calc: Calculation) -> None:
    """Run a single step of a computation."""

    # TODO would like to move this to pattern matching, but ruff does not support it:
    # https://github.com/charliermarsh/ruff/issues/282

    if calc.status == "created":
        with TemporaryDirectory() as in_tmpdir:
            await prepare_for_submission(calc, Path(in_tmpdir))
            await copy_to_remote(calc, Path(in_tmpdir))
        calc.status = "uploaded"
    elif calc.status == "uploaded":
        await submit_on_remote(calc)
        calc.status = "submitted"
    elif calc.status == "submitted":
        await poll_until_finished(calc)
        calc.status = "executed"
    elif calc.status == "executed":
        with TemporaryDirectory() as out_tmpdir:
            await copy_from_remote(calc, Path(out_tmpdir))
            await parse_output_files(calc, Path(out_tmpdir))
        calc.status = "finalised"
    else:
        raise ValueError(f"Unknown status {calc.status}")


async def prepare_for_submission(calc: Calculation, local_path: Path) -> None:
    """Prepares the (local) calculation folder with all inputs,
    ready to be copied to the compute resource.
    """
    LOGGER.info("prepare for submission: %s", calc.uuid)

    script_template = "\n".join(
        [
            "#!/bin/bash",
            "#SBATCH --job-name={{calc.uuid}}",
        ]
        + calc.code.script.splitlines()
    )
    job_script = jinja2.Template(script_template).render(calc=calc)
    local_path.joinpath(JOB_NAME).write_text(job_script, encoding="utf-8")


async def poll_object_transfer(
    obj: firecrest.ExternalStorage, interval: int = 1, timeout: int | None = 60
) -> None:
    """Poll until an object  has been transferred to/from the store."""
    start = time.time()
    while obj.in_progress:
        if timeout and time.time() - start > timeout:
            raise RuntimeError("timeout waiting for object transfer")
        await asyncio.sleep(interval)


async def copy_to_remote(calc: Calculation, local_folder: Path) -> None:
    """Copy the calculation inputs to the compute resource."""
    computer = calc.code.computer
    remote_folder = calc.remote_path
    LOGGER.info("copying to remote folder: %s", remote_folder)
    client = computer.client
    client.mkdir(computer.machine_name, str(remote_folder), p=True)
    for local_path in local_folder.glob("**/*"):
        target_path = remote_folder.joinpath(
            *local_path.relative_to(local_folder).parts
        )
        LOGGER.debug("copying to remote: %s", target_path)
        if local_path.is_dir():
            client.mkdir(computer.machine_name, str(target_path), p=True)
        if local_path.is_file():
            if computer.small_file_size_mb * 1024 * 1024 > local_path.stat().st_size:
                client.simple_upload(
                    computer.machine_name, str(local_path), str(target_path.parent)
                )
                await reliquish()
            else:
                up_obj = client.external_upload(
                    computer.machine_name, str(local_path), str(target_path.parent)
                )
                # TODO here we do not use pyfirecrest's finish_upload,
                # since it simply runs a subprocess to do the upload (calling curl)
                # instead we properly async the upload
                # up_obj.finish_upload()
                params = up_obj.object_storage_data["parameters"]
                # TODO this local fix for MACs was necessary for the demo
                params["url"] = params["url"].replace("192.168.220.19", "localhost")
                await upload_file_to_url(local_path, params)
                await poll_object_transfer(up_obj)
                # TODO invalidate the upload object


async def submit_on_remote(calc: Calculation) -> None:
    """Run the calculation on the compute resource."""
    computer = calc.code.computer
    script_path = calc.remote_path / JOB_NAME
    LOGGER.info("submitting on remote: %s", script_path)
    client = computer.client
    result = client.submit(computer.machine_name, str(script_path), local_file=False)
    calc.attributes["job_id"] = result["jobid"]


async def poll_until_finished(
    calc: Calculation, interval: int = 1, timeout: int | None = 60
) -> None:
    """Poll the compute resource until the calculation is finished."""
    LOGGER.info("polling job until finished: %s", calc.uuid)
    computer = calc.code.computer
    client = computer.client
    start = time.time()
    while timeout is None or (time.time() - start) < timeout:
        results = client.poll(computer.machine_name, [calc.attributes["job_id"]])
        if results and results[0]["state"] == "COMPLETED":
            break
        await asyncio.sleep(interval)
    else:
        raise RuntimeError("timeout waiting for calculation to finish")


async def copy_from_remote(calc: Calculation, local_folder: Path) -> None:
    """Copy the calculation outputs from the compute resource."""
    computer = calc.code.computer
    remote_folder = calc.remote_path
    LOGGER.info("copying from remote folder: %s", remote_folder)
    client = computer.client
    for item in ls_recurse(
        client, computer.machine_name, str(remote_folder), show_hidden=True
    ):
        if item["type"] == "-":
            remote_path = remote_folder / item["path"]
            LOGGER.debug("copying from remote: %s", remote_path)
            local_path = local_folder.joinpath(
                *remote_path.relative_to(remote_folder).parts
            )
            local_path.parent.mkdir(parents=True, exist_ok=True)
            if computer.small_file_size_mb * 1024 * 1024 > int(item["size"]):
                client.simple_download(
                    computer.machine_name, str(remote_path), str(local_path)
                )
                await reliquish()
            else:
                down_obj = client.external_download(
                    computer.machine_name, str(remote_path)
                )
                await poll_object_transfer(down_obj)

                # TODO here instead of using down_obj.finish_download
                # we use an asynchoronous version of it
                url = down_obj.object_storage_data
                # await download_url_to_file(url, local_path)

                # TODO however the url above doesn't work locally, with the demo docker
                # there was a fix already noted for MAC: url.replace("192.168.220.19", "localhost")
                # however, this still gives a 403 error:
                # "The request signature we calculated does not match the signature you provided.
                # Check your key and signing method.""
                # so for now, I'm just going to swap out the URL, with the actual location on disk
                # where the files are stored for the demo!
                store_path = (
                    "/Users/chrisjsewell/Documents/GitHub/firecrest/deploy/demo/minio"
                    + urlparse(url).path
                )
                await copy_file_async(store_path, local_path)
                # TODO invalidate the download object


async def parse_output_files(calc: Calculation, local_path: Path) -> None:
    """Parse the calculation outputs."""
    LOGGER.info("parsing output files: %s", local_path)
    paths = []
    for path in local_path.glob("**/*"):
        paths.append(
            path.relative_to(local_path).as_posix() + ("/" if path.is_dir() else "")
        )
    DataNode(creator=calc, attributes={"paths": paths})


# HELPER functions


async def copy_file_async(src: str | Path, dest: str | Path) -> None:
    """Copy a file asynchronously."""
    async with aiofiles.open(src, mode="rb") as fr, aiofiles.open(
        dest, mode="wb"
    ) as fw:
        while True:
            chunk = await fr.read(1024)
            if not chunk:
                break
            await fw.write(chunk)


class UploadParameters(TypedDict):
    """Parameters for the calculation."""

    url: str
    method: str
    data: dict[str, str]
    headers: dict[str, str]
    json: dict[str, Any]
    params: dict[str, str]


async def upload_file_to_url(filepath: Path | str, params: UploadParameters) -> str:
    """Upload a file from a local file to a URL."""
    # assert params["method"] == "POST" and not params["json"]
    async with aiohttp.ClientSession() as session:
        with open(filepath, "rb") as f:
            form = aiohttp.FormData()
            form.add_field("file", f, filename=str(filepath))
            for key, value in params["data"].items():
                form.add_field(key, value)
            async with session.post(
                params["url"],
                data=form,
                headers=params["headers"],
                params=params["params"],
            ) as resp:
                return await resp.text()


async def download_url_to_file(url: str, filepath: Path | str) -> None:
    """Download a file from a URL to a local file."""
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:
            with open(filepath, "wb") as f:
                while True:
                    chunk = await resp.content.read(1024)
                    if not chunk:
                        break
                    f.write(chunk)
