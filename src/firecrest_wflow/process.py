"""Run calculations via FirecREST."""
from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
import posixpath
import shutil

# import posixpath
from tempfile import TemporaryDirectory
import time
from typing import Any, Sequence, TypedDict
from urllib.parse import urlparse

import aiofiles
import aiohttp
import firecrest
import jinja2

from firecrest_wflow._orm import Calculation, DataNode, Processing
from firecrest_wflow.patches import ls_recurse
from firecrest_wflow.storage import Storage

LOGGER = logging.getLogger(__name__)

JOB_NAME = "job.sh"


def run_unfinished_calculations(storage: Storage, limit: None | int = None) -> None:
    """Run all unfinished calculations."""
    running = storage.get_unfinished(limit)
    asyncio.run(run_multiple_calculations(running, storage))


async def run_multiple_calculations(
    calcs: Sequence[Processing], storage: Storage
) -> None:
    """Run multiple calculations."""
    await asyncio.gather(*[run_calculation(calc, storage) for calc in calcs])


async def reliquish() -> None:
    """Simple function that relinquishes control to the event loop"""
    await asyncio.sleep(0)


async def run_calculation(process: Processing, storage: Storage) -> None:
    """Run a single calculation."""
    while process.step != "finalised":
        try:
            await run_step(process, storage)
        except Exception as exc:
            LOGGER.exception("Error running calculation %s", process.calculation.uuid)
            exc_str = f"{type(exc).__name__}: {exc}"
            process.exception = exc_str
            break
        storage.update_processing(process)
        await reliquish()


async def run_step(status: Processing, storage: Storage) -> None:
    """Run a single step of a computation."""

    # TODO would like to move this to pattern matching, but ruff does not support it:
    # https://github.com/charliermarsh/ruff/issues/282

    calc = status.calculation

    if status.step == "created":
        status.step = "uploading"
    if status.step == "uploading":
        with TemporaryDirectory() as in_tmpdir:
            await prepare_for_submission(calc, Path(in_tmpdir), storage)
            await copy_to_remote(calc, Path(in_tmpdir))
        status.step = "submitting"
    elif status.step == "submitting":
        await submit_on_remote(calc)
        status.step = "running"
    elif status.step == "running":
        await poll_until_finished(calc)
        status.step = "retrieving"
    elif status.step == "retrieving":
        with TemporaryDirectory() as out_tmpdir:
            await copy_from_remote(calc, Path(out_tmpdir))
            await parse_output_files(calc, Path(out_tmpdir))
        status.step = "finalised"
    else:
        raise ValueError(f"Unknown step name {status.step}")


async def prepare_for_submission(
    calc: Calculation, local_path: Path, storage: Storage
) -> None:
    """Prepares the (local) calculation folder with all inputs,
    ready to be copied to the compute resource.
    """
    LOGGER.info("prepare for submission: %s", calc.uuid)
    job_script = jinja2.Template(calc.code.script).render(calc=calc)
    local_path.joinpath(JOB_NAME).write_text(job_script, encoding="utf-8")
    for rel_path, key in calc.upload.items():
        path = local_path.joinpath(*posixpath.split(rel_path))
        if key is None:
            path.mkdir(parents=True, exist_ok=True)
        else:
            with open(path, "wb") as handle, storage._object_store.open(key) as obj:
                shutil.copyfileobj(obj, handle)


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
                # Note, here we do not use pyfirecrest's finish_upload,
                # since it simply runs a subprocess to do the upload (calling curl)
                # instead we properly async the upload
                # up_obj.finish_upload()
                params = up_obj.object_storage_data["parameters"]
                if os.environ.get("FIRECREST_DEMO"):
                    # TODO this local fix for MACs was necessary for the demo
                    params["url"] = params["url"].replace("192.168.220.19", "localhost")
                await upload_file_to_url(local_path, params)
                await poll_object_transfer(up_obj)


async def submit_on_remote(calc: Calculation) -> None:
    """Run the calculation on the compute resource."""
    computer = calc.code.computer
    script_path = calc.remote_path / JOB_NAME
    LOGGER.info("submitting on remote: %s", script_path)
    client = computer.client
    result = client.submit(computer.machine_name, str(script_path), local_file=False)
    calc.status.job_id = result["jobid"]


async def poll_until_finished(
    calc: Calculation, interval: int = 1, timeout: int | None = 60
) -> None:
    """Poll the compute resource until the calculation is finished."""
    LOGGER.info("polling job until finished: %s", calc.uuid)
    computer = calc.code.computer
    client = computer.client
    start = time.time()
    while timeout is None or (time.time() - start) < timeout:
        results = client.poll(computer.machine_name, [calc.status.job_id])
        if results and results[0]["state"] == "COMPLETED":
            break
        await asyncio.sleep(interval)
    else:
        raise RuntimeError("timeout waiting for calculation to finish")


async def copy_from_remote(calc: Calculation, local_folder: Path) -> None:
    """Copy the calculation outputs from the compute resource."""
    # TODO this should take the calc.download_globs, and only copy those
    # directly into the object storage
    # before downloading, we can also get the checksum to see if it is already in the store
    # once its in the store, then we want to update the calc process,
    # to record the (POSIX) path we retrieved

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

                # here instead of using down_obj.finish_download
                # we use an asynchoronous version of it
                url = down_obj.object_storage_data

                if os.environ.get("FIRECREST_DEMO"):
                    # TODO however the url above doesn't work locally, with the demo docker
                    # there was a fix already noted for MAC:url.replace("192.168.220.19", "localhost")
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
                else:
                    await download_url_to_file(url, local_path)

                # now invalidate the download object, since we no longer need it
                down_obj.invalidate_object_storage_link()


async def parse_output_files(calc: Calculation, local_path: Path) -> None:
    """Parse the calculation outputs."""
    LOGGER.info("parsing output files: %s", local_path)
    paths = []
    for path in local_path.glob("**/*"):
        paths.append(
            path.relative_to(local_path).as_posix() + ("/" if path.is_dir() else "")
        )
    calc.outputs.append(DataNode(attributes={"paths": paths}))


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
