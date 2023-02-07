"""Run calcjobs via FirecREST."""
from __future__ import annotations

import asyncio
from io import BytesIO
from itertools import chain
import logging
import os
from pathlib import Path
import posixpath
from tempfile import TemporaryDirectory
import time
from typing import Any, BinaryIO, Sequence, TypedDict
from urllib.parse import urlparse

import aiofiles
import aiohttp
import firecrest
from jinja2.environment import Template

from fireflow.orm import CalcJob, DataNode, Processing
from fireflow.patches import ls_recurse
from fireflow.storage import Storage

LOGGER = logging.getLogger(__name__)
REPORT_LEVEL = logging.INFO + 5
logging.addLevelName(REPORT_LEVEL, "REPORT")


def report(pk: int, msg: str, *args: Any) -> None:
    """Report on the calcjob process."""
    LOGGER.log(REPORT_LEVEL, f"PK-{pk}: " + str(msg), *args)


JOB_NAME = "job.sh"


def run_unfinished_calcjobs(storage: Storage, limit: None | int = None) -> None:
    """Run all unfinished calcjobs."""
    running = list(
        storage.iter_rows(
            Processing,
            page_size=limit,
            filters=[Processing.state == "playing"],
        )
    )
    asyncio.run(run_multiple_calcjobs(running, storage))


async def run_multiple_calcjobs(calcs: Sequence[Processing], storage: Storage) -> None:
    """Run multiple calcjobs."""
    await asyncio.gather(*[run_calcjob(calc, storage) for calc in calcs])


async def reliquish() -> None:
    """Simple function that relinquishes control to the event loop"""
    await asyncio.sleep(0)


async def run_calcjob(process: Processing, storage: Storage) -> None:
    """Run a single calcjob."""
    process._freeze(False)  # TODO better way to do this?
    while process.step != "finalised":
        try:
            await run_step(process, storage)
        except Exception as exc:
            LOGGER.exception("Error running calcjob %s", process.calcjob.uuid)
            process.state = "excepted"
            exc_str = f"{type(exc).__name__}: {exc}"
            process.exception = exc_str
            storage._update_row(process)
            break
        storage._update_row(process)
        await reliquish()

    if process.step == "finalised":
        process.state = "finished"
        storage._update_row(process)


async def run_step(process: Processing, storage: Storage) -> None:
    """Run a single step of a calcjob."""

    # TODO would like to move this to pattern matching, but ruff does not support it:
    # https://github.com/charliermarsh/ruff/issues/282

    calc = process.calcjob

    if process.step == "created":
        process.step = "uploading"
    if process.step == "uploading":
        await copy_to_remote(calc, storage)
        process.step = "submitting"
    elif process.step == "submitting":
        await submit_on_remote(calc)
        process.step = "running"
    elif process.step == "running":
        await poll_until_finished(calc)
        process.step = "retrieving"
    elif process.step == "retrieving":
        with TemporaryDirectory() as out_tmpdir:
            await copy_from_remote(calc, Path(out_tmpdir))
            await parse_output_files(calc, Path(out_tmpdir))
        process.step = "finalised"
    else:
        raise ValueError(f"Unknown step name {process.step}")


async def poll_object_transfer(
    obj: firecrest.ExternalStorage, interval: int = 1, timeout: int | None = None
) -> None:
    """Poll until an object  has been transferred to/from the store."""
    start = time.time()
    while obj.in_progress:
        if timeout and time.time() - start > timeout:
            raise RuntimeError("timeout waiting for object transfer")
        await asyncio.sleep(interval)


async def copy_to_remote(calc: CalcJob, storage: Storage) -> None:
    """Copy the calculation inputs to the compute resource."""
    # TODO could use checksums, to confirm upload,
    # see also: https://github.com/eth-cscs/pyfirecrest/issues/14

    # TODO omnipotence, don't upload files that are already on the remote (and same checksum)

    client_row = calc.code.client
    remote_folder = calc.remote_path
    report(calc.pk, "Uploading files to remote")

    # create the base remote folder
    client = client_row.client
    client.mkdir(client_row.machine_name, str(remote_folder), p=True)

    # create and upload the script file
    job_script: bytes = (
        Template(calc.code.script)
        .render(calc=calc, code=calc.code, client=calc.code.client)
        .encode("utf-8")
    )
    client.simple_upload(
        client_row.machine_name, BytesIO(job_script), str(remote_folder), JOB_NAME
    )
    await reliquish()

    # upload files / make directories specified on the code and calcjob
    for rel_path, key in chain(
        calc.code.upload_paths.items(), calc.upload_paths.items()
    ):
        remote_path = remote_folder.joinpath(*posixpath.split(rel_path))
        if key is None:
            client.mkdir(client_row.machine_name, str(remote_path), p=True)
        else:
            if remote_path.parent != remote_folder:
                client.mkdir(client_row.machine_name, str(remote_path.parent), p=True)
            file_size = storage.objects.get_size(key)
            if file_size <= client_row.small_file_size_mb * 1024 * 1024:
                with storage.objects.open(key) as obj:
                    # TODO big uqwploads
                    client.simple_upload(
                        client_row.machine_name,
                        obj,
                        str(remote_path.parent),
                        remote_path.name,
                    )
                await reliquish()
            else:
                # Note, officially the API requires a sourcepath on disk,
                # but really it is not necessary
                # TODO await response from https://github.com/eth-cscs/firecrest/issues/174
                up_obj = client.external_upload(
                    client_row.machine_name, remote_path.name, str(remote_path.parent)
                )
                # Note, here we do not use pyfirecrest's finish_upload,
                # since it simply runs a subprocess to do the upload (calling curl)
                # instead we properly async the upload
                # up_obj.finish_upload()
                params = up_obj.object_storage_data["parameters"]
                if os.environ.get("FIRECREST_LOCAL_TESTING"):
                    # TODO this local fix for MACs was necessary for the demo
                    params["url"] = params["url"].replace("192.168.220.19", "localhost")
                with storage.objects.open(key) as handle:
                    await upload_io_to_url(handle, remote_path.name, params)
                await poll_object_transfer(up_obj)


async def submit_on_remote(calc: CalcJob) -> None:
    """Run the calcjob on the compute resource."""
    client_row = calc.code.client
    script_path = calc.remote_path / JOB_NAME
    report(calc.pk, "submitting on remote")
    client = client_row.client
    result = client.submit(client_row.machine_name, str(script_path), local_file=False)
    calc.status.job_id = result["jobid"]


async def poll_until_finished(
    calc: CalcJob, interval: int = 1, timeout: int | None = None
) -> None:
    """Poll the compute resource until the calcjob is finished."""
    report(calc.pk, "polling job until finished")
    client_row = calc.code.client
    client = client_row.client
    start = time.time()
    while timeout is None or (time.time() - start) < timeout:
        results = client.poll(client_row.machine_name, [calc.status.job_id])
        if results and results[0]["state"] == "COMPLETED":
            break
        await asyncio.sleep(interval)
    else:
        raise RuntimeError("timeout waiting for calcjob to finish")


async def copy_from_remote(calc: CalcJob, local_folder: Path) -> None:
    """Copy the calcjob outputs from the compute resource."""
    # TODO this should take the calc.download_globs, and only copy those
    # directly into the object storage
    # before downloading, we can also get the checksum to see if it is already in the store
    # once its in the store, then we want to update the calc process,
    # to record the (POSIX) path we retrieved

    client_row = calc.code.client
    remote_folder = calc.remote_path
    report(calc.pk, "copying from remote folder")
    client = client_row.client
    for item in ls_recurse(
        client, client_row.machine_name, str(remote_folder), show_hidden=True
    ):
        if item["type"] == "-":
            remote_path = remote_folder / item["path"]
            LOGGER.debug("copying from remote: %s", remote_path)
            local_path = local_folder.joinpath(
                *remote_path.relative_to(remote_folder).parts
            )
            local_path.parent.mkdir(parents=True, exist_ok=True)
            if client_row.small_file_size_mb * 1024 * 1024 > int(item["size"]):
                client.simple_download(
                    client_row.machine_name, str(remote_path), str(local_path)
                )
                await reliquish()
            else:
                down_obj = client.external_download(
                    client_row.machine_name, str(remote_path)
                )
                await poll_object_transfer(down_obj)

                # here instead of using down_obj.finish_download
                # we use an asynchoronous version of it
                url = down_obj.object_storage_data

                if os.environ.get("FIRECREST_LOCAL_TESTING"):
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


async def parse_output_files(calc: CalcJob, local_path: Path) -> None:
    """Parse the calculation outputs."""
    report(calc.pk, "parsing output files")
    paths = []
    for path in local_path.glob("**/*"):
        paths.append(
            path.relative_to(local_path).as_posix() + ("/" if path.is_dir() else "")
        )
    report(calc.pk, "paths: %s", paths)
    DataNode(attributes={"paths": paths}, creator_pk=calc.pk)


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


async def upload_io_to_url(
    handle: BinaryIO, filename: str, params: UploadParameters
) -> str:
    """Upload a file from a local file to a URL."""
    # TODO assert params["method"] == "POST" and not params["json"] ?
    async with aiohttp.ClientSession() as session:
        form = aiohttp.FormData()
        form.add_field("file", handle, filename=filename)
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
