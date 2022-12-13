from __future__ import annotations

import asyncio
from pathlib import Path
from typing import NamedTuple, Any

import pytest
import aiofiles
import aiohttp
from aiohttp import ClientResponseError
from gcloud.aio.storage import Storage, Bucket, Blob
import backoff


class GCSPath(NamedTuple):
    bucket: Bucket
    path: str


@backoff.on_exception(backoff.expo, ClientResponseError, max_time=3600)
async def download_blob(bucket: Bucket, name: str) -> Any:
    blob = await bucket.get_blob(name)
    return await blob.download()


async def delete_blobs(client: Storage, bucket: Bucket, prefix: str) -> None:
    to_delete = await bucket.list_blobs(prefix)
    coroutines = [client.delete(bucket.name, filename) for filename in to_delete]
    await asyncio.gather(*coroutines)


@pytest.mark.asyncio
async def test_e2e_without_human_validation():
    slug = "20220901-0bd5e8"
    async with aiohttp.ClientSession() as session:
        # setup
        client = Storage(session=session)
        test_staging_bucket = Bucket(client, "in-gecko-ben-green-staging")
        working_staging_bucket = Bucket(client, "gecko-working-staging")
        ds_bucket = Bucket(client, "gecko-data-systems-dev")

        # clear old files (unnecessary if cleanup has already happened, but better safe than sorry)
        # await delete_blobs(client, test_staging_bucket, slug)

    #     # upload test slug
    #     # await upload_directory(session, Path("20220901-0bd5e8"), test_staging_bucket)
    #     # grab and test outputs
    #     staging_files, binned_data = await asyncio.gather(
    #         working_staging_bucket.list_blobs(f"{slug}/deliverable/ds"),
    #         download_blob(
    #             working_staging_bucket, f"{slug}/deliverable/ds/binned_plot_data.json"
    #         ),
    #     )
        staging_files = await working_staging_bucket.list_blobs(
            f"{slug}/deliverable/ds"
        )
        print(staging_files)
    #     # cleanup
    # # with open("binned_plot_data_ground_truth.json", "rb") as f:
    # #     f.write(binned_data)
    # print(staging_files)


async def upload_file(
    session: aiohttp.ClientSession,
    local_path: Path,
    destination_path: str,
    bucket: Bucket,
):
    blob = bucket.new_blob(destination_path)
    async with aiofiles.open(local_path, "rb") as f:
        contents = await f.read()
    await blob.upload(contents, session)


async def upload_directory(session: aiohttp.ClientSession, path: Path, bucket: Bucket):
    queue = list(path.iterdir())
    coroutines = []
    while queue:
        elem = queue.pop()
        if elem.is_dir():
            queue.extend(elem.iterdir())
        else:
            coroutine = upload_file(
                session, elem, str(elem.relative_to(path.parent)), bucket
            )
            coroutines.append(coroutine)
    await asyncio.gather(*coroutines)


"""
outputs:
In GCS:
    PROJECT Gecko Ops - Staging
        - VALUES IN: gecko-working-staging/[slug]/deliverable/ds/binned_plot_data.json (generated by https://validator-api-dot-gecko-ops-staging.uc.r.appspot.com/inspection/[slug]/deliverable)
        - FILES PRESENT IN: gecko-working-staging/20220901-0bd5e8/deliverable/ds
    PROJECT Data Systems Dev
        - VALUES IN: gecko-data-systems-dev/[slug]/analyzed/[slug]_analyzed.csv
In BigQuery:
    PROJECT gecko-dev-data-systems, DATASET data_systems
        - VALUES IN: [slug]_inspection_runs
"""