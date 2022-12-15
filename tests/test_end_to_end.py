from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

import aiofiles
import aiohttp
import backoff
import pytest
from aiohttp import ClientResponseError
from gcloud.aio.storage import Storage, Bucket
from google.cloud import bigquery

from tests.expected_data import ANALYZED_202209010bd5e8


@backoff.on_exception(backoff.expo, ClientResponseError, max_time=3600)
async def download_blob(bucket: Bucket, name: str) -> Any:
    blob = await bucket.get_blob(name)
    return await blob.download()


async def delete_blobs(client: Storage, bucket: Bucket, prefix: str) -> None:
    to_delete = await bucket.list_blobs(prefix)
    coroutines = [client.delete(bucket.name, filename) for filename in to_delete]
    await asyncio.gather(*coroutines)


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


def export_bigquery_table(
    client: bigquery.Client,
    project: str,
    dataset: str,
    table: str,
    gcs_bucket: str,
) -> str:
    dataset_ref = bigquery.DatasetReference(project, dataset)
    table_ref = dataset_ref.table(table)
    destination_uri = f"gs://{gcs_bucket}/{table}.csv"
    job = client.extract_table(table_ref, destination_uri, location="US")
    job.result()
    return destination_uri


@pytest.mark.asyncio
async def test_e2e_without_human_validation(caplog):
    caplog.set_level(logging.DEBUG)
    slug = "20220901-0bd5e8"
    async with aiohttp.ClientSession() as session:
        # setup
        client = Storage(session=session)
        test_staging_bucket = Bucket(client, "in-gecko-ben-green-staging")
        working_staging_bucket = Bucket(client, "gecko-working-staging")
        ds_bucket = Bucket(client, "gecko-data-systems-dev")
        bq_client = bigquery.Client("gecko-dev-data-systems")

        logging.debug("Initial clean")
        async def cleanup():
            try:
                deletes = [
                    delete_blobs(client, test_staging_bucket, slug),
                    delete_blobs(client, working_staging_bucket, slug),
                    delete_blobs(client, ds_bucket, slug),
                    client.delete(test_staging_bucket.name, f"{slug}_inspection_runs.csv"),
                ]
                await asyncio.gather(*deletes)
                bq_client.delete_table(f"gecko-dev-data-systems.data_systems.{slug}_inspection_runs")
            except ClientResponseError as e:
                print(e.headers)
        await cleanup()

        logging.debug("Uploading sample data")

        # upload test slug
        await upload_directory(
            session, Path(__file__).parent.parent / slug, test_staging_bucket
        )

        logging.debug("Waiting for results")
        # Grab data and test!
        binned_data = await download_blob(
            ds_bucket, f"{slug}/analyzed/{slug}_analyzed.csv"
        )
        staging_files = await working_staging_bucket.list_blobs(
            f"{slug}/deliverable/ds"
        )
        assert binned_data == ANALYZED_202209010bd5e8
        export_bigquery_table(
            bq_client,
            "gecko-dev-data-systems",
            "data_systems",
            f"{slug}_inspection_runs",
            "in-gecko-ben-green-staging",
        )
        inspection_data = await download_blob(
            test_staging_bucket, f"{slug}_inspection_runs.csv"
        )

        logging.debug("Clean Up")
        await cleanup()

        logging.debug("Testing")
        with open('inspection_data.csv', 'rb') as f:
            expected_inspection_data = f.read()
        assert inspection_data == expected_inspection_data


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
