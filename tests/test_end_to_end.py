from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any

import aiofiles
import aiohttp
import backoff
import pandas as pd
import pytest
from aiohttp import ClientResponseError
from gcloud.aio.storage import Storage, Bucket
from google.cloud import bigquery


@backoff.on_exception(
    backoff.constant,
    (ClientResponseError, asyncio.exceptions.TimeoutError),
    interval=30,
    max_time=1800,
)
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


@pytest.fixture
def expected_analyzed() -> pd.DataFrame:
    return (
        pd.read_csv("20220901-0bd5e8_analyzed_expected.csv")
        .reset_index(drop=True)
        .sort_values(["x_bin", "y_bin"])
        .reset_index(drop=True)
    )


@pytest.fixture
def expected_inspection_runs() -> pd.DataFrame:
    return (
        pd.read_csv("20220901-0bd5e8_inspection_runs_expected.csv")
        .reset_index(drop=True)
        .sort_values(["run_id", "row_num"])
        .reset_index(drop=True)
    )


@pytest.fixture
async def e2e_data(caplog) -> tuple[pd.DataFrame, pd.DataFrame]:
    caplog.set_level(logging.INFO)
    slug = "20220901-0bd5e8"
    async with aiohttp.ClientSession() as session:
        # setup
        client = Storage(session=session)
        test_staging_bucket = Bucket(client, "in-gecko-ben-green-staging")
        working_staging_bucket = Bucket(client, "gecko-working-staging")
        ds_bucket = Bucket(client, "gecko-data-systems-dev")
        bq_client = bigquery.Client("gecko-dev-data-systems")

        logging.info("Initial clean")

        async def cleanup():
            try:
                deletes = [
                    delete_blobs(client, test_staging_bucket, slug),
                    delete_blobs(client, working_staging_bucket, slug),
                    delete_blobs(client, ds_bucket, slug),
                    client.delete(
                        test_staging_bucket.name, f"{slug}_inspection_runs.csv"
                    ),
                ]
                await asyncio.gather(*deletes)
                bq_client.delete_table(
                    f"gecko-dev-data-systems.data_systems.{slug}_inspection_runs"
                )
            except ClientResponseError as e:
                print(e.headers)

        await cleanup()

        logging.info("Uploading sample data")
        await upload_directory(
            session, Path(__file__).parent.parent / slug, test_staging_bucket
        )

        logging.info(
            f"Waiting for results. Will begin attempting download at {datetime.now() + timedelta(minutes=10)}"
        )
        await asyncio.sleep(600)

        binned_analyzed_data_raw = await download_blob(
            ds_bucket, f"{slug}/analyzed/{slug}_analyzed.csv"
        )
        binned_analyzed = (
            pd.read_csv(StringIO(binned_analyzed_data_raw.decode()))
            .reset_index(drop=True)
            .sort_values(["x_bin", "y_bin"])
            .reset_index(drop=True)
        )

        # export bigquery table to gcs and then download from there
        export_bigquery_table(
            bq_client,
            "gecko-dev-data-systems",
            "data_systems",
            f"{slug}_inspection_runs",
            test_staging_bucket.name,
        )
        inspection_runs = await download_blob(
            test_staging_bucket, f"{slug}_inspection_runs.csv"
        )
        inspection_runs = (
            pd.read_csv(StringIO(inspection_runs.decode()))
            .reset_index(drop=True)
            .sort_values(["run_id", "row_num"])
            .reset_index(drop=True)
        )

        logging.info("Cleanup")
        await cleanup()

    return binned_analyzed, inspection_runs


@pytest.mark.asyncio
async def test_e2e_without_human_validation(
    caplog,
    e2e_data: tuple[pd.DataFrame, pd.Dataframe],
    expected_analyzed: pd.DataFrame,
    expected_inspection_runs,
):
    binned_analyzed, inspection_data = e2e_data
    pd.testing.assert_frame_equal(
        binned_analyzed,
        expected_analyzed,
        check_names=False,
    )
    pd.testing.assert_frame_equal(
        inspection_data.drop(columns=["created_at"]),
        expected_inspection_runs.drop(columns=["created_at"]),
        check_names=False,
    )


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
