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
from google.cloud.exceptions import NotFound

SLUG = "20220901-0bd5e8"


@pytest.fixture(scope="session")
def event_loop():
    """Overrides pytest default function scoped event loop"""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()


@backoff.on_exception(
    backoff.constant,
    (ClientResponseError, asyncio.exceptions.TimeoutError),
    interval=30,
    max_time=1800,
    jitter=None,
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


@pytest.fixture(scope="session")
def expected_analyzed() -> pd.DataFrame:
    path = Path(__file__).parent / "20220901-0bd5e8_analyzed_expected.csv"
    return (
        pd.read_csv(path)
        .reset_index(drop=True)
        .sort_values(["x_bin", "y_bin"])
        .reset_index(drop=True)
    )


@pytest.fixture(scope="session")
def expected_inspection_runs() -> pd.DataFrame:
    path = Path(__file__).parent / "20220901-0bd5e8_inspection_runs_expected.csv"
    return (
        pd.read_csv(path)
        .reset_index(drop=True)
        .sort_values(["run_id", "row_num"])
        .reset_index(drop=True)
    )


@pytest.fixture(scope="session")
async def session() -> aiohttp.ClientSession:
    session = aiohttp.ClientSession()
    yield session
    await session.close()


@pytest.fixture(scope="session")
async def gcs_client(session) -> Storage:
    yield Storage(session=session)


@pytest.fixture(scope="session")
async def staging_test_bucket(gcs_client: Storage) -> Bucket:
    bucket = Bucket(gcs_client, "in-gecko-ben-green-staging")

    async def clean_inspection_runs():
        try:
            await gcs_client.delete(bucket.name, f"{SLUG}_inspection_runs.csv")
        except ClientResponseError as e:
            if e.status != 404:
                raise e

    await asyncio.gather(
        delete_blobs(gcs_client, bucket, SLUG),
        clean_inspection_runs(),
    )
    yield bucket
    await asyncio.gather(
        delete_blobs(gcs_client, bucket, SLUG),
        clean_inspection_runs(),
    )


@pytest.fixture(scope="session")
async def staging_working_bucket(gcs_client: Storage) -> Bucket:
    bucket = Bucket(gcs_client, "gecko-working-staging")
    await delete_blobs(gcs_client, bucket, SLUG)
    yield bucket
    await delete_blobs(gcs_client, bucket, SLUG)


@pytest.fixture(scope="session")
async def ds_bucket(gcs_client: Storage) -> Bucket:
    bucket = Bucket(gcs_client, "gecko-data-systems-dev")
    await delete_blobs(gcs_client, bucket, SLUG)
    yield bucket
    await delete_blobs(gcs_client, bucket, SLUG)


def bq_table_exists(bq_client: bigquery.Client, table: str) -> bool:
    try:
        bq_client.get_table(table)
        return True
    except NotFound:
        return False


@pytest.fixture(scope="session")
async def bq_client(session) -> bigquery.Client:
    bq_client = bigquery.Client("gecko-dev-data-systems")
    table_id = f"gecko-dev-data-systems.data_systems.{SLUG}_inspection_runs"
    if bq_table_exists(bq_client, table_id):
        bq_client.delete_table(table_id)
    yield bq_client
    if bq_table_exists(bq_client, table_id):
        bq_client.delete_table(table_id)


@pytest.fixture(scope="session")
async def e2e_data(
    session: aiohttp.ClientSession,
    staging_test_bucket: Bucket,
    ds_bucket: Bucket,
    bq_client: bigquery.Client,
) -> tuple[pd.DataFrame, pd.DataFrame]:

    logging.info("Uploading sample data")
    await upload_directory(
        session, Path(__file__).parent.parent / SLUG, staging_test_bucket
    )

    logging.info(
        f"Waiting for results. Will begin attempting download at {datetime.now() + timedelta(minutes=15)}"
    )
    await asyncio.sleep(900)

    slug_analyzed_raw = await download_blob(
        ds_bucket, f"{SLUG}/analyzed/{SLUG}_analyzed.csv"
    )
    slug_analyzed = (
        pd.read_csv(StringIO(slug_analyzed_raw.decode()))
        .reset_index(drop=True)
        .sort_values(["x_bin", "y_bin"])
        .reset_index(drop=True)
    )

    # export bigquery table to gcs and then download from there
    export_bigquery_table(
        bq_client,
        "gecko-dev-data-systems",
        "data_systems",
        f"{SLUG}_inspection_runs",
        staging_test_bucket.name,
    )
    inspection_runs = await download_blob(
        staging_test_bucket, f"{SLUG}_inspection_runs.csv"
    )
    inspection_runs = (
        pd.read_csv(StringIO(inspection_runs.decode()))
        .reset_index(drop=True)
        .sort_values(["run_id", "row_num"])
        .reset_index(drop=True)
    )

    yield slug_analyzed, inspection_runs


@pytest.mark.asyncio
async def test_slug_analyzed_no_edits(
    e2e_data: tuple[pd.DataFrame, pd.DataFrame], expected_analyzed: pd.DataFrame
):
    slug_analyzed, _ = e2e_data

    pd.testing.assert_frame_equal(
        slug_analyzed,
        expected_analyzed,
        check_names=False,
    )


@pytest.mark.asyncio
async def test_inspection_runs_no_edits(
    e2e_data: tuple[pd.DataFrame, pd.Dataframe],
    expected_inspection_runs: pd.DataFrame,
):
    _, inspection_runs = e2e_data
    pd.testing.assert_frame_equal(
        inspection_runs.drop(columns=["created_at"]),
        expected_inspection_runs.drop(columns=["created_at"]),
        check_names=False,
    )


@pytest.mark.asyncio
async def test_binned_plot_data_no_edits():
    ...


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
