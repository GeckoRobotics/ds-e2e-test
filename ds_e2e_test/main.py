import asyncio

import aiohttp
from gcloud.aio.storage import Bucket, Storage
from google.cloud import bigquery
from tests.test_end_to_end import export_bigquery_table, download_blob


async def main():
    # client = bigquery.Client("gecko-dev-data-systems")
    # export_bigquery_table(
    #     client,
    #     "gecko-dev-data-systems",
    #     "data_systems",
    #     "20220901-0bd5e8_inspection_runs",
    #     "in-gecko-ben-green-staging"
    # )
    async with aiohttp.ClientSession() as session:
        # setup
        slug = "20220901-0bd5e8"
        client = Storage(session=session)
        test_staging_bucket = Bucket(client, "in-gecko-ben-green-staging")
        inspection_data = await download_blob(test_staging_bucket, f"{slug}_inspection_runs.csv")
        with open('inspection_data.csv', 'wb') as f:
            f.write(inspection_data)


if __name__ == "__main__":
    asyncio.run(main())
