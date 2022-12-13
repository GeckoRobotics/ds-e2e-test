import asyncio
import ssl

import aiohttp
import certifi
from gcloud.aio.storage import Storage, Bucket


async def main():
    slug = "20220901-0bd5e8"
    # ssl_context = ssl.create_default_context(cafile=certifi.where())
    # conn = aiohttp.TCPConnector(ssl=ssl_context)
    async with aiohttp.ClientSession() as session:
        # setup
        client = Storage(session=session)
        working_staging_bucket = Bucket(client, "gecko-working-staging")
        staging_files = await working_staging_bucket.list_blobs(
            f"{slug}/deliverable/ds"
        )
    print(staging_files)


if __name__ == "__main__":
    asyncio.run(main())
