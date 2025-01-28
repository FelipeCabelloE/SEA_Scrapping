import asyncio
import os
from curl_cffi.requests import AsyncSession
import logging
from rich.logging import RichHandler
import time
from pymongo import MongoClient
import geopandas as gpd
from datetime import datetime

MONGO_LINK = "mongodb://root:example@localhost:27017/?authSource=admin"
MONDB_NAME = "SCRAPED_SEA"
# Collection name based on time scraped
MONCOL_NAME = f"proyects_{int(time.time())}"

client = MongoClient(MONGO_LINK)
db = client[MONDB_NAME]
collection = db[MONCOL_NAME]

start_time = time.time()

logging.basicConfig(
    format="{asctime} [{levelname}] {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
    handlers=[RichHandler()],
)

log = logging.getLogger("rich")

proyects = gpd.read_file("./DATA/proyectos_cleaned.gpkg")
# Get the first 10 constructed links
urls = proyects["constructed_link"][:10].tolist()


async def run():
    async with AsyncSession() as session:
        tasks = []

        for url in urls:
            task = session.get(url)
            tasks.append(task)

        result = await asyncio.gather(*tasks)
    return result


data = asyncio.run(run())

failed = []
results = []

for i, response in enumerate(data):
    if response.status_code != 200:
        log.warning(f"Request {urls[i]} failed with status code {response.status_code}")
        failed.append(response.url)
    else:
        results.append(
            {"url": response.url, "date": datetime.now(), "content": response.text}
        )

inserted = collection.insert_many(results)
log.info(f"Inserted {len(inserted.inserted_ids)} documents into {MONCOL_NAME}")

log.info(f"Failed requests: {failed}")

print(f"--- {time.time() - start_time} seconds ---")
