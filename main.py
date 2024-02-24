import asyncio
import aiohttp
from motor.motor_asyncio import AsyncIOMotorClient
from dysdera.dysderacrawler import DysderaCrawler
from dysdera.extractors import MongoExtractor
from dysdera.policy import Policy


async def main(collection):
    crawler = DysderaCrawler(verbose=True, max_timeout=50)
    async with aiohttp.ClientSession() as session:
        await crawler.start(session, Policy(),
                            MongoExtractor(collection),
                            'https://www.primevideo.com/', 'https://www.crunchyroll.com/', 'https://mediasetinfinity.mediaset.it/', 'https://aniplay.co/',
                            'https://www.netflix.com/', 'https://www.raiplay.it/', 'https://www.disneyplus.com/', 'https://streamingcommunity.express/')


if __name__ == "__main__":
    mongo = AsyncIOMotorClient("mongodb://localhost:27017")
    try:
        loop = asyncio.new_event_loop()
        loop.run_until_complete(main(mongo.dysderadb.film))
    finally:
        mongo.close()
