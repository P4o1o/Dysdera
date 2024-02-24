"""
in this files are defined the information extractors
"""
import re
from abc import abstractmethod, ABC
from datetime import datetime
from typing import Callable
from dysdera.parser import absolute_timestamp
from dysdera.web import WebTarget
from motor.motor_asyncio import AsyncIOMotorCollection
from aiofiles import open as aio_open


class DysderaExtractor(ABC):
    """
    abstract class for the informarion extractor
    """

    @abstractmethod
    async def extract(self, x: WebTarget): # in this class shuold be defined the logic of the page saving
        pass


class MongoExtractor(DysderaExtractor): # extractors that saves crawl information in a mongodb collection

    def __init__(self, collection: AsyncIOMotorCollection,
                 save_if: Callable[[dict], bool] = lambda x: True):
        self.save_if = save_if
        self.collection = collection

    @staticmethod
    def page_to_dict(x: WebTarget) -> dict:
        titoli = ""
        titll = x.extract_titles()
        if titll is not None:
            titoli = " ".join(titll)
        testo = ""
        txt = x.extract_text()
        if txt is not None:
            testo = " ".join(txt)
        figcapts = ""
        fcextract = x.extract_figcaptions()
        if fcextract is not None:
            figcapts = " ".join(fcextract)
        canonicurl = x.canonical_url()
        return {'url': x.url(),
                'domain': x.url.domain,
                'name': x.extract_page_title(),
                'titles': re.sub(r'\s+', ' ', titoli),
                'text': re.sub(r'\s+', ' ', testo),
                'figcapt': re.sub(r'\s+', ' ', figcapts),
                'links': [link() for link in x.extract_links() if link is not None],
                'canonical_url': canonicurl() if canonicurl is not None else None,
                'meta': x.extract_metadata(),
                'visited': datetime.now(),
                'lastmod': x.last_modify,
                'timestamp_UTC': absolute_timestamp(x.last_modify) if x.last_modify is not None else None}

    async def extract(self, x: WebTarget):
        html = x.is_html()
        if html is None:
            html = x.lxml_is_html()
        if html:
            page = self.page_to_dict(x)
            if self.collection is not None and self.save_if(page):
                await self.collection.insert_one(page)


class FileExtractor(DysderaExtractor): # extractors that saves all files with one of the required extension

    def __init__(self, *extension: str, output_dir: str = ""):
        self.ext = extension
        self.out_dir = output_dir

    async def extract(self, x: WebTarget):
        exten = x.url.ext()
        if exten is not None or exten != '':
            if exten in self.ext:
                async with aio_open(f'{self.out_dir}/{x.url.name()}.{exten}', 'wb') as file:
                    await file.write(x.parser.text)
        elif not x.is_html():
            for ex in self.ext:
                if ex in x.head['type']:
                    async with aio_open(f'{self.out_dir}/{x.url.name()}.{ex}', 'wb') as file:
                        await file.write(x.parser.text)
                    break