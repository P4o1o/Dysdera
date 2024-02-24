"""
This file contains some class for managing webpages, sitemaps and robots.txt rules
"""
import heapq
import ssl
from abc import abstractmethod
from datetime import datetime
from itertools import count
from typing import Dict, List, Optional, Type, Callable, Union
import aiohttp
from dysdera.parser import AntParser, MosquitoParser, RobotsParser, URL, DysderaParser
from chardet import UniversalDetector
from dateutil import parser


class ResponseStatusException(Exception): # download failed

    def __init__(self, status: int):
        self.status = status

    def __str__(self) -> str:
        return str(self.status)


class ResponseStatusNotModified(Exception): # response: 304 not modified
    pass


class NotSavedException(Exception):
    pass


class MissingDownloadException(Exception): # if download doesn't go well or if you try to call methods without first downloading the page

    def __init__(self, url: URL):
        self.url = url()

    def __str__(self) -> str:
        return self.url


class WrongParserException(Exception): # if the sitemap or the robots.txt downloaded are not text type

    def __init__(self, url: URL, contenttype: str, parsercls: Type[DysderaParser]):
        self.url = url()
        self.parsercls = parsercls
        self.type = contenttype


class WebPage:
    """
    class for defining the default webpage utils
    """

    def __init__(self, session: aiohttp.ClientSession, url: URL, timeout: int, refer: URL = None,
                 if_modified_since: datetime = None):
        """
        params:     session             the aiohttp client session
                    url                 the page url
                    timeout             the max timeout for the requests
                    refer               the page from you visited this one
                    if_modified_since   if not None the page will be downloaded only if it was modifiey after the date you put (download will throw ResponseStatusNotModified)
        """
        self.session = session
        self.url = url
        self.timeout = timeout
        self.type = ''
        self.head = None
        self.parser = None
        self.refer = refer
        self.if_modify_since = if_modified_since
        self.last_modify = None

    def __enter__(self):
        self.download()
        return self

    def cames_from_same_domain(self) -> bool:
        if self.refer is None:
            return False
        return self.url.same_domain(self.refer)

    @property
    def request_header(self) -> dict:
        """
        returns the headers for the http request
        """
        res = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
            'Accept-Language': 'it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'br, gzip, deflate, zstd, snappy, lz4',
            "Sec-Ch-Ua": '"Not A(Brand";v="99", "Microsoft Edge";v="121", "Chromium";v="121"',
            "Sec-Ch-Ua-Mobile": "?0",
            "Sec-Ch-Ua-Platform": "Windows",
            "Upgrade-Insecure-Requests": "1",
        }
        if self.refer is not None:
            res['Refer'] = self.refer()
        if self.if_modify_since is not None:
            res['If-Modified-Since'] = self.if_modify_since.strftime("%a, %d %b %Y %H:%M:%S %Z")
        return res

    @staticmethod
    def parse_web_date(date: str) -> Optional[datetime]: # from string to datetime
        if date is not None:
            try:
                return datetime.strptime(date, '%a, %d %b %Y %H:%M:%S %Z')  # parsing di data nativo più rapido
            except ValueError:  # non sempre il timezone viene riconosciuto
                try:
                    return parser.parse(date)
                finally:
                    return datetime.strptime(date, '%a, %d %b %Y %H:%M:%S')
            finally:
                return None

    def _set_head(self, response):
        self.last_modify = self.parse_web_date(response.headers.get('Last-Modified'))
        self.head = {'type': response.headers.get('Content-Type'),
                     'length': response.headers.get('Content-Length'),
                     'cache': response.headers.get('Cache-Control'),
                     'death': response.headers.get('Expires'),
                     'etag': response.headers.get('ETag'),
                     'server': response.headers.get('Server')}

    async def _set_content(self, response):
        content_type = self.head['type'].lower()
        if 'text' in content_type or 'html' in content_type or 'xml' in content_type or 'json' in content_type or 'css' in content_type or 'javascript' in content_type:
            self.type = 'text'
            if 'charset' in content_type:
                encoding = content_type.split('charset=')[-1]
                content = await response.text(encoding=encoding)
            else:  # Se il campo Content-Type non contiene la codifica, usa chardet
                part = await response.read()
                detector = UniversalDetector()
                detector.feed(part)
                detector.close()
                result = detector.result
                encoding = result['encoding']
                content = await response.text(encoding=encoding)
        elif 'application' or 'pdf' in content_type or 'image' in content_type or 'audio' in content_type or 'video' in content_type:
            self.type = 'byte'
            content = await response.read()
        else:
            self.type = 'unknown'
            content = await response.text()
        self.set_text_parser(content)
        return

    async def get_header_info(self, without_ssl: bool = False):
        if self.head is None:
            ssl_context = ssl.create_default_context()
            if without_ssl:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
            async with self.session.head(self.url(), timeout=aiohttp.ClientTimeout(total=self.timeout),
                                         headers=self.request_header, ssl=ssl_context, allow_redirects=False) as response:
                status = response.status
                if status == 200:
                    self._set_head(response)
                elif int(status / 100) == 3:
                    if status == 304:
                        raise ResponseStatusNotModified()
                    newurl = response.headers.get('Location')
                    if newurl is not None: # page moved address, if new location in specified we move there
                        self.url = URL(newurl, from_page=self.url)
                        await self.download()
                    else:
                        raise ResponseStatusException(status)
                else:
                    raise ResponseStatusException(status)

    def is_html(self) -> bool:
        if self.head is None:
            raise MissingDownloadException(self.url)
        return 'html' in self.head['type'].lower()

    def __eq__(self, other) -> bool: # comparison based on urls
        return self.url == other.url

    def content_hash(self):
        if self.parser is None:
            raise MissingDownloadException(self.url)
        return self.parser.hash()

    def duplicate(self, other) -> bool: # comparison based on hashes
        if self.parser is None:
            raise MissingDownloadException(self.url)
        if other.parser is None:
            raise MissingDownloadException(other.url)
        return self.parser == other.parser

    def near_duplicate(self, other, max_dist=10, size_hash=64) -> bool: # comparison based on simhashes
        if self.parser is None:
            raise MissingDownloadException(self.url)
        if other.parser is None:
            raise MissingDownloadException(other.url)
        return self.parser.simhash_distance(other.parser, size=size_hash) < max_dist

    async def download(self, without_ssl: bool = False):
        if self.parser is None or self.parser.text is None:
            ssl_context = ssl.create_default_context()
            if without_ssl:
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
            async with self.session.get(self.url(), timeout=aiohttp.ClientTimeout(total=self.timeout),
                                        headers=self.request_header, ssl=ssl_context, allow_redirects=False) as response:
                status = response.status
                if int(status / 100) == 2:
                    self._set_head(response)
                    await self._set_content(response)
                elif int(status / 100) == 3:
                    if status == 304:
                        raise ResponseStatusNotModified()
                    newurl = response.headers.get('Location')
                    if newurl is not None: # page moved address, if new location in specified we move there
                        self.url = URL(newurl, from_page=self.url)
                        await self.download()
                    else:
                        raise ResponseStatusException(status)
                else:
                    raise ResponseStatusException(status)

    @abstractmethod
    def set_text_parser(self, content: Union[str, bytes]):
        pass


class WebSet:
    """
    a collection of WebPage, usefull for checking whether a page was already visited
    """

    def __init__(self):
        self.items = []

    def __iter__(self):
        self.pos = 0 # index
        return self

    def __next__(self) -> WebPage:
        if self.pos < len(self.items):
            res = self.items[self.pos]
            self.pos += 1
            return res
        else:
            raise StopIteration()

    def reset(self):
        self.items.clear()

    def contains_url(self, url: str | URL) -> bool: # comparison based on urls
        for elem in self.items:
            if elem.url == url:
                return True
        return False

    def contains_page(self, item: WebPage) -> bool: # comparison based on urls and last modify
        for elem in self.items:
            if elem == item:
                if elem.last_modify == item.last_modify:
                    return True
        return False

    def contains_duplicate(self, item: WebPage) -> bool: # comparison based on hashes
        for elem in self.items:
            if elem.duplicate(item):
                return True
        return False

    def contains_nearduplicate(self, item: WebPage, max_distance=10) -> bool: # comparison based on simhashes
        for elem in self.items:
            if elem.near_duplicate(item, max_dist=max_distance):
                return True
        return False

    def add(self, element: WebPage):
        if element not in self.items:
            self.items.append(element)

    def remove(self, element: WebPage):
        if element in self.items:
            self.items.remove(element)

    def __len__(self):
        return len(self.items)


class WebTarget(WebPage):
    """
    class for all html or not html pages we will meet during the crawling
    """

    def __getitem__(self, item) -> str:
        if self.head is None:
            raise MissingDownloadException(self.url)
        if item == 'content':
            return self.parser.text
        if item == 'url':
            return self.url()
        return self.head[item]

    def set_text_parser(self, content: str | bytes):
        self.parser = AntParser(content, self.type == 'text')

    @property
    def request_header(self) -> dict:
        res = super().request_header
        res['Accept'] = 'text/html;q=1, application/xhtml+xml;q=0.9, */*;q=0.8' # we ask only for html pages
        return res

    def lxml_is_html(self) -> bool: # check based on the content not on the header of response
        if self.parser is None:
            raise MissingDownloadException(self.url)
        return self.parser.html_content()

    def extract_links(self) -> List[URL]:
        if self.parser is None:
            raise MissingDownloadException(self.url)
        return self.parser.get_links(self.url)

    def canonical_url(self) -> Optional[URL]:
        if self.parser is None:
            raise MissingDownloadException(self.url)
        return self.parser.get_canonical_url()

    def extract_titles(self) -> List[str]:
        if self.parser is None:
            raise MissingDownloadException(self.url)
        return self.parser.get_titles()

    def extract_page_title(self) -> str:
        if self.parser is None:
            raise MissingDownloadException(self.url)
        return self.parser.get_page_title()

    def extract_text(self) -> List[str]:
        if self.parser is None:
            raise MissingDownloadException(self.url)
        return self.parser.get_text()

    def extract_figcaptions(self) -> List[str]:
        if self.parser is None:
            raise MissingDownloadException(self.url)
        return self.parser.get_fig_caption()

    def extract_metadata(self) -> Dict[str, str]:
        if self.parser is None:
            raise MissingDownloadException(self.url)
        return self.parser.get_metadata()


class WebRobots(WebPage):
    """
    class for robots.txt pages
    """

    def set_text_parser(self, content: str, encoding: str = None):
        if self.type == 'text':
            self.parser = RobotsParser(content)
        else:
            raise WrongParserException(self.url, self.type, RobotsParser)

    @property
    def request_header(self) -> dict:
        res = super().request_header
        res['Accept'] = 'text/plain'
        return res

    @property
    def delay(self) -> Optional[int]:
        if self.parser is None:
            raise MissingDownloadException(self.url)
        return self.parser.polite_delay

    def process(self, agent=None): # to be called before calling get_sitemaps or delay
        if self.parser is None:
            raise MissingDownloadException(self.url)
        self.parser.parse(self.url, as_agent=agent)

    def get_sitemaps(self) -> Optional[WebSet]: # returns the sitemaps in a webset
        if self.parser is None:
            raise MissingDownloadException(self.url)
        res = WebSet()
        if len(self.parser.sitemap) > 0:
            for link in self.parser.sitemap:
                res.add(WebMap(self.session, link, self.timeout))
            return res
        return None


class RobotsRules:
    """
    class rappresenting the rules described in the robots.txt for every domain
    """

    def __init__(self):
        self.rules = dict() # rules saved as a dict with url.domain as key and value (prohibited dir, [list of allowed subdir])

    def got_rules_from(self, url: URL):
        return url.domain in self.rules

    def is_respected(self, by: URL):
        if by.domain not in self.rules or len(self.rules[by.domain]) == 0:
            return True
        for rule in self.rules[by.domain]:
            if by.parsed.path.startswith(rule[0]):
                for allowed in rule[1]:
                    if by.parsed.path.startswith(allowed):
                        return True
                return False
        return True

    def add_rules(self, robots: WebRobots):
        prohibs = sorted(list(robots.parser.prohibited), key=lambda s: len(s), reverse=True) # getting the longest stiring firs we will set the allowed subdirectory in the right prohibited directory position
        allows = sorted(list(robots.parser.allowed), key=lambda s: len(s), reverse=True)
        rule = []
        for proh in prohibs:
            encapsulated = []
            for allo in allows:
                if len(proh) > len(allo):
                    break
                if allo.startswith(proh): # an allowed dir inside a prohibited dir
                    encapsulated.append(allo)
                    allows.remove(allo)
            rule.append((proh, encapsulated))
        self.rules[robots.url.domain] = rule

    def delete_rules(self, ruler: URL):
        self.rules.pop(ruler.domain)

    def reset(self):
        self.rules.clear()


class WebMap(WebPage):
    """
    class for xml sitemap pages
    """

    def __init__(self, session: aiohttp.ClientSession, url: URL, timeout: int):
        super().__init__(session, url, timeout)
        self.map = None
        self.map_of_maps = None
        self.sorted_by_latest = None

    def set_text_parser(self, content: str):
        if self.type == 'text':
            try:
                self.parser = MosquitoParser(content)
            except Exception as e:
                print(self.url())
                raise e
            self.map_of_maps = self.parser.map_of_maps()
        else:
            raise WrongParserException(self.url, self.type, MosquitoParser)

    @property
    def request_header(self) -> dict:
        res = super().request_header
        res['Accept'] = 'application/xml;q=1, application/xhtml+xml;q=0.9'
        return res

    def process(self):
        if self.parser is None:
            raise MissingDownloadException(self.url)
        if self.map_of_maps:
            self.map, self.sorted_by_latest = self.parser.get_maps()
        else:
            self.map = self.parser.get_pages()

    def get_latest_map(self) -> Optional[URL]: # only if self.map_of_map
        if self.parser is None:
            raise MissingDownloadException(self.url)
        if self.map_of_maps:
            for mappa in self.map:
                if mappa().endswith('.xml'):
                    return mappa
        else:
            return None

    def get_all_maps(self) -> List[URL]: # only if self.map_of_map
        if self.parser is None:
            raise MissingDownloadException(self.url)
        if self.map_of_maps:
            return self.map
        else:
            return []

    def get_links(self) -> List[URL]: # only if not self.map_of_map
        if self.parser is None:
            raise MissingDownloadException(self.url)
        if not self.map_of_maps:
            return list(self.map.keys())
        else:
            return []

    def __getitem__(self, item: URL) -> Dict[str, str | bool]: # only if not self.map_of_map
        if self.parser is None:
            raise MissingDownloadException(self.url)
        return self.map[item]

    def save_map(self): # save to file
        if self.parser.text is None:
            raise MissingDownloadException(self.url)
        name = self.url.name()
        nome_file = f'{name}_sitemap_{self.last_modify}.xml'
        with open(nome_file, 'wb') as file:
            file.write(self.parser.text.encode('utf-8'))

    def remember_map(self): # load from file
        name = self.url.name()
        nome_file = f'{name}_sitemap_{self.last_modify}.xml'
        try:
            with open(nome_file, 'rb') as file:
                self.set_text_parser(file.read().decode('utf-8'))
        except Exception:
            raise NotSavedException()


class WebQueue:
    """
    class for the queue of pages to be crawled (lower is the cost sooner the page will be pop)
    """

    def __init__(self):
        self.heap = []
        self.counter = count()

    def is_empty(self) -> int:
        return len(self.heap) == 0

    def push(self, item: WebTarget, cost: Callable[[WebTarget], int]):
        """
        params:     item    the web page
                    cost    a function returnin the cost for visiting the page
        """
        heapq.heappush(self.heap, (cost(item), next(self.counter), item))

    def pop(self) -> WebTarget:
        if self.is_empty():
            raise IndexError("WebQueue is empty")
        cost, counter, item = heapq.heappop(self.heap)
        return item

    def __iter__(self):
        return self

    def __next__(self) -> WebTarget:
        if self.is_empty():
            raise StopIteration()
        cost, counter, item = heapq.heappop(self.heap)
        return item

    def __len__(self) -> int:
        return len(self.heap)
