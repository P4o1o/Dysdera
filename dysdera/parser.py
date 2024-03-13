"""
This file contains some parser necessary for the program
"""
from datetime import datetime
import pytz
from hashlib import md5, sha256
import os
import re
from typing import Optional, List, Dict, Tuple, Union
from urllib.parse import urlparse, urljoin
import xml.etree.ElementTree as ET
from lxml import html


class MalformedURLException(Exception):
    pass


def absolute_timestamp(date: datetime) -> float:
    system_tz = pytz.timezone('Europe/Rome')
    return system_tz.localize(date).timestamp()


class URL:
    """
    class for managing the urls
    """

    def __init__(self, url: str, from_page=None):
        """
        params:     url
                    from_page   the page from witch the link was extracted (necessary for dynamic links)
        if url doesn't starts with a valid http scheme or doesn't have any netloc from_page must not be None, else MalformedURLException is thrown
        """
        url = url.rstrip('/')
        if from_page is None and (url.startswith('//') or not url.startswith('http')):
            raise MalformedURLException()
        self.parsed = urlparse(url)
        if self.parsed.netloc == '':
            if from_page is None:
                raise MalformedURLException()
            else:
                root = from_page if isinstance(from_page, str) else from_page.parsed.geturl()
                self.parsed = urlparse(urljoin(root, url))
        if self.parsed.scheme != 'https':
            self.parsed = self.parsed._replace(scheme='https')

    def __call__(self) -> str:
        """
        return:     the actual url as a string
        """
        return self.parsed.geturl()

    def domain_str(self):
        """
        return:     the url as a string of the domain of the url in use
        """
        domain = self.parsed._replace(path='', query='', fragment='')
        return domain.geturl()

    def ext(self) -> str:
        """
        return:     the extension of url or an empty string if it has no extension
        """
        lista = os.path.splitext(self.parsed.path)
        return lista[1].lower() if len(lista) > 1 else ''

    def name(self) -> str:
        """
        return:     the name of the file that contains the web page
        """
        return os.path.split(os.path.splitext(self.parsed.path)[0])[1]

    @property
    def domain(self) -> str:
        """
        return:     the domain of the url (only the complete domain, es: google.com)
        """
        return self.parsed.netloc

    def same_domain(self, other) -> bool:
        return self.parsed.netloc == other.parsed.netloc

    def __eq__(self, other) -> bool:
        """
        can compare also string
        """
        if isinstance(other, str):
            oparsed = urlparse(other)
        elif isinstance(other, URL):
            oparsed = other.parsed
        else:
            return False
        return self.parsed.netloc == oparsed.netloc and self.parsed.path == oparsed.path \
            and self.parsed.query == oparsed.query

    def __hash__(self):
        return self.parsed.__hash__()


class DysderaParser:
    """
    class for defining the default parser for a webpage
    """

    def __init__(self, text: str or bytes):
        """
        param:      text    the content of the webpage, as string or as bytes
        """
        self.text = text
        self.r_hash = None # dont wont to recalculate every time
        self.s_hash = None

    def hash(self):
        """
        returns the sha256 hash of the whole page
        """
        if self.r_hash is None:
            vals = self.text if isinstance(self.text, bytes) else self.text.encode('utf-8')
            self.r_hash = sha256(vals)
        return self.r_hash

    def __eq__(self, other) -> bool:
        return self.hash() == other.hash()

    def simhash(self, hash_size) -> int:
        """
        returns the simhash of the content, works only on str type content
        """
        if isinstance(self.text, bytes):
            return self.hash()
        if self.s_hash is None:
            hashes = [0] * hash_size
            vals = self.text.decode('utf-8') if isinstance(self.text, bytes) else self.text
            for line in vals.split('\n'):
                for word in line.split():
                    t1wh = int(md5(word.encode('utf-8')).hexdigest(), 16)
                    for i in range(hash_size):
                        hashes[i] += ((t1wh >> i) & 1) * 2 - 1
            self.s_hash = 0
            for i in range(hash_size):
                if hashes[i] > 0:
                    self.s_hash |= 1 << i
        return self.s_hash

    def simhash_distance(self, ant, size=64) -> int:
        return bin(ant.simhash(size) ^ self.simhash(size)).count('1')  # Hamming distance on simhashes, a ^ b is True <-> a != b


class AntParser(DysderaParser):
    """
    class for parsing html pages
    """

    def __init__(self, text: str, text_type: bool = True):
        """
        params:     text        the html documet contet
                    text_type   if the content is text html
        """
        super().__init__(text)
        if text_type:
            if text.startswith("<!--?xml"): # lxml doesn't support this tipes of declarations
                val = re.sub(r'<!--\?xml\s.*?\?-->', '', text, 1)
            elif text.startswith("<?xml"):
                val = re.sub(r'<\?xml\s.*?\?>', '', text, 1)
            else:
                val = text
            self.tree = html.fromstring(val)
        else:
            self.tree = None

    def html_content(self) -> bool:
        """
        returns if the content is na acltual html page searching the <html> tag
        """
        if self.tree is None:
            return False
        ishtml = self.tree.xpath('//html')
        if ishtml is not None:
            return len(ishtml) > 0
        return False

    def get_page_title(self) -> Optional[str]:
        """
        returns the title of the page or None
        """
        title = self.tree.xpath('//head/title/text()')
        if title:
            return title[0]
        else:
            return None

    def get_titles(self) -> List[str]:
        """
        returns the titles written on the page with the <h_> tag
        """
        title = [
            self.tree.xpath("//body//h1/text() | //body//h1/*/text()"),
            self.tree.xpath("//body//h2/text() | //body//h2/*/text()"),
            self.tree.xpath("//body//h3/text() | //body//h3/*/text()"),
            self.tree.xpath("//body//h4/text() | //body//h4/*/text()"),
            self.tree.xpath("//body//h5/text() | //body//h5/*/text()"),
            self.tree.xpath("//body//h6/text() | //body//h6/*/text()")
        ]
        res = []
        for i in range(3):
            if title[i]:
                res.extend(title[i])
        return res

    def get_text(self) -> Optional[str]:
        """
        returns the text on the page under the <p> tag
        """
        caption = self.tree.xpath("//body//p/text() | //body//p//*/text()")
        if caption:
            return caption
        return None

    def get_article_title(self) -> List[str]:
        """
        returns the titles written on the page with the <h_> tag only in the article part of the page
        """
        article = [
            self.tree.xpath("//body//article//h1/text()"),
            self.tree.xpath("//body//article//h2/text()"),
            self.tree.xpath("//body//article//h3/text()"),
            self.tree.xpath("//body//*[contains(@class, 'article')]//h1/text()"),
            self.tree.xpath("//body//*[contains(@class, 'article')]//h2/text()"),
            self.tree.xpath("//body//*[contains(@class, 'article')]//h3/text()")
        ]
        res = []
        for i in range(6):
            if article[i]:
                res.extend(article[i])
        return res

    def get_article_text(self) -> Optional[List[str]]:
        """
        returns the text on the page under the <p> tag only in the article part of the page
        """
        article = self.tree.xpath("//body//article//p/text()")
        if not article:
            article = self.tree.xpath("//body//*[contains(@class, 'article')]//p/text()")
            if not article:
                return None
        return article

    def get_links(self, url: URL) -> List[URL]:
        """
        returns the links on the page under the <a> tag
        """
        links = []
        for link in self.tree.xpath("//a[@href]"):
            found_link = link.get("href")
            if found_link == "/":
                continue
            try:
                url = URL(found_link, from_page=url)
                links.append(url)
            except MalformedURLException:
                continue
        return links

    def get_fig_caption(self) -> Optional[List[str]]:
        """
        returns the figcaption on the page under the <figcaption> tag
        """
        caption = self.tree.xpath("//body//figcaption/text() | //body//figcaption//*/text()")
        if caption:
            return caption
        return None

    def get_canonical_url(self) -> Optional[URL]:
        """
        returns the canonical url of the page if it has it
        """
        canonical_link = self.tree.find(".//link[@rel='canonical']")
        if canonical_link is not None:
            return URL(canonical_link.get("href"))
        else:
            return None

    def get_metadata(self) -> dict:
        """
        returns the metadata of the page as a dict
        """
        description = self.tree.find('.//meta[@name="description"]')
        keywords = self.tree.find('.//meta[@name="keywords"]')
        author = self.tree.find('.//meta[@name="author"]')
        language = self.tree.find('.//html')
        metadata = {
            'description': description.get('content') if description is not None else '',
            'keywords': keywords.get('content') if keywords is not None else None,
            'author': author.get('content') if author is not None else '',
            'language': language.get('lang') if language is not None else ''
        }
        return metadata


class SitemapException(Exception):
    def __init__(self, tipo: str):
        self.tipo = tipo

    def __str__(self) -> str:
        return "Sitemap: " + self.tipo + " not supported"


class MosquitoParser(DysderaParser):
    """
    class for parsing xml sitemaps
    """

    def __init__(self, text: str):
        """
        params:     text        the xml content
        if not content is not in the xml format throws a xml.etree.ElementTree.ParseError
        """
        super().__init__(text)
        self.root = ET.fromstring(self.text)

    def map_of_maps(self) -> bool:
        """
        returns if the sitemap is a map of other sitemaps
        """
        if self.root.tag.endswith('sitemapindex'):
            return True
        elif self.root.tag.endswith('urlset'):
            return False
        else:
            raise SitemapException(self.root.tag) # file xml not supported

    def get_maps(self) -> Tuple[List[URL], bool]:
        """
        returns the sitemaps in the current sitemap and if the maps has a lastmodified field (if it is, probably the sitemap is a map of versions of a sitemap, but it is not certain), to call only if self.map_of_maps(self) == True
        """
        res = dict()
        ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9',
              'ns_old': 'http://www.google.com/schemas/sitemap/0.84'}
        contains_lastmod = False
        for sitemap_element in self.root.findall('.//ns:sitemap', namespaces=ns):
            check = sitemap_element.find('ns:loc', namespaces=ns)
            if check is None:
                continue
            loc = check.text.strip()
            lastmod = sitemap_element.find('ns:lastmod', namespaces=ns)
            if lastmod is not None and lastmod.text is not None:
                res[URL(loc)] = lastmod.text.strip()
                contains_lastmod = True
            else:
                res[URL(loc)] = 0
        for sitemap_element in self.root.findall('.//ns_old:sitemap', namespaces=ns):
            check = sitemap_element.find('ns_old:loc', namespaces=ns)
            if check is None:
                continue
            loc = check.text.strip()
            lastmod = sitemap_element.find('ns_old:lastmod', namespaces=ns)
            if lastmod is not None and lastmod.text is not None:
                res[URL(loc)] = lastmod.text.strip()
                contains_lastmod = True
            else:
                res[URL(loc)] = 0
        if contains_lastmod:
            return sorted(res, key=lambda x: res[x]), True  # formato di lastmod: ISO 8601, sort funziona
        else:
            return list(res.keys()), False

    def get_pages(self) -> Dict[URL, Dict[str, Union[str, bool]]]:
        """
        returns the pages in the current sitemap with their infos in a dict, to call only if self.map_of_maps(self) == False
        """
        res = {}
        ns = {
            'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9',
            'news': 'http://www.google.com/schemas/sitemap-news/0.9'
        }
        for url_element in self.root.findall('.//ns:url', namespaces=ns):
            indr = url_element.find('ns:loc', namespaces=ns)
            if indr is None:
                continue
            url = URL(indr.text.strip())
            lmod = url_element.find('ns:lastmod', namespaces=ns)
            cfreq = url_element.find('ns:changefreq', namespaces=ns)
            pri = url_element.find('ns:priority', namespaces=ns)
            news_element = url_element.find('news:news', namespaces=ns)
            news = news_element is not None
            if news:
                nam = news_element.find('news:publication/news:name', namespaces=ns)
                leng = news_element.find('news:publication/news:language', namespaces=ns)
                date = news_element.find('news:publication_date', namespaces=ns)
                tit = news_element.find('news:title', namespaces=ns)
                kw = news_element.find('news:keywords', namespaces=ns)
                res[url] = {
                    'lastmod': lmod.text.strip() if lmod is not None and lmod.text is not None else None,
                    'changefreq': cfreq.text.strip() if cfreq is not None and cfreq.text is not None else None,
                    'priority': pri.text.strip() if pri is not None and pri.text is not None else None,
                    'news': True,
                    'name': nam.text.strip() if nam is not None and nam.text is not None else None,
                    'lenguage': leng.text.strip() if leng is not None and leng.text is not None else None,
                    'date': date.text.strip() if date is not None and date.text is not None else None,
                    'title': tit.text.strip() if tit is not None and tit.text is not None else None,
                    'keywords': kw.text.strip() if kw is not None and kw.text is not None else None
                }
            else:
                res[url] = {
                'lastmod': lmod.text.strip() if lmod is not None and lmod.text is not None else None,
                'changefreq': cfreq.text.strip() if cfreq is not None and cfreq.text is not None else None,
                'priority': pri.text.strip() if pri is not None and pri.text is not None else None,
                'news': news
            }
        return res


class RobotsParser(DysderaParser):
    """
    class for parsing robots.txt files
    """

    def __init__(self, text: str):
        super().__init__(text)
        self.sitemap = set()
        self.prohibited = set()
        self.allowed = set()
        self.polite_delay = None

    def parse(self, url: URL, as_agent: List[str] = None):
        """
        params:     url         an url of the current domain, the robots.txt url is perfect
                    as_agent    a list of the agent name for the crawler, if None ["*"]
        parse the file and populates the class field
        """
        lines = self.text.split('\n')
        if as_agent is None:
            as_agent = ["*"]
        for i in range(len(lines)):
            if lines[i].startswith("#") or not lines[i].strip():
                continue
            elif lines[i].lower().startswith("user-agent:"):
                agent = lines[i][len("User-agent:"):].strip()
                if agent in as_agent:
                    i += 1
                    for j in range(i, len(lines)):
                        if lines[j].startswith("#") or not lines[j].strip():
                            continue
                        elif lines[j].lower().startswith("disallow:"):
                            self.prohibited.add(lines[j][len("Disallow:"):].strip())
                        elif lines[j].lower().startswith("allow:"):
                            self.allowed.add(lines[j][len("Allow:"):].strip())
                        elif lines[j].lower().startswith("crawl-delay:"):
                            delay = lines[j][len("Crawl-delay:"):].strip()
                            if delay.isdigit():
                                self.polite_delay = int(delay)
                        elif lines[j].lower().startswith("noindex:"):
                            self.prohibited.add(lines[j][len("Noindex:"):].strip())
                        elif lines[j].lower().startswith("nofollow:"):
                            self.prohibited.add(lines[j][len("Nofollow:"):].strip())
                        elif lines[i].lower().startswith("sitemap:"):
                            self.sitemap.add(URL(lines[i][len("Sitemap:"):].strip(), from_page=url))
                        elif lines[j].lower().startswith("user-agent:") or lines[j].lower().startswith("sitemap:"):
                            break
                        i = j - 1
            elif lines[i].lower().startswith("sitemap:"):
                self.sitemap.add(URL(lines[i][len("Sitemap:"):].strip(), from_page=url))
