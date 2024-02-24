"""
file containing the logic of the web crawler
"""
import asyncio
from ssl import SSLCertVerificationError
from typing import Optional, Tuple
import aiohttp
from dysdera.extractors import DysderaExtractor
from dysdera.logger import DysderaLogger
from dysdera.parser import SitemapException, URL
from dysdera.web import \
    WebTarget, ResponseStatusException, WebMap, WebRobots, WebQueue, WebSet, MissingDownloadException, RobotsRules, \
    WebPage, ResponseStatusNotModified
from dysdera.policy import Policy
from xml.etree.ElementTree import ParseError


class DysderaCrawler:

    def __init__(self, verbose=False, verbose_log=False, max_timeout=10, duplicate_sensibility: int = 0): # if duplicate_sensibility  <= 0 nothing, if duplicate_sensibility == 1 check for identical hashes, if if duplicate_sensibility > 1 simhash with maximum distance if duplicate_sensibility
        self.visited = WebSet()
        self.visited_lock = asyncio.Lock()
        self.robots = RobotsRules()
        self.robots_lock = asyncio.Lock()
        self.timeout = max_timeout
        self.logger = DysderaLogger(verbose, verbose_log)
        self.duplicate_sensibility = duplicate_sensibility
        self.domains_queues = dict()
        self.new_queue_lock = asyncio.Lock()
        self.event_queue = asyncio.Queue()
        self.premature_end = False

    def violate_duplicate_policy(self, x: WebPage):
        if self.duplicate_sensibility <= 0:
            return False
        elif self.duplicate_sensibility == 1:
            return self.visited.contains_duplicate(x)
        else:
            return self.visited.contains_nearduplicate(x, max_distance=self.duplicate_sensibility)

    async def load_queue(self, page: WebTarget, policy: Policy, sitemap: WebMap = None):
        if policy.headers_before_visit(page):
            try:
                await page.get_header_info()
            except SSLCertVerificationError:
                if policy.force_without_ssl(page):
                    await page.get_header_info(without_ssl=True)
                else:
                    return
            except ResponseStatusNotModified:
                return
            except ResponseStatusException:
                return
            except asyncio.TimeoutError:
                return
            except aiohttp.ClientConnectionError:
                return
        if await policy.should_visit(page, sitemap=sitemap):
            new = False
            if page.url.domain not in self.domains_queues:
                new = True
                async with self.new_queue_lock:
                    self.domains_queues[page.url.domain] = WebQueue()
            if sitemap is None:
                self.domains_queues[page.url.domain].push(
                    page,
                    policy.queue_weight()
                )
            else:
                self.domains_queues[page.url.domain].push(
                    page,
                    policy.map_queue_weight(sitemap)
                )
            if new:
                await self.event_queue.put(page.url.domain_str())

    async def search_robots(self, session: aiohttp.ClientSession, url: str,
                            policy: Policy, as_agent: str = None) -> Tuple[Optional[int], Optional[WebSet]]:
        duty = "Searching robots.txt"
        robots_url = URL("/robots.txt", from_page=url)
        try:
            self.logger.info_output(f"Acquiring robots.txt", duty, at=url)
            robot = WebRobots(session, robots_url, self.timeout)
            try:
                await robot.download()
            except SSLCertVerificationError:
                self.logger.err_output("SSL certificate verify failed", duty, blame=robots_url)
                if policy.force_without_ssl(robot):
                    self.logger.info_output("Forcing download without ssl", duty, at=robots_url)
                    await robot.download(without_ssl=True)
                else:
                    self.logger.warn_output(f"robots.txt not found", duty, blame=url)
                    return None, None
            finally:
                async with self.visited_lock:
                    self.visited.add(robot)
                self.logger.info_output(f"robots.txt downloaded", duty, at=robots_url)
                robot.process(as_agent)
                async with self.robots_lock:
                    self.robots.add_rules(robot)
                self.logger.info_output(f"Robots.txt processed", duty, at=robots_url)
                return robot.delay, robot.get_sitemaps()
        except MissingDownloadException as e:  # per errori di download o pagine non gestibili
            self.logger.err_output("Missing download", duty, blame=e.__str__())
        except ResponseStatusException as e:
            self.logger.err_output("Error downloading, HTTP response code: " + e.__str__(), duty, blame=robots_url)
        except SSLCertVerificationError:
            self.logger.err_output("SSL certificate verify failed", duty, blame=robots_url)
        except asyncio.TimeoutError:
            self.logger.err_output("Timeout", duty, blame=robots_url)
        except aiohttp.ClientConnectionError:
            self.logger.err_output("Connection error", duty, blame=robots_url)
        except aiohttp.ClientError:
            self.logger.err_output("Client error", duty, blame=robots_url)
        finally:
            pass
        self.logger.warn_output(f"robots.txt not found", duty, blame=url)
        return None, None

    async def update_queue_from_sitemap(self, session: aiohttp.ClientSession, maps: WebSet,
                                        policy: Policy, politeness_delay: float):
        duty = "Sitemaps processing"
        if maps is None or len(maps) == 0:
            self.logger.err_output("missing sitemap", duty)
            return None
        found = set()
        for mappa in maps:
            try:
                politeness = asyncio.create_task(asyncio.sleep(politeness_delay))
                try:
                    await mappa.download()
                except SSLCertVerificationError:
                    self.logger.err_output("SSL certificate verify failed", duty, blame=mappa.url)
                    if policy.force_without_ssl(mappa):
                        self.logger.info_output("Forcing download without ssl", duty, at=mappa.url)
                        await mappa.download(without_ssl=True)
                    else:
                        continue
                self.logger.info_output("Sitemap downloaded", duty, at=mappa.url)
                async with self.visited_lock:
                    self.visited.add(mappa)
                mappa.process()
                if mappa.map_of_maps:
                    self.logger.info_output("This sitemap is a index of other sitemaps, updating sitemap list",
                                            duty,
                                            at=mappa.url)
                    all_maps = mappa.get_all_maps()
                    for sitemap in all_maps:
                        maps.add(WebMap(session, sitemap, self.timeout))
                    self.logger.info_output(f"List of Sitemaps Acquired", duty, at=mappa.url)
                else:
                    for link in mappa.get_links():
                        if (not self.visited.contains_url(link) and
                                (not policy.respect_robots or self.robots.is_respected(link)) and
                                link not in found):
                            new_target = WebTarget(session, link, self.timeout, if_modified_since=await policy.dload_if_modified_since(link))
                            await self.load_queue(new_target, policy, sitemap=mappa)
                            found.add(link)
                    self.logger.info_output("Sitemap processed", duty, at=mappa.url)
                await politeness
            except SitemapException as e:
                self.logger.err_output("Sitemap not supported " + e.__str__(), duty, blame=mappa.url)
                continue
            except ResponseStatusException as e:
                self.logger.err_output("Error downloading, HTTP response code: " + e.__str__(), duty, blame=mappa.url)
                continue
            except asyncio.TimeoutError:
                self.logger.err_output("Timeout", duty, blame=mappa.url)
                continue
            except aiohttp.ClientConnectionError:
                self.logger.err_output("Connection error", duty, blame=mappa.url)
                continue
            except MissingDownloadException as e:  # per errori di download o mappa inesistente
                self.logger.err_output("Missing download", duty, blame=e.__str__())
                continue
            except ParseError:
                self.logger.err_output("Can't parse this sitemap: written wrong", duty, blame=mappa.url)
            finally:
                continue
        self.logger.info_output(f"Queue upgraded with {len(found)} links", duty)

    async def priority_crawl(self, session, domain: URL, policy: Policy, extractor: DysderaExtractor, delay: float):
        duty = "Priority crawl"
        await self.load_queue(WebTarget(session, domain, self.timeout, if_modified_since=await policy.dload_if_modified_since(domain)), policy)
        for target in self.domains_queues[domain.domain]:
            try:
                if self.visited.contains_url(target.url):
                    continue
                politeness = asyncio.create_task(asyncio.sleep(delay))
                try:
                    await target.download()
                except SSLCertVerificationError:
                    self.logger.err_output("SSL certificate verify failed", duty, blame=target.url)
                    if policy.force_without_ssl(target):
                        self.logger.info_output("Forcing download without ssl", duty, at=target.url)
                        await politeness
                        politeness = asyncio.create_task(asyncio.sleep(delay))
                        await target.download(without_ssl=True)
                    else:
                        continue
                self.logger.info_output("Downloaded", duty, at=target.url)
                if self.violate_duplicate_policy(target):
                    self.logger.info_output(
                        f"Skipping page: duplicate of an already visited one (sensibility={self.duplicate_sensibility})",
                        duty, at=target.url)
                    continue
                savepage = asyncio.create_task(extractor.extract(target))
                async with self.visited_lock:
                    self.visited.add(target)
                if target.is_html():
                    can_url = target.canonical_url()
                    if policy.canonical_url and can_url is not None and (can_url != target.url):
                        self.logger.warn_output("Found a canonical url: " + can_url(), duty, blame=target.url)
                        if not self.visited.contains_url(can_url):
                            if not policy.respect_robots or self.robots.is_respected(can_url):
                                await self.load_queue(WebTarget(session, can_url, self.timeout, refer=target.url, if_modified_since=await policy.dload_if_modified_since(can_url)), policy)
                                self.logger.info_output(f"Queue upgraded with canonical url", duty, at=target.url)
                            else:
                                self.logger.info_output(f"Canonical url {can_url()} prohibited by robots.txt", duty,
                                                        at=target.url)
                        else:
                            self.logger.info_output(f"Canonical url {can_url()} already visited", duty, at=target.url)
                await savepage
                if await policy.should_crawl(target):
                    self.logger.info_output("Valid page, searching links", duty, at=target.url)
                    for elem in target.extract_links():
                        if (not self.visited.contains_url(elem) and
                                (not policy.respect_robots or self.robots.is_respected(elem))):
                            await self.load_queue(WebTarget(session, elem, self.timeout, refer=target.url, if_modified_since=await policy.dload_if_modified_since(elem)), policy)
                    await politeness
            except ResponseStatusNotModified:
                self.logger.info_output("Page not modified, skipping", duty, blame=target.url)
            except MissingDownloadException as e:  # per errori di download o pagine non gestibili
                self.logger.err_output("Missing download", duty, blame=e.__str__())
            except ResponseStatusException as e:
                self.logger.err_output("Error downloading, HTTP response code: " + e.__str__(), duty,
                                       blame=target.url)
            except UnicodeDecodeError:
                self.logger.err_output("Encoding not supported", duty, blame=target.url)
            except TimeoutError:
                self.logger.err_output("Timeout", duty, blame=target.url)
            except aiohttp.ClientConnectionError:
                self.logger.err_output("Connection error", duty, blame=target.url)
            finally:
                continue

    async def crawl_domain(self, session, domain: str, policy: Policy, extractor: DysderaExtractor):
        self.logger.info_output('Starting', 'Domain Crawl', at=domain)
        delay, sitemaps = await self.search_robots(session, domain, policy, as_agent=policy.agent_name)
        delay = policy.default_delay if delay is None or delay <= 0 else delay
        entry = URL(domain)
        if sitemaps is not None and policy.visit_sitemap(entry):
            await self.update_queue_from_sitemap(session, sitemaps, policy, delay)
        await self.priority_crawl(session, entry, policy, extractor, delay)
        self.logger.info_output('Ended', 'Domain Crawl', at=domain)

    async def start(self, session, policy: Policy, extractor: DysderaExtractor, *domains: str):
        self.premature_end = False
        all_sub_task = []
        for domain in domains:
            all_sub_task.append(asyncio.create_task(self.crawl_domain(session, domain, policy, extractor)))
        try:
            while not self.premature_end:
                try:
                    new_domain = self.event_queue.get_nowait()
                    all_sub_task.append(asyncio.create_task(self.crawl_domain(session, new_domain, policy, extractor)))
                except asyncio.QueueEmpty:
                    pass
                await asyncio.sleep(10)
                for task in all_sub_task:
                    if task.done():
                        all_sub_task.remove(task)
                if len(all_sub_task) < 1:
                    break
        finally:
            pass
        for task in all_sub_task:
            task.cancel()


    async def terminate(self):
        async with asyncio.Lock():
            self.premature_end = True
