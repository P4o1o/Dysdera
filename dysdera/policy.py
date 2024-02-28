"""
in this class is defined the class Policy rappresenting the policy of the web crawler and its subclasses
"""
from datetime import datetime
from typing import Awaitable, Callable, Dict, Union
from dysdera.selectionpolicy import SchedulingCost
from dysdera.web import WebTarget, WebMap, WebPage
from dysdera.parser import URL
from motor.motor_asyncio import AsyncIOMotorCollection

async def unknown_last_modify(url: URL):
    return None

async def default_false(x):
    return False

async def default_true(x):
    return True


class Policy:

    def __init__(self, focus_policy: Callable[[WebTarget], Awaitable[bool]] = default_true, # should crawl?  corutine
                 sitemap_scheduling_cost: Callable[[Dict[str, Union[str, bool]]], int] = lambda x: 1,
                 scheduling_cost: Callable[[WebTarget], int] = lambda x: 1,
                 sitemap_selection_policy: Callable[[Dict[str, Union[str, bool]]], bool] = lambda x: True,  # should visit?
                 selection_policy: Callable[[WebTarget],  Awaitable[bool]] = default_true,  # should visit?  corutine
                 headers_before_visit: Callable[[WebTarget],  Awaitable[bool]] = default_false,#  corutine
                 respect_robots=True, agent_name=None, canonical_url=True, default_delay: float = 5,
                 can_dload_without_ssl: Callable[[WebPage], bool] = lambda x: False,
                 visit_sitemap: Callable[[URL], bool] = lambda x: True, # should visit the sitemaps of this domain?
                 dload_if_modified_since = unknown_last_modify):
        """
        params:     focus_policy                corutine(WebTarget) -> bool     shoul I visit the links on the page?
                    selection_policy            corutine(WebTarget) -> bool     should I visit this page?
                    headers_before_visit        corutine(WebTarget) -> bool     should I get the headers for use them in the selection_policy before visit this page?
                    sitemap_selection_policy    (sitemaps infos) -> bool        should I visit this page?
                    scheduling_cost             (WebTarget) -> int              visit first the minor page
                    sitemap_scheduling_cost     (sitemaps infos) -> int         visit first the minor page
                    canonical_url               if True, if canonical url found must visit it
                    dload_if_modified_since     if not None download page if lastmod > dload_if_modified_since
                    respect_robots              respect robots.txt?
                    agent_name                  for robots.txt
                    default_delay               if dealy ismissing in robots.txt use default delay
                    visit_sitemap               (URL) -> bool      visit the sitemap of this domain?
        """
        self.focus_policy = focus_policy
        self.selection_policy = selection_policy
        self.headers_before_visit = headers_before_visit
        self.canonical_url = canonical_url
        self.visit_sitemap = visit_sitemap
        self.sitemap_selection_policy = sitemap_selection_policy
        self.scheduling_cost = scheduling_cost
        self.sitemap_scheduling_cost = sitemap_scheduling_cost
        self.respect_robots = respect_robots
        self.agent_name = agent_name
        self.default_delay = default_delay
        self.force_without_ssl = can_dload_without_ssl
        self.dload_if_modified_since = dload_if_modified_since

    async def should_visit(self, link: WebTarget, sitemap: WebMap = None):
        if sitemap is None:
            return self.selection_policy(link)
        return await self.selection_policy(link) and (self.sitemap_selection_policy(sitemap[link.url]))

    def queue_weight(self, not_in_map=1):
        return SchedulingCost.combine({self.scheduling_cost: not_in_map})

    def map_queue_weight(self, mappa: WebMap):
        def final_heuristic(x: WebTarget) -> int:
            return self.sitemap_scheduling_cost(mappa[x.url])

        return SchedulingCost.combine({
            self.scheduling_cost: 1,
            final_heuristic: 1
        })

    async def should_crawl(self, x: WebTarget) -> bool:
        if x.is_html() and x.lxml_is_html():
            return await self.focus_policy(x)
        else:
            return False


class DomainPolicy(Policy): # for all the pages in some required domains

    def __init__(self, *domains: str,
                 sitemap_scheduling_cost: Callable[[Dict[str, Union[str, bool]]], int] = lambda x: 1,
                 scheduling_cost: Callable[[WebTarget], int] = lambda x: 1):
        self.targets = {URL(domain) for domain in domains}
        super().__init__(focus_policy=default_true, sitemap_selection_policy=lambda y: True,
                         selection_policy=self.url_same_domain, sitemap_scheduling_cost=sitemap_scheduling_cost,
                         scheduling_cost=scheduling_cost)

    async def url_same_domain(self, x: WebTarget) -> bool:
        for target in self.targets:
            if x.url.same_domain(target):
                return True
        return False


class ExtendedDomainPolicy(Policy): # for all the pages in some required domains and the pages they poin to (visit all, crawl only if same domain)

    def __init__(self, *domains: str,
                 sitemap_scheduling_cost: Callable[[Dict[str, Union[str, bool]]], int] = lambda x: 1,
                 scheduling_cost: Callable[[WebTarget], int] = lambda x: 1):
        self.targets = {URL(domain) for domain in domains}
        super().__init__(focus_policy=self.page_same_domain, selection_policy=default_true,
                         sitemap_selection_policy=lambda x: True, sitemap_scheduling_cost=sitemap_scheduling_cost,
                         scheduling_cost=scheduling_cost)

    async def page_same_domain(self, x: WebTarget) -> bool:
        for target in self.targets:
            if x.url.same_domain(target):
                return True
        return False


class MongoMemoryPolicy(Policy): # visit the page only if was modified since last time, data from a mongodb collection 

    def __init__(self, collection: AsyncIOMotorCollection, focus_policy: Callable[[WebTarget], Awaitable[bool]] = default_true, sitemap_scheduling_cost: Callable[[Dict[str, str | bool]], int] = lambda x: 1, scheduling_cost: Callable[[WebTarget], int] = lambda x: 1, sitemap_selection_policy: Callable[[Dict[str, str | bool]], bool] = lambda x: True, selection_policy: Callable[[WebTarget], Awaitable[bool]] = default_true, headers_before_visit: Callable[[WebTarget], Awaitable[bool]] = default_false, respect_robots=True, agent_name=None, canonical_url=True, default_delay: float = 5, can_dload_without_ssl: Callable[[WebPage], bool] = lambda x: False, visit_sitemap: Callable[[URL], bool] = lambda x: True, dload_if_modified_since: Callable[[URL], datetime] = lambda x: None):
        self.collection = collection
        super().__init__(focus_policy, sitemap_scheduling_cost, scheduling_cost, sitemap_selection_policy, selection_policy, headers_before_visit, respect_robots, agent_name, canonical_url, default_delay, can_dload_without_ssl, visit_sitemap, self.was_not_modified)

    async def was_not_modified(self, page: URL):
        pipeline = [
            {
                "$match": {
                    "url": page()
                }
            },
            {
                "$sort": {
                    "lastmod": -1
                }
            },
            {
                "$limit": 1
            },
            {
                "$project": {
                    "_id": 0,
                    "lastmod": 1
                }
            }
        ]
        async with self.collection.aggregate(pipeline) as cursor:
            try:
                result = await cursor.next()
                return result["lastmod"]
            except StopAsyncIteration:
                return None
            finally:
                return None
