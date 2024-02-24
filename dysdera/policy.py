"""
in this class is defined the class Policy rappresenting the policy of the web crawler and its subclasses
"""
from datetime import datetime
from typing import Callable, Dict, Union
from dysdera.selectionpolicy import SchedulingCost
from dysdera.web import WebTarget, WebMap, WebPage
from dysdera.parser import URL
from motor.motor_asyncio import AsyncIOMotorCollection

async def unknown_last_modify(url: URL):
    return None


class Policy:

    def __init__(self, focus_policy: Callable[[WebTarget], bool] = lambda x: True, # should crawl?
                 sitemap_scheduling_cost: Callable[[Dict[str, Union[str, bool]]], int] = lambda x: 1,
                 scheduling_cost: Callable[[WebTarget], int] = lambda x: 1,
                 sitemap_selection_policy: Callable[[Dict[str, Union[str, bool]]], bool] = lambda x: True,  # should visit?
                 selection_policy: Callable[[WebTarget], bool] = lambda x: True,  # should visit?
                 headers_before_visit: Callable[[WebTarget], bool] = lambda x: False,
                 respect_robots=True, agent_name=None, canonical_url=True, default_delay: float = 5,
                 can_dload_without_ssl: Callable[[WebPage], bool] = lambda x: False,
                 visit_sitemap: Callable[[URL], bool] = lambda x: True, # should visit the sitemaps of this domain?
                 dload_if_modified_since = unknown_last_modify):
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

    def should_visit(self, link: WebTarget, sitemap: WebMap = None):
        if sitemap is None:
            return self.selection_policy(link)
        return self.selection_policy(link) and (self.sitemap_selection_policy(sitemap[link.url]))

    def queue_weight(self, not_in_map=1):
        return SchedulingCost.combine({self.scheduling_cost: not_in_map})

    def map_queue_weight(self, mappa: WebMap):
        def final_heuristic(x: WebTarget) -> int:
            return self.sitemap_scheduling_cost(mappa[x.url])

        return SchedulingCost.combine({
            self.scheduling_cost: 1,
            final_heuristic: 1
        })

    def should_crawl(self, x: WebTarget) -> bool:
        if x.is_html() and x.lxml_is_html():
            return self.focus_policy(x)
        else:
            return False


class DomainPolicy(Policy): # for all the pages in some required domains

    def __init__(self, *domains: str,
                 sitemap_scheduling_cost: Callable[[Dict[str, Union[str, bool]]], int] = lambda x: 1,
                 scheduling_cost: Callable[[WebTarget], int] = lambda x: 1):
        self.targets = {URL(domain) for domain in domains}
        super().__init__(focus_policy=lambda x: True, sitemap_selection_policy=lambda y: True,
                         selection_policy=self.url_same_domain, sitemap_scheduling_cost=sitemap_scheduling_cost,
                         scheduling_cost=scheduling_cost)

    def url_same_domain(self, x: WebTarget) -> bool:
        for target in self.targets:
            if x.url.same_domain(target):
                return True
        return False


class ExtendedDomainPolicy(Policy): # for all the pages in some required domains and the pages they poin to (visit all, crawl only if same domain)

    def __init__(self, *domains: str,
                 sitemap_scheduling_cost: Callable[[Dict[str, Union[str, bool]]], int] = lambda x: 1,
                 scheduling_cost: Callable[[WebTarget], int] = lambda x: 1):
        self.targets = {URL(domain) for domain in domains}
        super().__init__(focus_policy=self.page_same_domain, selection_policy=lambda x: True,
                         sitemap_selection_policy=lambda x: True, sitemap_scheduling_cost=sitemap_scheduling_cost,
                         scheduling_cost=scheduling_cost)

    def page_same_domain(self, x: WebTarget) -> bool:
        for target in self.targets:
            if x.url.same_domain(target):
                return True
        return False


class MongoMemoryPolicy(Policy): # visit the page only if was modified since last time, data from a mongodb collection 

    def __init__(self, collection: AsyncIOMotorCollection, focus_policy: Callable[[WebTarget], bool] = lambda x: True, sitemap_scheduling_cost: Callable[[Dict[str, str | bool]], int] = lambda x: 1, scheduling_cost: Callable[[WebTarget], int] = lambda x: 1, sitemap_selection_policy: Callable[[Dict[str, str | bool]], bool] = lambda x: True, selection_policy: Callable[[WebTarget], bool] = lambda x: True, headers_before_visit: Callable[[WebTarget], bool] = lambda x: False, respect_robots=True, agent_name=None, canonical_url=True, default_delay: float = 5, can_dload_without_ssl: Callable[[WebPage], bool] = lambda x: False, visit_sitemap: Callable[[URL], bool] = lambda x: True, dload_if_modified_since: Callable[[URL], datetime] = lambda x: None):
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
                    "visited": -1
                }
            },
            {
                "$limit": 1
            },
            {
                "$project": {
                    "_id": 0,
                    "visited": 1
                }
            }
        ]
        async with self.collection.aggregate(pipeline) as cursor:
            try:
                result = await cursor.next()
                return result["visited"]
            except StopAsyncIteration:
                return None
            finally:
                return None
