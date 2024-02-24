"""
in this file there's some selection policy and functions to calculate the cost of visiting a page that could be helpful
"""
from datetime import datetime
import math
import sys
from typing import Any, Awaitable, Callable, Dict, Union
from dysdera.web import WebTarget, WebPage
from dysdera.parser import URL, absolute_timestamp

class AgedSelectionPolicy:
    """
    class witch object implements a selection policy based on the age of the page, calculated from a previus database of crawls
    """

    def __init__(self, collection, max_age = 20, not_present=False):
        self.collection = collection
        self.max_age = max_age
        self.not_present = not_present

    async def __call__(self, x: WebTarget) -> bool:
        pipeline = [
            {
                "$match": {
                    "url": x.url()
                }
            },
            {
                "$sort": {
                    "lastmod": -1
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "lastmod": 1,
                    "visited": 1
                }
            }
        ]
        async with self.collection.aggregate(pipeline) as cursor:
            try:
                t_istants = []
                visited_min = sys.maxsize
                visited_max = 0
                latestmod = 0
                changes = 0
                now = absolute_timestamp(datetime.now())
                prec_last_mod = None
                for doc in cursor:
                    lastmod = doc["lastmod"]
                    visited = doc["visited"]
                    if lastmod is None:
                        return self.not_present
                    lastv = absolute_timestamp(visited)
                    lastm = absolute_timestamp(lastmod)
                    if prec_last_mod is not None and prec_last_mod != lastm:
                        changes += 1
                    t_istants.append(lastv - lastm)
                    visited_min = lastv if lastv is not None and lastv < visited_min else visited_min
                    visited_max = lastv if lastv is not None and lastv > visited_max else visited_max
                    latestmod = lastm if lastm is not None and lastm > latestmod else latestmod
                    prec_last_mod = lastm
                known_for = visited_max - visited_min
                if known_for <= 0:
                    return self.not_present
                t = visited_max - latestmod
                lambd = changes/known_for
                age = (t + lambd * math.exp(-lambd * t) - 1) / lambd
                return age > self.max_age

            except StopAsyncIteration:
                return self.not_present
            finally:
                return self.not_present

class SelectionPolicy:

    @staticmethod
    async def must_contain(word: str) -> Callable[[WebTarget], Awaitable[bool]]:
        async def final_policy(target: WebTarget) -> bool:
            return word.lower() in target.url().lower()

        return final_policy

    @staticmethod
    async def same_domain(of_url: str) -> Callable[[WebTarget], Awaitable[bool]]:
        parsed_of = URL(of_url)

        async def final_policy(target: WebTarget) -> bool:
            return parsed_of.same_domain(target.url)

        return final_policy

    @staticmethod
    async def not_true(policy: Callable[[WebTarget], Awaitable[bool]]) -> Callable[[WebTarget], Awaitable[bool]]:
        async def final_policy(target: WebTarget) -> bool:
            return not await policy(target)

        return final_policy

    @staticmethod
    async def all_true(*policy: Callable[[WebTarget], Awaitable[bool]]) -> Callable[[WebTarget], Awaitable[bool]]:
        async def final_policy(target: WebTarget) -> bool:
            return all(await func(target) for func in policy)

        return final_policy

    @staticmethod
    async def at_least_one_true(*policy: Callable[[WebTarget], Awaitable[bool]]) -> Callable[[WebTarget], Awaitable[bool]]:
        async def final_policy(target: WebTarget) -> bool:
            return any(await func(target) for func in policy)

        return final_policy


class SelectionPolicyWithHeaders:
    @staticmethod
    async def modify_only_before(date: datetime, if_date_absent=False) -> Callable[[WebTarget], Awaitable[bool]]:
        date_stamp = absolute_timestamp(date)

        async def final_policy(target: WebTarget) -> bool:
            if target.last_modify is None:
                return if_date_absent
            return date_stamp > absolute_timestamp(target.last_modify)

        return final_policy

    @staticmethod
    async def modify_only_after(date: datetime, if_date_absent=False) -> Callable[[WebTarget], Awaitable[bool]]:
        date_stamp = absolute_timestamp(date)

        async def final_policy(target: WebTarget) -> bool:
            if target.last_modify is None:
                return if_date_absent
            return date_stamp < absolute_timestamp(target.last_modify)

        return final_policy

    @staticmethod
    async def modify_between(start: datetime, end: datetime, if_date_absent=False) -> Callable[[WebTarget], Awaitable[bool]]:
        start_stamp = absolute_timestamp(start)
        end_stamp = absolute_timestamp(end)

        async def final_policy(target: WebTarget) -> bool:
            if target.last_modify is None:
                return if_date_absent
            return start_stamp > absolute_timestamp(target.last_modify) < end_stamp

        return final_policy

    @staticmethod
    async def is_html() -> Callable[[WebTarget], Awaitable[bool]]:
        async def final_policy(target: WebTarget) -> bool:
            return target.is_html()
        return final_policy


class SitemapSelectionPolicy:

    @staticmethod
    def not_true(policy) -> Callable[[Dict[str, Union[str, bool]]], bool]:
        def final_policy(info: Dict[str, Union[str, bool]]) -> bool:
            return not policy(info)

        return final_policy

    @staticmethod
    def all_true(*policy) -> Callable[[Dict[str, Union[str, bool]]], bool]:
        def final_policy(info: Dict[str, Union[str, bool]]) -> bool:
            return all(func(info) for func in policy)

        return final_policy

    @staticmethod
    def at_least_one_true(*policy) -> Callable[[Dict[str, Union[str, bool]]], bool]:
        def final_policy(info: dict) -> bool:
            return any(func(info) for func in policy)

        return final_policy

    @staticmethod
    def modify_only_before(date: datetime, if_date_absent=False) -> Callable[[Dict[str, Union[str, bool]]], bool]:
        final_date = absolute_timestamp(date)

        def final_policy(info: Dict[str, Union[str, bool]]) -> bool:
            if info['lastmod'] is not None:
                return absolute_timestamp(WebPage.parse_web_date(info['lastmod'])) < final_date
            else:
                return if_date_absent

        return final_policy

    @staticmethod
    def modify_only_after(date: datetime, if_date_absent=False) -> Callable[[Dict[str, Union[str, bool]]], bool]:
        final_date = absolute_timestamp(date)

        def final_policy(info: Dict[str, Union[str, bool]]) -> bool:
            if info['lastmod'] is not None:
                return absolute_timestamp(WebPage.parse_web_date(info['lastmod'])) > final_date
            else:
                return if_date_absent

        return final_policy

    @staticmethod
    def modify_between(start: datetime, end: datetime,
                       if_date_absent=False) -> Callable[[Dict[str, Union[str, bool]]], bool]:
        start_stamp = absolute_timestamp(start)
        end_stamp = absolute_timestamp(end)

        def final_policy(info: Dict[str, Union[str, bool]]) -> bool:
            if info['lastmod'] is not None:
                return start_stamp < absolute_timestamp(WebPage.parse_web_date(info['lastmod'])) < end_stamp
            else:
                return if_date_absent

        return final_policy

    @staticmethod
    def is_news() -> Callable[[Dict[str, Union[str, bool]]], bool]:
        def final_policy(info: Dict[str, Union[str, bool]]) -> bool:
            return info['news']

        return final_policy

    @staticmethod
    def news_contains(word: str) -> Callable[[Dict[str, Union[str, bool]]], bool]:
        def final_policy(info: Dict[str, Union[str, bool]]) -> bool:
            if not info['news']:
                return False
            check = False
            if info['title'] is not None:
                check = check or (word.lower() in info['title'].lower())
                if check:
                    return True
            if info['name'] is not None:
                check = check or (word.lower() in info['name'].lower())
                if check:
                    return True
            if info['keywords'] is not None:
                check = check or (word.lower() in info['keywords'].lower())
            return check

        return final_policy


class SchedulingCost:

    @staticmethod
    def fifo() -> Callable[[WebTarget], int]:
        def breadthfirstsearch(x: WebTarget) -> int:
            return 1

        return breadthfirstsearch

    @staticmethod
    def lifo() -> Callable[[WebTarget], int]:
        def depthfirstsearch(x: WebTarget) -> int:
            return -1

        return depthfirstsearch

    @staticmethod
    def from_selection_policy(selection_policy, ontrue=0, onfalse=100) -> Callable[[WebTarget], int]:
        def final_policy(x: WebTarget) -> int:
            return ontrue if selection_policy(x.url) else onfalse

        return final_policy

    @staticmethod
    def url_contains(word: str, cost=0, if_false=100) -> Callable[[WebTarget], int]:
        def final_policy(x: WebTarget) -> int:
            if word.lower() in x.url().lower():
                return cost
            return if_false

        return final_policy

    @staticmethod
    def combine(policy: Dict[Callable[[WebTarget], int], int]) -> Callable[[WebTarget], int]:
        def final_policy(x: WebTarget) -> int:
            res = sum(func(x) * policy[func] for func in policy.keys())
            return res

        return final_policy

    @staticmethod
    def multiply(policy_one, policy_two) -> Callable[[WebTarget], int]:
        def final_policy(x: WebTarget) -> int:
            return policy_one(x) * policy_two(x)

        return final_policy


class SchedulingCostWithHeader:

    @staticmethod
    def latest_modify(missing=0) -> Callable[[WebTarget], int]:
        def final_policy(x: WebTarget) -> int:
            date = x.last_modify
            if date is None:
                return missing
            return int(-absolute_timestamp(date))

        return final_policy


class SitemapSchedulingCost:

    @staticmethod
    def combine(policy: Dict[Callable[[Dict[str, Union[str, bool]]], int], int]) -> Callable[[Dict[str, Union[str, bool]]], int]:
        def final_policy(info: Dict[str, Union[str, bool]]) -> int:
            return sum(func(info) * policy[func] for func in policy.keys())
        return final_policy

    @staticmethod
    def from_selection_policy(sitemap_policy: Callable[[Dict[str, Union[str, bool]]], bool],
                              ontrue=1, onfalse=0) -> Callable[[Dict[str, Union[str, bool]]], int]:
        def final_policy(info: Dict[str, Union[str, bool]]) -> int:
            return ontrue if sitemap_policy(info) else onfalse

        return final_policy

    @staticmethod
    def latest_modify(missing=0) -> Callable[[Dict[str, Union[str, bool]]], int]:
        def final_policy(info: Dict[str, Union[str, bool]]) -> int:
            if info['lastmod'] is not None:
                return int(-absolute_timestamp(WebPage.parse_web_date(info['lastmod'])))
            return missing
        return final_policy
