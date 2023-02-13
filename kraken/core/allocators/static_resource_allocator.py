from typing import Iterator, List

from kraken.core.types import (
    Series,
    Crawl,
    Target,
)
from kraken.utils.mongodb import MongoEngineContextManager

from .abstract_resource_allocator import AbstractResourceAllocator


class StaticResourceAllocator(AbstractResourceAllocator):
    def __init__(self, crawl: Crawl, step_size: int = 1000):
        self.crawl: Crawl = crawl
        self.step_size: int = step_size
        self.filter: dict = {
            "$and": [
                {
                    "$or": [
                        {f"queued.{self.crawl.series.id}": {"$exists": False}},
                        {f"queued.{self.crawl.series.id}.-1": {"$lt": crawl.started}},
                    ]
                },
                self.crawl.get_filter(),
            ]
        }

    def allocate(self) -> Iterator[List[Target]]:
        # Loop over all Endpoints
        while True:
            # Connect to Database
            with MongoEngineContextManager():
                # Get all targets which have not been queued for this crawl
                targets: List[Target] = (
                    Target.objects(__raw__=self.filter)
                    .order_by(f"queued.{self.crawl.series.id}.-1")
                    .limit(self.step_size)
                )

                # Break if no targets are left
                if len(targets) <= 0:
                    break

            yield [*targets]
