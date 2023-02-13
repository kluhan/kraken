from math import sqrt
from itertools import count
from typing import Iterator, List

from mongoengine.queryset.visitor import Q

from kraken.core.types.target import Target
from kraken.utils.mongodb import MongoEngineContextManager
from kraken.core.types import Bucket

from .abstract_resource_allocator import AbstractResourceAllocator


class UniformResourceAllocator(AbstractResourceAllocator):
    def __init__(self):
        super().__init__()

    def __recompute_buckets(
        self,
        weight_path,
        bucket_count=64,
        boundaries=None,
        importance_factors=None,
        filter=None,
    ):
        # Create default boundaries and relative resources per bucket if not supplied
        if boundaries is None and importance_factors is None:
            boundaries = [2**x for x in range(0, bucket_count - 1)]
            boundaries.insert(0, 0)
            importance_factors = [sqrt(x) for x in range(1, bucket_count + 1)]

        # Create empty filter is none is supplied
        if filter is None:
            filter = Q()

        # Connect to Database
        with MongoEngineContextManager():
            # Bucketing via MongoDB
            db_buckets = list(
                Target.objects(filter).aggregate(
                    [
                        {
                            "$bucket": {
                                "groupBy": f"${str.replace(weight_path, '__', '.')}",
                                "boundaries": boundaries,
                                "default": "unweighted",
                            },
                        }
                    ]
                )
            )
            total_size = Target.objects(filter).count()

        for index, bucket in enumerate(db_buckets):  # TODO rework edge-case
            if bucket["_id"] == "unweighted":
                db_buckets.pop(index)

        buckets: List[Bucket] = []

        # Calculate resources allocation for each bucket
        for ((index, db_bucket), importance_factor) in zip(
            enumerate(db_buckets), importance_factors
        ):
            # Determine the upper limit based on the lower limit of the next
            # bucket or, in the case of the last bucket, based on the highest
            # boundary
            upper_bound = (
                db_buckets[index + 1]["_id"]
                if index + 1 < len(db_buckets)
                else max(boundaries)
            )

            buckets.append(
                Bucket(
                    path=weight_path,
                    importance_factor=importance_factor,
                    lower_bound=db_bucket["_id"],
                    upper_bound=upper_bound,
                    absolute_size=db_bucket["count"],
                    relative_size=db_bucket["count"] / total_size,
                    filter=filter,
                )
            )

        # Normalise importance factor
        for bucket in buckets:
            bucket.normalise(sum(bucket.weight() for bucket in buckets))

        return buckets

    def allocate(
        self,
        crawl,
        weight_path,
        step_size=1000,
        bucket_ttl=10,
        boundaries=None,
        importance_factors=None,
        filter=None,
        bucket_count=64,
        database_update_delay=None,
    ) -> Iterator[List[Target]]:

        for iteration in count(0):
            # Recompute buckets if necessary
            if iteration % bucket_ttl == 0:
                buckets = self.__recompute_buckets(
                    weight_path=weight_path,
                    bucket_count=bucket_count,
                    boundaries=boundaries,
                    importance_factors=importance_factors,
                    filter=filter,
                )

            # Get endpoints for each bucket
            chunk = []

            for bucket in buckets:
                chunk += bucket.allocate(
                    step_size=step_size,
                    min_allocation=1,
                    crawl=crawl,
                )

            self.update_last_queued(
                crawl=crawl,
                endpoints=chunk,
                delay=database_update_delay,
            )
            yield chunk
