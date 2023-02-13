from dataclasses import dataclass, field
from typing import List

from mongoengine.queryset.visitor import Q

from kraken.utils.mongodb import MongoEngineContextManager

from .crawl import Crawl
from .target import Target


@dataclass
class Bucket:
    """Bucket representing a range of targets based on a specific field. It
    allows to allocate resources across multiple buckets relative to their
    importance factor and size."""

    importance_factor: float  # Relative importance of the targets within the bucket
    lower_bound: int  # Inclusive
    upper_bound: int  # Exclusive
    absolute_size: int  # Amount of targets within the bucket
    path: str
    allocated_resources: float = field(
        default=None
    )  # Proportion of the total resources allocated to this bucket
    filter: Q = field(
        default_factory=Q
    )  # Additional filter applied to the targets, beside upper and lower bound

    def weight(self) -> float:
        """Function to calculate the weight of the bucket based on importance
        factor and the absolute size of the bucket.

        Returns:
            float: Product of importance factor and absolute size
        """
        return self.importance_factor * self.absolute_size

    def normalise(self, total_weight: float) -> None:
        """Function to normalise the bucket based on the total weight of
        multiple buckets. It allows to calculate the proportion of the total
        resources which should be allocated to this bucket.

        Args:
            total_weight (float): Sum of the weights of all buckets.

        Raises:
            ValueError: Raised if the bucket has already been normalized.
        """
        if self.allocated_resources is None:
            self.allocated_resources = self.weight() / total_weight
        else:
            raise ValueError("Can not normalize an already normalized bucket")

    def allocate(self, step_size: int, crawl: Crawl, min_allocation: int = 1) -> float:
        """Function to determine the next targets to which resources should be
        allocated to. The prioritization within a bucket is based on the last
        time resources have been allocated to a target within the Bucket.

        Args:
            step_size (int): Total amount of resources to bee allocated across
                all buckets.
            crawl (Crawl): Crawl for which resources should be allocated.
            min_allocation (int): Minimum amount of returned targets. Useful to
                prevent a bucket from starving, which can occur if the step size
                is chosen too small. Attention: This may cause the total amount
                of targets returned across all buckets to exceed step_size.
                Defaults to 1

        Raises:
            ValueError: Raised if the bucket has not been normalized.

        Returns:
            List[Target]: Targets to which resources should be allocated to.
        """
        if self.allocated_resources is None:
            raise ValueError("Can not allocate resources to a non-normalized bucket")

        with MongoEngineContextManager():
            allocated_resources = max(
                min_allocation, round(step_size * self.allocated_resources)
            )
            # Prioritise endpoints which have never been queued before
            targets: List[Target] = Target.objects(
                self.filter
                & Q(  # noqa
                    **{
                        f"{self.path}__gte": self.lower_bound,
                        f"{self.path}__lt": self.upper_bound,
                        f"last_queued__{crawl.name}__exists": False,
                    }
                )
            ).limit(allocated_resources)

            # If the resources are not yet exhausted fill up with endpoints
            # that have already been queued before
            if len(targets) < allocated_resources:
                additional_targets = (
                    Target.objects(
                        self.filter
                        & Q(  # noqa
                            **{
                                f"{self.path}__gte": self.lower_bound,
                                f"{self.path}__lt": self.upper_bound,
                                f"last_queued__{crawl.name}__exists": True,
                            }
                        )
                    )
                    .order_by(
                        f"last_queued__{crawl.name}"
                    )  # prioritize the endpoints that have not been queued for the longest time
                    .limit(allocated_resources - len(targets))
                )
                targets = [*targets, *additional_targets]

        return targets
