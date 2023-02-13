import concurrent.futures
import time

from datetime import timedelta
from typing import Iterator, List

from kraken.core.types import (
    Series,
    Crawl,
    Target,
)


class AbstractResourceAllocator:
    """Abstract class for all resource allocators."""

    def __init__(self) -> None:
        pass

    def allocate(self, crawl: Crawl, series: Series) -> Iterator[List[Target]]:
        raise NotImplementedError
