from typing import Iterator, List
from datetime import timedelta, datetime
from rich.progress import Progress, SpinnerColumn, TimeElapsedColumn, MofNCompleteColumn
from celery.canvas import Signature

from kraken.core.types import (
    Crawl,
    Target,
)
from kraken.utils.mongodb import MongoEngineContextManager

from .abstract_scheduler import AbstractScheduler


class StaticScheduler(AbstractScheduler):

    # Total amount of targets
    __total_targets = None

    def __init__(
        self,
        crawl: Crawl,
        crawl_task: Signature,
        step_size: int = 100,
        step_period: timedelta = timedelta(seconds=60),
    ):
        # Call super constructor
        super().__init__(
            crawl=crawl,
            crawl_task=crawl_task,
            step_size=step_size,
            step_period=step_period,
        )
        self.crawl: Crawl = crawl
        self.step_size: int = step_size
        # Compile filter
        self.filter: dict = {
            "$and": [
                {
                    "$or": [
                        # Targets which have never been queued
                        {f"queued.{self.crawl.series.id}": {"$exists": False}},
                        # Targets which have been queued, but not for this crawl
                        {
                            f"$expr": {
                                "$lt": [
                                    {"$last": f"$queued.{self.crawl.series.id}"},
                                    crawl.started,
                                ]
                            }
                        },
                    ]
                },
                self.crawl.get_filter(),
            ]
        }
        # Progress Bar
        self.progress = None

    def _allocate(self) -> Iterator[List[Target]]:
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
                    self.logger.info("No targets left. Exiting...")
                    break

            yield [*targets]

    def _targets_left(self) -> int:
        # Connect to Database
        with MongoEngineContextManager():
            # Get all targets which have not been queued for this crawl
            return Target.objects(__raw__=self.filter).count()

    def _targets_total(self, refresh: bool = False) -> int:
        # Load total targets if not already loaded or refresh is requested
        if self.__total_targets is None or refresh:
            # Connect to Database
            with MongoEngineContextManager():
                # Get all targets which qualify for this crawl (regardless of queue status)
                self.__total_targets = Target.objects(
                    __raw__=self.crawl.get_filter()
                ).count()
        return self.__total_targets

    def _display(self) -> None:
        if self.progress is None:
            self.total_targets = self._targets_total()
            self.progress = Progress(
                SpinnerColumn(),
                *Progress.get_default_columns(),
                MofNCompleteColumn(),
                TimeElapsedColumn(),
                expand=True,
            )
            self.progress_scheduled = self.progress.add_task(
                "[red]Scheduling...",
                total=self._targets_total(),
                completed=self.crawl.targets_scheduled,
            )
            self.progress_processed = self.progress.add_task(
                "[green]Processing...",
                total=self._targets_total(),
                completed=(self.crawl.targets_finished + self.crawl.targets_failed),
            )
            self.progress.start()
        else:
            self.progress.update(
                self.progress_scheduled,
                completed=self.crawl.targets_scheduled,
                total=self._targets_total(),
            )
            self.progress.update(
                self.progress_processed,
                completed=(self.crawl.targets_finished + self.crawl.targets_failed),
                total=self._targets_total(),
            )

    def start(self):

        for targets in self._allocate():
            # Submit targets
            self._submit(targets)
            with MongoEngineContextManager():
                self.crawl.reload()
            self._get_logger().info(f"Status: {self._get_status()}")
            self._display()
            self._wait()

        self.crawl.finished = datetime.now()
        self._display()
        while not self.progress.finished:
            with MongoEngineContextManager():
                self.crawl.reload()
            self._get_logger().info(f"Status: {self._get_status()}")
            self._display()
            self._wait()
