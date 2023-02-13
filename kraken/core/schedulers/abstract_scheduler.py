import logging
from datetime import datetime, timedelta

from dataclasses import asdict
from typing import List

from celery.canvas import Signature
from rich.logging import RichHandler
from pause import until

from kraken.core.types import Crawl, ExecutionToken, Target
from kraken.utils import (
    MongoEngineContextManager,
    increment_nested_dict,
    combine_dicts_by_addition,
)


class AbstractScheduler:
    def __init__(
        self,
        crawl: Crawl,
        crawl_task: Signature,
        step_size: int = 100,
        step_period: timedelta = timedelta(seconds=60),
    ):
        self.crawl = crawl
        self.stages = crawl.get_stages()
        self.crawl_task = crawl_task
        self.step_size = step_size
        self.step_period = step_period
        self.last_step = None

        # Initialize statistics and submitted tasks
        self.submitted_tasks = 0

        # Initialize logger
        logging.basicConfig(
            level="INFO",
            format="%(message)s",
            datefmt="[%X]",
            handlers=[RichHandler()],
        )
        self.logger = logging.getLogger("rich")

    def _get_logger(self) -> logging.Logger:
        return self.logger

    def _wait(self):
        """
        Waits until the next step should be executed. If the scheduler is running
        slower than specified, a warning is logged.
        """

        now = datetime.now()

        if self.last_step is None:
            self.last_step = now
        elif now >= self.last_step + self.step_period:
            self.last_step = now
            self.logger.warning(f"Scheduler is running slower than specified.")
        else:
            self.logger.info(f"Waiting until {self.last_step + self.step_period}")
            until(self.last_step + self.step_period)
            self.last_step = datetime.now()

    def _get_status(self) -> dict:
        """
        Returns the status of the scheduler.

        Returns:
            dict: The status of the scheduler.
        """

        return {
            "scheduled": self.crawl.targets_scheduled,
            "finished": self.crawl.targets_finished,
            "retried": self.crawl.targets_retried,
            "failed": self.crawl.targets_failed - self.crawl.targets_retried,
            "backpressure": self.crawl.targets_scheduled - self.crawl.targets_finished,
        }

    def _submit(self, targets: List[Target]):
        """
        Submits a crawl task for the given targets. This directly adds the
        crawl task to the queue in exactly the same order as the targets are given.

        Args:
            targets (List[Target]): The targets to be scheduled.
        """

        expectations = {stage.name: {} for stage in self.stages}

        # Open DB connection
        with MongoEngineContextManager():
            # timestamp used for the updates of the targets
            timestamp = datetime.now()

            # Queue each target
            for target in targets:
                # Inject Target in all Stages and sum up expectations based on
                # the results of the last crawl.
                for stage in self.stages:
                    # Inject Target into Stage
                    stage.target = target.slim()

                    # Sum up expectations
                    expectations[stage.name] = combine_dicts_by_addition(
                        expectations[stage.name],
                        target.latest_statistics(
                            str(self.crawl.series.id), stage_name=stage.name
                        ),
                    )

                # Create ExecutionToken to monitor execution
                token = ExecutionToken(
                    crawl=str(self.crawl.id), stages=[asdict(x) for x in self.stages]
                )
                token.save()

                # Build callable crawl task with all stages and token
                task = self.crawl_task.clone(
                    kwargs={
                        "crawl_id": str(self.crawl.id),
                        "stages": self.stages,
                        "execution_token_id": str(token.id),
                    }
                )

                # Call task
                _ = task.apply_async()

                # Mark targets as queued
                target.update(
                    **{f"push__queued__{str(self.crawl.series.id)}": timestamp}
                )

            # Add the amount of scheduled targets to the update dict
            update = {"inc__targets_scheduled": len(targets)}

            # Add all expectations to update dict
            update = increment_nested_dict(expectations, "expectations", update)

            # Dispatch all collected updates and reload
            self.crawl.update(**update)

            # Log the amount of scheduled targets
            self._get_logger().debug(f"Submitted {len(targets)} tasks to queue.")
