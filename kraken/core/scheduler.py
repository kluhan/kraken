from datetime import datetime

from dataclasses import asdict, replace
from typing import Iterable, List


from celery.canvas import Signature, chain, group

from kraken.core.allocators.abstract_resource_allocator import (
    AbstractResourceAllocator,
)
from kraken.core.types import Crawl, ExecutionToken, Series, Stage, Target
from kraken.utils import (
    MongoEngineContextManager,
    increment_nested_dict,
    combine_dicts_by_addition,
)


class AbstractScheduler:
    """
    A abstract scheduler that allows full control over the scheduling of Targets.

    Use caution when scheduling a large number of Targets at once or scheduling
    them too quickly, as this can cause backpressure on the broker and negatively
    impact performance.
    """

    def __init__(
        self,
        crawl: Crawl,
        crawl_task: Signature,
    ):
        self.crawl = crawl
        self.stages = crawl.stages
        self.crawl_task = crawl_task

        # Initialize statistics and submitted tasks
        self.submitted_tasks = 0

    def __submit(self, targets: List[Target]):
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
                        target.latest_statistics(str(self.crawl.series.id), stage_name=stage.name),
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
                target.update(**{f"push__queued__{str(self.crawl.series.id)}": timestamp})

            # Add the amount of scheduled targets to the update dict
            update = {"inc__targets_scheduled": len(targets)}

            # Add all expectations to update dict
            update = increment_nested_dict(expectations, "expectations", update)

            # Dispatch all collected updates
            self.crawl.update(**update)


class ManuelScheduler(AbstractScheduler):

    def submit(self, targets: List[Target]):
        return super().__submit(targets)




# Szenarien

# 1. Manuel Scheduler
# 2. Scheduler mit Resource Allocator
# * 2.1 Batch Scheduler (Static)
# * 2.2 Continuos Scheduler (Prop, Uniform)
# 3. Special Scheduler
# * 3.1 BFA Scheduler