from __future__ import annotations
from typing import TYPE_CHECKING

from kraken.utils.pipeline import combine_dicts_by_addition

from celery.canvas import group, Signature
from celery.result import allow_join_result

from dacite.core import from_dict

from kraken.core.spider import Spider
from kraken.core.types import PipelineResult, Stage

if TYPE_CHECKING:
    from kraken.core.spider import RequestResult


TERMINATOR_KEY_TARGET_NOT_FOUND = "target_not_found"
TERMINATOR_KEY_TARGET_EXHAUSTED = "target_exhausted"


# TODO: Rework, remove strong dependency to Stage
class StageProcessor:
    def __init__(self, stage: Stage, crawl_id: str, final_stage: bool = False) -> None:
        self.stage = stage
        self.crawl_id = crawl_id
        self.final_stage = final_stage

    def __execute_pipelines(self, request_result: RequestResult):
        # Return empty result if no pipelines are supplied
        if not self.stage.pipelines:
            return {}

        # Prepare signature for all pipelines
        pipeline_signatures = []

        for pipeline in self.stage.pipelines:
            # Build signature and inject the request_result and crawl_id
            pipeline_signatures.append(
                pipeline.clone(
                    kwargs={
                        "request_result": request_result,
                        "crawl_id": self.crawl_id,
                    }
                )
            )

        # Create a group based on the list of signatures
        pipeline_group = group(pipeline_signatures)

        # Execute group
        promise = pipeline_group.apply_async()

        # Wait for all results to be available.
        with allow_join_result():
            results = promise.join()

        # Deserialize results
        results = [
            from_dict(data_class=PipelineResult, data=result) for result in results
        ]

        # Change result list to dict with pipeline name as key
        return {
            pipeline.name: result
            for pipeline, result in zip(self.stage.pipelines, results)
        }

    def __execute_callbacks(self):
        # Stop if no pipelines are supplied
        if not self.stage.callbacks:
            return None

        # Prepare signature for all callbacks
        callback_signatures = []

        for callback in self.stage.callbacks:
            # Build signature and inject the stage
            callback_signatures.append(
                callback.clone(
                    kwargs={
                        "stage": self.stage,
                        "crawl_id": self.crawl_id,
                        "final_stage": self.final_stage,
                    }
                )
            )

        # Create a group based on the list of signatures
        callback_group = group(callback_signatures)

        # Execute group
        _ = callback_group.apply_async()

        return None

    def __execute_terminators(self, spider: Spider):
        # Check if a non natural termination is required
        for terminator in self.stage.terminators:
            # Build signature and inject the stage
            terminator_sig: Signature = terminator.clone(kwargs={"stage": self.stage})

            # Check if terminator triggers
            with allow_join_result():
                if terminator_sig.apply().get():
                    self.stage.progress.terminated_by[terminator.name] = True

            # Check if a natural termination has occurred
            if spider.target_not_found:
                self.stage.progress.terminated_by[
                    TERMINATOR_KEY_TARGET_NOT_FOUND
                ] = True
            if spider.target_exhausted:
                self.stage.progress.terminated_by[
                    TERMINATOR_KEY_TARGET_EXHAUSTED
                ] = True

    def process(self):
        """Executes the stage specified by, target, request, pipeline and
        terminators step by step. After each step the current progress is returned.

        Yields:
            StageResult: Current results of the stage.
        """
        # Initialize spider
        spider = Spider(target=self.stage.target, task_signature=self.stage.request)  # type: ignore

        # Start crawling the target
        for request_result in spider:
            # Update progress
            self.stage.progress.cost += request_result.cost
            self.stage.progress.gain += request_result.gain

            # Process result only if the request produced data
            if not request_result.target_not_found:

                # Feed result into all pipelines and await result
                _pipeline_results = self.__execute_pipelines(
                    request_result=request_result
                )

                # Aggregate pipeline results
                self.stage.progress.pipeline_results = combine_dicts_by_addition(
                    self.stage.progress.pipeline_results,
                    _pipeline_results,
                )

            yield self.stage.progress

            # Execute terminators
            _ = self.__execute_terminators(spider=spider)

            # Stop the generator when at least one terminator has triggered
            if True in self.stage.progress.terminated_by.values():
                break

        # Invoke callbacks before termination
        self.__execute_callbacks()
