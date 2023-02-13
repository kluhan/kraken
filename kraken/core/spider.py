# TODO: Check if can be removed
from __future__ import annotations

# TODO: Check if can be removed
from typing import TYPE_CHECKING

from celery import Task
from celery.result import allow_join_result

from kraken.core.types import RequestResult

# TODO: Check if can be removed
if TYPE_CHECKING:
    from kraken.core.types import Target, SlimTarget


class Spider:
    def __init__(self, target: Target | SlimTarget, task_signature: Task):
        self.target = target
        self.task_signature = task_signature

        self.target_exhausted = False
        self.target_not_found = False
        self.parameters_for_next_request = target.kwargs

    def __iter__(self):
        return self

    def __next__(self) -> RequestResult:
        # Only perform a request if the target can be bound and isn't already exhausted
        if not self.target_exhausted and not self.target_not_found:

            # Create task based on request and parameters
            task = self.task_signature.apply_async(
                kwargs=self.parameters_for_next_request
            )

            # Wait until task is ready and get result
            with allow_join_result():
                request_result = RequestResult(**task.get())  # type: ignore

            # Set not_found flag if it is set in the response
            if request_result.target_not_found:
                self.target_not_found = True

            # Set exhausted flag if no new parameters have been returned
            if not request_result.subsequent_kwargs:
                self.target_exhausted = True

            # Update parameters for next request based on last request
            if not self.target_exhausted and not self.target_not_found:
                self.parameters_for_next_request.update(
                    **request_result.subsequent_kwargs
                )

            # Return result
            return request_result

        else:
            raise StopIteration
