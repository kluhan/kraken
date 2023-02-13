import itertools

from typing import List, Union
from celery import shared_task

from kraken.core.types import (
    RequestResult,
    PipelineResult,
    Target,
    Crawl,
)
from kraken.core.tasks import DatabaseTask
from mongoengine import BulkWriteError


# TODO: Add option to disable metrics, weights and statistics
@shared_task(
    bind=True,
    base=DatabaseTask,
    name="kraken.pipeline.target_discovery",
)
def target_discovery_pipeline(
    self,
    request_result: RequestResult,
    crawl_id: str,
    target_field: str,
    target_defaults: Union[dict, List[dict]] = None,
) -> PipelineResult:
    """Checks if the passed data contains new Targets and creates them if necessary.
    Defaults for the new Targets can be passed via target_defaults.

    Args:
        request_result (RequestResult): RequestResult containing the data to
            check for new Targets.
        crawl_id (str): Specifies which crawl produced the data.
        target_field (str): A key to a field in the RequestResult.data segment that
            holds the kwargs for potential new Targets. This field must be a list of
            dicts, each containing the values for the kwargs field of the Targets
            to be created.
        target_defaults (Union[dict, List[dict]]): Default parameters for new
            Targets. If a list is passed, a Target will be created for
            each dict in the list. If a dict is passed, a single target will
            be created.

    Returns:
        PipelineResult: Number of new targets found and targets checked.
    """
    # Deserialize request_result if serialised
    if not isinstance(request_result, RequestResult):
        request_result = RequestResult(**request_result)

    # Get crawl from database
    crawl: Crawl = Crawl.objects(id=crawl_id).get()

    # If no target_defaults are passed, use an empty dict, so we can still loop
    # over it later
    if target_defaults is None:
        target_defaults = {}
    # If target_defaults is a dict, wrap it in a list so we can use the same code
    # for both cases
    if isinstance(target_defaults, dict):
        target_defaults = [target_defaults]

    # Wrap data into temporary list if its not already a batch
    if not request_result.batch:
        _result = [request_result.result]
    else:
        _result = request_result.result

    # Combine lists with possible targets found by the crawler as each document
    # might have found different targets
    targets_kwargs = []
    for raw_document in _result:
        if isinstance(raw_document.get(target_field, None), list):
            targets_kwargs += raw_document[target_field]

    # Remove duplicate targets_kwargs, by converting to a set of tuples and back
    targets_kwargs = [dict(t) for t in {tuple(d.items()) for d in targets_kwargs}]

    # Create new Targets based on the target_defaults and the target_kwargs
    targets = []
    for target_kwargs, target_default in itertools.product(
        targets_kwargs, target_defaults
    ):
        # Merge target_kwargs and target_default into a single dict
        merged_target = target_default.copy()
        merged_target["kwargs"] = (
            merged_target["kwargs"] if "kwargs" in merged_target else {}
        )
        merged_target["kwargs"].update(target_kwargs)

        # Create target only if it doesn't exist yet
        if not Target.objects(kwargs=merged_target["kwargs"]):
            targets.append(Target(discovered_by=crawl, **merged_target))

    # Insert new Targets into database if any were found
    if targets:
        try:
            Target.objects.insert(targets)
        except BulkWriteError as e:
            pass

    # Return the amount of new Targets and Targets checked
    return PipelineResult(
        statistics={
            "new_targets": len(targets),
            "checked_targets": len(targets_kwargs) * len(target_defaults),
        }
    )
