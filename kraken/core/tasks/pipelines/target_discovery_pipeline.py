from dataclasses import asdict
import itertools
from typing import List

from celery import shared_task

from kraken.core.types import (
    RequestResult,
    PipelineResult,
    Target,
    SlimTarget,
    Crawl,
)
from kraken.core.tasks import DatabaseTask
from mongoengine import BulkWriteError


@shared_task(
    bind=True,
    base=DatabaseTask,
    name="kraken.pipeline.target_discovery",
)
def target_discovery_pipeline(
    self,
    request_result: RequestResult,
    crawl_id: str,
    target_defaults: List[SlimTarget] = None,
) -> PipelineResult:
    """
    Inserts all new adjacent Targets into the database. If :attr:`target_defaults`
    are passed, a new Target will be created for each combination of adjacent Target
    and provided default. Only new Targets will be inserted into the database.

    Parameters
    ----------
    request_result : :class:`kraken.core.types.RequestResult`
        :class:`RequestResult` containing the adjacent Targets to insert.
    crawl_id : str
        Specifies which :class:`kraken.core.types.Crawl` produced the data. Is used
        to set the :attr:`discovered_by` attribute of the new Targets.
    target_defaults : List[:class:`kraken.core.types.SlimTarget`]
        Default parameters for new :class:`kraken.core.types.Target`.


    Returns
    -------
    :class:`kraken.core.types.PipelineResult`
        Number of new targets found and targets checked. The number of
        new targets found is only an estimate, as it might happen that two
        target_discovery_pipeline tasks insert the same Target at the same time.
        In this case, both tasks will report that they inserted a new Target, even
        though only one was actually inserted.
    """

    # Deserialize request_result if serialised
    if not isinstance(request_result, RequestResult):
        request_result = RequestResult.from_dict(request_result)

    # Deserialize target_defaults if serialised
    if isinstance(target_defaults[0], SlimTarget):
        target_defaults = [SlimTarget.from_dict(x) for x in target_defaults]
    elif target_defaults is None:
        target_defaults = []

    # Get crawl from database
    crawl: Crawl = Crawl.objects(id=crawl_id).get()

    # Remove duplicates by converting to a set of tuples and back
    adjacent_targets = [set(request_result.adjacent_targets)]

    # Create new Targets based on the target_defaults and the target_kwargs
    targets = []
    for adjacent_target, target_default in itertools.product(
        adjacent_targets, target_defaults
    ):
        # Merge target_default and adjacent_target
        _target = SlimTarget.merge(target_default, adjacent_target)

        # Create target only if it doesn't exist yet
        if not Target.objects(kwargs=_target["kwargs"]):
            targets.append(Target(discovered_by=crawl, **asdict(_target)))

    # Insert new Targets into database if any were found
    if targets:
        try:
            Target.objects.insert(targets)
        except BulkWriteError:
            # A BulkWriteError can occur if the same Target is created by a other
            # target_discovery_pipeline task at the same time. If this happens,
            # we save the Targets one by one. This is not ideal, but it should
            # not happen often, so the performance impact should be minimal.
            for target in targets:
                target.save(
                    force_insert=True
                )  # insert only if it doesn't exist yet and dont update existing targets

    # Return the amount of new Targets and Targets checked
    return PipelineResult(
        statistics={
            "new_targets": len(targets),
            "checked_targets": len(adjacent_targets) * len(target_defaults),
        }
    )
