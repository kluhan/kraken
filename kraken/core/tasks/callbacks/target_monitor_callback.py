import dataclasses
from datetime import datetime

from celery import shared_task
from celery.canvas import signature, Signature
from dacite.core import from_dict
from dacite.config import Config

from kraken.core.types import (
    Crawl,
    Stage,
    Target,
)
from kraken.core.tasks import DatabaseTask
from kraken.utils import mongodb_key_sanitizer as sanitizer


# TODO: Rewrite documentation
@shared_task(
    bind=True,
    base=DatabaseTask,
    name="kraken.callback.target_monitor",
)
def target_monitor_callback(self, stage: Stage, crawl_id: str, final_stage: bool) -> None:
    """Adds the progress of a stage to the corresponding target of the stage.
    PipelineResults fields which are prefixed with '__' are ignored.

    Args:
        stage (Stage): Stage for which the progress should be added to the target.
        crawl_id (str): ID of the crawl to which the stage belongs.

    Returns:
        None
    """
    # timestamp used for the update of the target
    timestamp = datetime.now()

    # If stage is serialized try to deserialize
    if not isinstance(stage, Stage):
        stage = from_dict(
            data_class=Stage,
            data=stage,
            config=Config(type_hooks={Signature: signature}),
        )

    # Get the parent crawl
    crawl: Crawl = Crawl.objects(pk=crawl_id).get()

    # Dictionary to collect all queries to update the target
    update = {}

    # Path to statistics object
    base_path = sanitizer(f"statistics__{crawl.series.pk}__{stage.name}")

    # Queries to set cost and gain and push corresponding entries to the history
    update["set__" + base_path + "__cost"] = stage.progress.cost
    update["set__" + base_path + "__gain"] = stage.progress.gain
    update["push__" + base_path + "__cost_history"] = {
        "value": stage.progress.cost,
        "timestamp": timestamp,
    }
    update["push__" + base_path + "__gain_history"] = {
        "value": stage.progress.gain,
        "timestamp": timestamp,
    }

    # Look for weight and collect all metrics in temporary dict
    _metrics = {}
    for _, pipeline_result in stage.progress.pipeline_results.items():
        if pipeline_result.weight is not None:
            # Queries to set weight and push the corresponding entry to the history
            update["set__" + base_path + "__weight"] = pipeline_result.weight
            update["push__" + base_path + "__weight_history"] = {
                "value": pipeline_result.weight,
                "timestamp": timestamp,
            }
        # As we do not know which metric is produced by which pipeline we collect
        # all metrics in a temporary dict to handle them later.
        # TODO: Add Warning if a single metric is produced by multiple pipelines
        _metrics.update(pipeline_result.metrics)

    # Queries to set metrics and push corresponding entries to the history
    for metric_name, metric_value in _metrics.items():
        update["set__" + base_path + "__metrics__" + metric_name] = metric_value
        update["push__" + base_path + "__metrics_history__" + metric_name] = {
            "value": metric_value,
            "timestamp": timestamp,
        }

    # Query to set pipeline-results and push corresponding entries to the history
    update["set__" + base_path + "__result"] = dataclasses.asdict(stage.progress)
    update["push__" + base_path + "__result_history"] = {
        "value": dataclasses.asdict(stage.progress),
        "timestamp": timestamp,
    }

    # Query to add timestamp to processed-array
    update[f"push__processed__{crawl.series.id}"] = timestamp

    # Dispatch all collected updates
    Target.objects(id=stage.target.id).update(**update)
