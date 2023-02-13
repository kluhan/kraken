from celery import shared_task
from dacite.core import from_dict
from dacite.config import Config

from kraken.core.types.stage import Stage
from kraken.core.tasks.pipelines import data_storage_pipeline


@shared_task(
    bind=True,
    name="kraken.terminator.static",
)
def static_terminator(
    self,
    stage: Stage,
    limit=1000,
) -> int:
    """Terminates stage after x processed documents"""

    # If stage is serialized try to deserialize
    if not isinstance(stage, Stage):
        stage = Stage.from_dict(stage)

    if (
        stage.progress.pipeline_results[data_storage_pipeline.name].statistics[
            "processed_documents"
        ]
        >= limit
    ):
        return 1
    else:
        return 0
