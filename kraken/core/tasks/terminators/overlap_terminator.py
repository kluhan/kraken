from celery import shared_task
from dacite.core import from_dict
from dacite.config import Config

from kraken.core.types.stage import Stage
from kraken.core.tasks.pipelines import data_storage_pipeline


@shared_task(
    bind=True,
    name="kraken.terminator.overlap",
)
def overlap_terminator(self, stage: Stage, overlap=100) -> None:
    """Terminates stage if a overlap is detected"""

    # If stage is serialized try to deserialize
    if not isinstance(stage, Stage):
        stage = Stage.from_dict(stage)

    new_documents = stage.progress.pipeline_results[
        data_storage_pipeline.name
    ].statistics["new_documents"]
    processed_documents = stage.progress.pipeline_results[
        data_storage_pipeline.name
    ].statistics["processed_documents"]

    if (processed_documents - new_documents) >= overlap:
        return 1
    else:
        return 0
