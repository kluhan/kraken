from celery import shared_task
from dacite.core import from_dict
from dacite.config import Config

from kraken.core.types import Stage
from kraken.core.tasks.pipelines import data_storage_pipeline


# TODO: WIP, Add doc and dependency with data_storage
@shared_task(
    bind=True,
    name="kraken.terminator.budget",
)
def budget_terminator(
    self,
    stage: Stage,
    budget=100,
    budget_inc=10,
    budget_dec=1,
    model="bfm",
) -> None:
    """Terminates stage if a overlap is detected"""

    # If stage is serialized try to deserialize
    if not isinstance(stage, Stage):
        stage = Stage.from_dict(stage)

    acquired_budget = (
        budget
        + stage.progress.pipeline_results[data_storage_pipeline.name].statistics[model]
        * budget_inc
    )
    spent_budget = (
        stage.progress.pipeline_results[data_storage_pipeline.name].statistics[
            "processed_documents"
        ]
        * budget_dec
    )

    if spent_budget > acquired_budget:
        return 1
    else:
        return 0
