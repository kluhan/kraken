from dataclasses import asdict

from celery import shared_task
from celery.canvas import signature, Signature
from dacite.core import from_dict
from dacite.config import Config

from kraken.core.types import (
    ExecutionToken,
    Stage,
)
from kraken.core.tasks import CrawlTask
from kraken.core import StageProcessor


@shared_task(
    bind=True,
    base=CrawlTask,
    name="kraken.crawler.single_stage",
    serializer="orjson",
)
def single_stage_crawler(
    self,
    stage: Stage,
    execution_token_id: str,
) -> Stage:
    """Processes each stage one after the other"""

    # If stages is serialized try to deserialize
    if not isinstance(stage, Stage):
        stage = Stage.from_dict(stage)

    stage_processor = StageProcessor(stage=stage)
    # Process stage and update progress/token
    for _ in stage_processor.process():

        self.update_state(
            state="PROGRESS",
            meta={"progress": stage},
        )

        # Load and update Execution Token
        token: ExecutionToken = ExecutionToken.objects.get(pk=execution_token_id)  # type: ignore
        token.progress = [asdict(x) for x in stage]
        token.save()

    return stage
