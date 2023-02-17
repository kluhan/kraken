from typing import List
from dataclasses import asdict

from celery import shared_task

from kraken.core.types import (
    ExecutionToken,
    Stage,
)

from kraken.core.tasks import CrawlTask
from kraken.core import StageProcessor


@shared_task(
    bind=True,
    base=CrawlTask,
    name="kraken.crawler.multi_stage",
    serializer="orjson",
)
def multi_stage_crawler(
    self,
    stages: List[Stage],
    execution_token_id: str,
    crawl_id: str,
) -> List[Stage]:
    """Processes each stage one after the other"""

    # If stages is serialized try to deserialize
    if not isinstance(stages[0], Stage):
        stages = [Stage.from_dict(stage) for stage in stages]

    # Process all stages in sequence
    for i, stage in enumerate(stages):
        # Check if this is the last stage
        final_stage = True if i == len(stages) - 1 else False

        stage_processor = StageProcessor(stage=stage, crawl_id=crawl_id, final_stage=final_stage)
        # Process stage and update progress/token
        for _ in stage_processor.process():

            self.update_state(
                state="PROGRESS",
                meta={"progress": stages},
            )

            # Load and update Execution Token
            token: ExecutionToken = ExecutionToken.objects.get(pk=execution_token_id)  # type: ignore
            token.progress = [asdict(x) for x in stages]
            token.save()

    return stages
