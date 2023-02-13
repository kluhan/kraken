from celery import shared_task, signature
from celery.canvas import Signature
from celery.result import allow_join_result

from kraken.core.types import (
    Crawl,
    HistoricDocument,
    RequestResult,
    PipelineResult,
)

from kraken.core.tasks import DatabaseTask
from kraken.utils.pipeline import combine_dicts_by_addition


# TODO: Add option to disable metrics, weights and statistics
@shared_task(
    bind=True,
    base=DatabaseTask,
    name="kraken.pipeline.data_storage",
    autoretry_for=(Exception,),
    max_retries=3,
)
def data_storage_pipeline(
    self,
    request_result: RequestResult,
    crawl_id: str,
    factory_task: Signature,
) -> PipelineResult:
    """Stores the passed data in the database.

    Args:
        request_result (RequestResult): RequestResult containing the data to store.
        crawl_id (str): Specifies which crawl produced the data.

    Returns:
        PipelineResult: Total number of new documents, updated documents and changes
    """
    # If request_result is serialized try to deserialize
    if not isinstance(request_result, RequestResult):
        request_result = RequestResult(**request_result)

    # If factory_task is serialized try to deserialize
    if not isinstance(factory_task, Signature):
        factory_task = signature(
            factory_task["task"],
            args=factory_task["args"],
            kwargs=factory_task["kwargs"],
        )

    # Get crawl from database
    crawl: Crawl = Crawl.objects(id=crawl_id).get()

    # Amount of documents seen for the first time
    new_documents = 0
    # Amount of documents which received a minimum of one update
    updated_documents = 0
    # Total amount of updates for all documents
    total_changes = 0
    # Magnitude of changes according to the models
    metrics = {}
    # Total weight of the documents
    weight = 0

    # Wrap data into temporary list if its not already a batch
    if not request_result.batch:
        _result = [request_result.result]
    else:
        _result = request_result.result

    # Save all documents
    for raw_document in _result:
        # Build documents form raw response using the passed factory
        with allow_join_result():
            document: HistoricDocument = (
                factory_task.clone().apply(kwargs={"document": raw_document}).get()
            )

        # Pass crawl to custom save method
        new, changes, _metrics = document.save(crawl=crawl)

        # Aggregate basic statistics
        new_documents += 1 if new else 0
        updated_documents += 1 if changes else 0
        total_changes += changes
        metrics = combine_dicts_by_addition(metrics, _metrics)
        weight += document.weight()

    return PipelineResult(
        statistics={
            "new_documents": new_documents,
            "updated_documents": updated_documents,
            "processed_documents": len(_result),
            "total_changes": total_changes,
        },
        metrics=metrics,
        weight=weight,
    )
