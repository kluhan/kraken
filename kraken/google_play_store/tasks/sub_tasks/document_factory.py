from typing import Union

from kraken.google_play_store.documents import (
    Detail,
    Permission,
    Review,
    DataSafety,
    DocumentType,
)
from kraken.celery_app import app


@app.task(
    name="kraken.google_play_store.sub_task.document_factory",
    autoretry_for=(Exception,),
    max_retries=3,
)
def document_factory(document: dict) -> Union[Detail, Permission, Review, DataSafety]:
    """Creates a :class:`Detail`, :class:`Permission`, :class:`Review` and
    :class:`DataSafety` object from a :class:`dict`. The type of the
    document created is determined by ``document["document_type"]``, which must
    be of Type :class:`DocumentType`.

    .. note::
        This function is designed to be used as the :attr:`factory_task` for the
        :func:`kraken.core.tasks.pipelines.data_storage_pipeline.data_storage_pipeline`.

    Default values for the :class:`celery.Task` are set as follows:

    | Setting         | Value                                    | Documentation
    | --------------- | ---------------------------------------- | ----------------------------------------------------------------------------------------------
    | name            | kraken.google_play_store.sub_task.document_factory   | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#Task.name>`_
    | autoretry_for   | Exception                                | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#automatic-retry-for-known-exceptions>`_
    | max_retries     | 3                                        | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#id0>`_


    Parameters
    ----------
    document : dict
        The dict to create the document from.

    Returns
    -------
    Union[Detail, Permission, Review, DataSafety]
        The created document.

    Raises
    ------
    AttributeError
        If the :attr:`document` does not contain the ``document_type`` key.
    TypeError
        If the :attr:`document` does not contain a valid ``document_type``.
    """
    match document["document_type"]:
        case DocumentType.DETAIL:
            return Detail.from_response(document)

        case DocumentType.PERMISSION:
            return Permission.from_response(document)

        case DocumentType.REVIEW:
            return Review.from_response(document)

        case DocumentType.DATA_SAFETY:
            return DataSafety.from_response(document)

        case _:
            raise TypeError()
