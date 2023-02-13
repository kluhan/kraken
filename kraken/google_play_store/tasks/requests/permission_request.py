from google_play_scraper import permissions as permission_scraper
from google_play_scraper.exceptions import NotFoundError

from kraken.core.spider import RequestResult

from kraken.google_play_store.documents import DocumentType
from kraken.celery_app import app


@app.task(
    name="kraken.google_play_store.request.permission",
    autoretry_for=(Exception,),
    max_retries=3,
    retry_backoff=5,
    retry_jitter=True,
    serializer="orjson",
    rate_limit="10/s",
)
def request_permissions(
    app_id: str,
    lang: str,
) -> RequestResult:
    """Retrieves the permissions for a specified app from the Google Play Store
    in the specified language using the :func:`google-play-scraper.permissions`
    function.

    .. note::
        As of 2021, permissions can no longer be accessed through the Google
        Play Store website, but only through the Google Play Store API. As a
        result, this feature may be considered deprecated.

    If the app is not found, the :attr:`target_not_found` attribute of the
    returned :class:`RequestResult` will be set to ``True``.

    Default values for the :class:`celery.Task` are set as follows:

    | Setting         | Value                                    | Documentation
    | --------------- | ---------------------------------------- | ----------------------------------------------------------------------------------------------
    | name            | kraken.google_play_store.request.permission    | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#Task.name>`_
    | autoretry_for   | Exception                                | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#automatic-retry-for-known-exceptions>`_
    | max_retries     | 3                                        | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#id0>`_
    | retry_backoff   | 5                                        | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#Task.retry_backoff>`_
    | retry_jitter    | True                                     | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#Task.retry_jitter>`_
    | serializer      | "orjson"                                 | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#Task.serializer>`_
    | rate_limit      | "10/s"                                   | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#Task.rate_limit>`_


    Parameters
    ----------
    app_id : str
        The unique ID of the app in the Google Play Store.
    lang : str
        The language in which the data should be retrieved.


    Returns
    -------
    RequestResult
        The retrieved permissions. If the app is not found, the :attr:`target_not_found`
        attribute of the returned :class:`RequestResult` will be set to ``True``.
    """

    # Try to load data via google-play-scraper library.
    try:
        response = permission_scraper(app_id, lang=lang)
        # inject language into response
        response["lang"] = lang
        # inject app_id into response
        response["app_id"] = app_id
        # inject DocumentType so it can later be parsed
        response["document_type"] = DocumentType.PERMISSION

    # due to an bug, the NotFoundError is not triggered, instead a
    # TypeError is issued later on
    except (NotFoundError, TypeError):
        return RequestResult(result=None, target_not_found=True, gain=0)

    return RequestResult(result=response)
