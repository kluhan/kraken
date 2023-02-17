from google_play_scraper import data_safety as data_safety_scraper
from google_play_scraper.exceptions import NotFoundError

from kraken.core.spider import RequestResult

from kraken.google_play_store.documents.base.document_type import DocumentType
from kraken.celery_app import app


@app.task(
    name="kraken.google_play_store.request.data_safety",
    autoretry_for=(Exception,),
    max_retries=3,
    retry_backoff=5,
    retry_jitter=True,
    serializer="orjson",
    rate_limit="10/s",
)
def request_data_safety(
    app_id: str,
    lang: str,
) -> RequestResult:
    """
    Retrieves data safety information for a specified app from the Google Play Store
    in the specified language using the  :func:`google-play-scraper.data_safety`.
    function.

    If the app is not found, the :attr:`target_not_found` attribute of the
    returned :class:`RequestResult` will be set to ``True``.

    Default values for the :class:`celery.Task` are set as follows:

    | Setting         | Value                                    | Documentation
    | --------------- | ---------------------------------------- | ----------------------------------------------------------------------------------------------
    | name            | kraken.google_play_store.request.data_safety   | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#Task.name>`_
    | autoretry_for   | Exception                                | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#automatic-retry-for-known-exceptions>`_
    | max_retries     | 3                                        | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#id0>`_
    | retry_backoff   | 5                                        | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#Task.retry_backoff>`_
    | retry_jitter    | True                                     | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#Task.retry_jitter>`_
    | serializer      | "orjson"                                 | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#Task.serializer>`_
    | rate_limit      | "10/s"                                   | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#Task.rate_limit>`_

    Examples
    --------
    Typically, this function is not called directly by the user, but via the
    :func:`kraken.core.tasks.multi_stage_crawler` task. However, if
    it is called directly, it is used as follows:

    ```python
    from kraken.google_play_store.tasks.requests import request_data_safety
    # The celery_app must be imported before the task is called, otherwise
    # celery cannot connect to the broker.
    from kraken import celery_app

    # Retrieve data safety information via a worker.
    result = request_data_safety.apply_async(app_id="com.google.android.youtube", lang="en")

    # Retrieve data safety information directly.
    result = request_data_safety(app_id="com.google.android.youtube", lang="en")
    ```

    The result of the task is a :class:`RequestResult` object, which contains
    the retrieved data safety information and looks as follows:

    ```python
    >>> print(result)
    RequestResult(
        result={
            'dataCollected': {
                'Location': [
                    {
                        'name': 'Approximate location',
                        'optional': False,
                        'usage': 'App functionality, Analytics, Developer communications, Advertising or marketing, Fraud prevention, security, and compliance, Personalization, Account management'
                    },
                    {
                        'name': 'Precise location',
                        'optional': True,
                        'usage': 'App functionality, Analytics, Personalization'
                    }
                ],
                'Personal info': [
                    ...
                ],
                ...
            },
            'dataShared': {},
            'securityPractices': [
                {
                    'name': 'Data is encrypted in transit',
                    'description': 'Your data is transferred over a secure connection'
                },
                ...
            ],
            'appId': 'com.google.android.youtube',
            'url': 'https://play.google.com/store/apps/datasafety?id=com.google.android.youtube&hl=en&gl=us',
            'lang': 'en',
            'app_id': 'com.google.android.youtube',
            'document_type': <DocumentType.DATA_SAFETY: 'DATA_SAFETY'>
        },
        subsequent_kwargs=None,
        batch=False,
        gain=1,
        cost=1,
        target_not_found=False,
        target_exhausted=None
    )
    ```
    For further information on the returned data, see the documentation of the
    :func:`google-play-scraper.data_safety` function.


    Parameters
    ----------
    app_id : str
        The unique ID of the app in the Google Play Store.
    lang : str
        The language in which the data should be retrieved.

    Returns
    -------
    RequestResult
        The retrieved data safety informations. If the app is not found, the :attr:`target_not_found`
        attribute of the returned :class:`RequestResult` will be set to ``True``.
    """

    # Try to load data via google-play-scraper library.
    try:
        response = data_safety_scraper(app_id, lang=lang)
        # inject language into response
        response["lang"] = lang
        # inject app_id into response
        response["app_id"] = app_id
        # inject DocumentType so it can later be parsed
        response["document_type"] = DocumentType.DATA_SAFETY

    except NotFoundError:
        return RequestResult(result=None, target_not_found=True, gain=0)

    return RequestResult(result=response, target_exhausted=True)
