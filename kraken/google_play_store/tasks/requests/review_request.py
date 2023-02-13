from math import ceil
from datetime import datetime

from google_play_scraper import reviews as app_reviews
from google_play_scraper.exceptions import NotFoundError
from google_play_scraper.features.reviews import _ContinuationToken
from google_play_scraper.constants.google_play import Sort

from kraken.core.spider import RequestResult

from kraken.google_play_store.documents import DocumentType
from kraken.celery_app import app

RESULTS_PER_REQUEST = 200


@app.task(
    name="kraken.google_play_store.request.reviews",
    autoretry_for=(Exception,),
    max_retries=3,
    retry_backoff=5,
    retry_jitter=True,
    serializer="orjson",
)
def request_reviews(
    app_id: str,
    lang: str = "en",
    continuation_token: dict = None,
    count=200,
    sort: Sort = Sort.NEWEST,
) -> RequestResult:
    """Retrieves reviews for a specified app from the Google Play Store
    in the specified language using the  :func:`google-play-scraper.reviews`.
    function.

    If the app is not found, the :attr:`target_not_found` attribute of the
    returned :class:`RequestResult` will be set to ``True``.

    Default values for the :class:`celery.Task` are set as follows:

    | Setting         | Value                                    | Documentation
    | --------------- | ---------------------------------------- | ----------------------------------------------------------------------------------------------
    | name            | kraken.google_play_store.request.reviews       | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#Task.name>`_
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
        The language for which the reviews should be retrieved.
    continuation_token : dict
        The continuation token to use for the request. If not supplied, the
        reviews will be retrieved from the beginning.
    count : int
        The number of reviews to request. Defaults to 200.
    sort : Sort
        The sort order in which the reviews will be requested. Defaults to
        :attr:`google_play_scraper.constants.google_play.Sort.NEWEST`.

    Returns
    -------
    RequestResult
        The requested reviews. If the app is not found, the :attr:`target_not_found`
        attribute of the returned :class:`RequestResult` will be set to ``True``.
        If reviews are remaining, the :attr:`subsequent_kwargs` attribute of the
        returned :class:`RequestResult` will be set to a dictionary containing
        the continuation token to use for the next request.

    """

    # Build _ContinuationToken if supplied
    if continuation_token is not None:
        continuation_token = _ContinuationToken(**continuation_token)

    # Try to load data via google-play-scraper library.
    try:
        response, _continuation_token = app_reviews(
            app_id,
            lang=lang,
            count=count,
            continuation_token=continuation_token,
            sort=sort,
        )

        for review in response:
            # inject language into response
            review["lang"] = lang
            # inject app_id into response
            review["app_id"] = app_id
            # inject DocumentType so it can later be parsed
            review["document_type"] = DocumentType.REVIEW
            # parse datetime into timestamp to bring it inline with details
            if review.get("at", None) is not None:
                review["at"] = int(datetime.timestamp(review["at"]))
            if review.get("repliedAt", None) is not None:
                review["repliedAt"] = int(datetime.timestamp(review["repliedAt"]))

    # Due to a limitation of the google-play-store-api, the NotFoundError
    # is not triggered by the underlying library if a non-existent app_id
    # is supplied.
    except NotFoundError:
        return RequestResult(result=None, target_not_found=True, gain=0)

    # Serialize _continuation_token if returned and build subsequent kwargs
    if _continuation_token.token is None:
        subsequent_kwargs = None
    else:
        subsequent_kwargs = dict(
            # Cant use ** since _continuation_token uses __slots__
            continuation_token=dict(
                token=_continuation_token.token,
                lang=_continuation_token.lang,
                country=_continuation_token.country,
                sort=_continuation_token.sort,
                count=_continuation_token.count,
                filter_score_with=_continuation_token.filter_score_with,
            )
        )

    return RequestResult(
        result=response,
        subsequent_kwargs=subsequent_kwargs,
        batch=True,
        gain=len(response),
        cost=max(ceil(len(response) / RESULTS_PER_REQUEST), 1),
    )
