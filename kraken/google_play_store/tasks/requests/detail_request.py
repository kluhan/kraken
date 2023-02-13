from google_play_scraper import app as app_scraper
from google_play_scraper import developer as app_developer
from google_play_scraper import collection as app_collection
from google_play_scraper.exceptions import NotFoundError
from google_play_scraper.constants.google_play import PageType

from kraken.core.spider import RequestResult

from kraken.google_play_store.documents import DocumentType
from kraken.celery_app import app


def __expand_collection(
    response: dict, page_key: str, target_key: str, lang: str = "en"
):
    """
    Loads the apps of a collection or developer page and adds them to the response
    using :func:`google_play_scraper.collection` or :func:`google_play_scraper.developer`.

    Parameters
    ----------
    response : dict
        The response to expand.
    page_key : str
        The key of the page to expand.
    target_key : str
        The key to store the expanded page in.
    lang : str
        The language to use for the request.
        Defaults to "en".

    Raises
    ------
    ValueError
        If the type of the page to expand is not of type :class:`PageType`.

    Returns
    -------
    dict
        Modified :attr:`response` with the expanded page.
    """

    # Chose correct function for accessing the developer
    match response[page_key]["type"]:
        # Use app_developer if type equals DEVELOPER
        case PageType.DEVELOPER:
            similarApps = app_developer(response[page_key]["token"], lang=lang)

        # Use app_collection if type equals COLLECTION
        case PageType.COLLECTION:
            similarApps = app_collection(response[page_key]["token"], lang=lang)

        # Raise ValueError if type does match
        case _:
            raise ValueError("page_key.type must be of type PageType")

    response[target_key] = similarApps["apps"]
    response.pop(page_key)

    return response


@app.task(
    name="kraken.google_play_store.request.detail",
    autoretry_for=(Exception,),
    max_retries=3,
    retry_backoff=5,
    retry_jitter=True,
    serializer="orjson",
    rate_limit="10/s",
)
def request_details(
    app_id: str,
    lang: str,
    load_similar_apps: bool = True,
    load_more_apps_by_developer: bool = True,
) -> RequestResult:
    """This function retrieves details for a specified app (given by the :attr:`app_id` parameter)
    from the Google Play Store in the specified language (given by the :attr:`lang` parameter)
    using the :func:`google-play-scraper.app` function. If :attr:`load_similar_apps` or
    :attr:`load_more_apps_by_developer` is set to ``True``, additional requests
    to the Google Play Store will be made to retrieve the corresponding data.

    .. note::
        If :attr:`load_similar_apps` or :attr:`load_more_apps_by_developer` is set to ``True``,
        the function will make additional requests to the Google Play Store. Adjust the
        :attr:`rate_limit` accordingly.

    If the app is not found, the :attr:`target_not_found` attribute of the returned
    :class:`RequestResult` will be set to ``True``.

    Default values for the :class:`celery.Task` are set as follows:

    | Setting         | Value                                    | Documentation
    | --------------- | ---------------------------------------- | ----------------------------------------------------------------------------------------------
    | name            | kraken.google_play_store.request.detail        | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#Task.name>`_
    | autoretry_for   | Exception                                | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#automatic-retry-for-known-exceptions>`_
    | max_retries     | 3                                        | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#id0>`_
    | retry_backoff   | 5                                        | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#Task.retry_backoff>`_
    | retry_jitter    | True                                     | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#Task.retry_jitter>`_
    | serializer      | "orjson"                                 | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#Task.serializer>`_
    | rate_limit      | "10/s"                                   | `Link <https://docs.celeryq.dev/en/stable/userguide/tasks.html#Task.rate_limit>`_


    Parameters
    ----------
    app_id : str
        The app_id of the app for which the details should be downloaded.
    lang : str
        The language in which the details should be downloaded.
    load_similar_apps : bool, optional
        Specifies if all similar apps should be loaded via a additional request.
        Defaults to True.
    load_more_apps_by_developer : bool, optional
        Specifies if all apps by the developer should be loaded via a additional
        request. Defaults to True.

    Returns
    -------
    RequestResult
        The requested details. If the app is not found, the :attr:`target_not_found`
        attribute of the returned :class:`RequestResult` will be set to ``True``.
    """

    # Try to load details via google-play-scraper library.
    try:
        response = app_scraper(app_id, lang=lang)

        # inject language into response
        response["lang"] = lang
        # inject DocumentType so it can later be parsed
        response["document_type"] = DocumentType.DETAIL

    except NotFoundError:
        return RequestResult(result=None, target_not_found=True, gain=0)

    # Try to load all apps by developer
    if (
        load_more_apps_by_developer
        and response.get("moreByDeveloperPage", None) is not None
    ):
        response = __expand_collection(
            response=response,
            page_key="moreByDeveloperPage",
            target_key="moreByDeveloper",
        )

    # Try to load all similar apps
    if load_similar_apps and response.get("similarAppsPage", None) is not None:
        response = __expand_collection(
            response=response,
            page_key="similarAppsPage",
            target_key="similarApps",
        )

    # Create list of targets for kraken.pipeline.target_discovery

    response["potential_targets"] = [
        {"app_id": __app_id, "lang": lang}
        for __app_id in response.get("similarApps", [])
        + response.get("moreByDeveloper", [])
    ]

    return RequestResult(
        result=response, cost=(1 + load_similar_apps + load_more_apps_by_developer)
    )
