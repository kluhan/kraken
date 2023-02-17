from google_play_scraper import app as app_scraper
from google_play_scraper import developer as app_developer
from google_play_scraper import collection as app_collection
from google_play_scraper.exceptions import NotFoundError
from google_play_scraper.constants.google_play import PageType

from kraken.core.spider import RequestResult
from kraken.core.types import SlimTarget

from kraken.google_play_store.documents import DocumentType
from kraken.celery_app import app


def __load_collection(
    collection_type: PageType,
    token: str,
    lang: str = "en",
):
    """
    Loads the apps of a collection or developer page and returns them. Uses
    :func:`google_play_scraper.collection` or :func:`google_play_scraper.developer`.

    Parameters
    ----------
    collection_type : dict
        The type of the collection to load.
    token : str
        Token of the collection or developer page.
    lang : str
        The language to use for the request.
        Defaults to "en".

    Raises
    ------
    ValueError
        If  :attr:`collection_type` is not of type :class:`PageType`.

    Returns
    -------
    dict[str, Any]
        Data of the collection or developer page.
    """

    # Chose correct function for accessing the developer
    match collection_type:
        # Use app_developer if type equals DEVELOPER
        case PageType.DEVELOPER:
            collection = app_developer(token, lang=lang)

        # Use app_collection if type equals COLLECTION
        case PageType.COLLECTION:
            collection = app_collection(token, lang=lang)

        # Raise ValueError if type does match
        case _:
            raise ValueError("type must be of type PageType")

    return collection


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


    Examples
    --------
    Typically, this function is not called directly by the user, but via the
    :func:`kraken.core.tasks.multi_stage_crawler` task. However, if
    it is called directly, it is used as follows:

    ```python
    from kraken.google_play_store.tasks.requests import request_details
    # The celery_app must be imported before the task is called, otherwise
    # celery cannot connect to the broker.
    from kraken import celery_app

    # Retrieve data safety information via a worker.
    result = request_details.apply_async(app_id="com.google.android.youtube", lang="en")

    # Retrieve data safety information directly.
    result = request_details(app_id="com.google.android.youtube", lang="en")
    ```

    The result of the task is a :class:`RequestResult` object, which contains
    the retrieved data safety information and looks as follows:

    ```python
    >>> print(result)
    RequestResult(
        result={
            'title': 'YouTube',
            'description': 'Get the official YouTube app on Android phones and tablets. See what the world is watching...',
            'summary': 'Enjoy your favorite videos and channels with the official YouTube app.',
            'installs': '10,000,000,000+',
            'minInstalls': 10000000000,
            'realInstalls': 13442950489,
            'score': 4.184395,
            'ratings': 146957386,
            'reviews': 2821602,
            'histogram': [19425609, 4684141, 7350970, 13402323, 102094288],
            'price': 0,
            'free': True,
            'appId': 'com.google.android.youtube',
            'url': 'https://play.google.com/store/apps/details?id=com.google.android.youtube&hl=en&gl=us',
            'lang': 'en',
            'document_type': <DocumentType.DETAIL: 'DETAIL'>,
            'moreByDeveloper': [
                'com.google.android.apps.youtube.unplugged',
                'com.google.android.youtube.tvunplugged',
                ..
            ],
            'similarApps': [
                'com.android.chrome',
                'com.zhiliaoapp.musically',
                ...
            ],
            ...
        },
        subsequent_kwargs=None,
        batch=False,
        gain=1,
        cost=3,
        target_not_found=False,
        target_exhausted=None
        adjacent_targets=[
            SlimTarget(kwargs={"app_id": "com.google.android.youtube", "lang": "en"}),
            SlimTarget(kwargs={"app_id": "com.google.android.youtube.tvunplugged", "lang": "en"}),
            ...
            SlimTarget(kwargs={"app_id": "com.android.chrome", "lang": "en"}),
            ...
        ]
    )
    ```
    For further information on the returned data, see the documentation of the
    :func:`google-play-scraper.app` function.

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
    :class:`kraken.core.types.RequestResult`
        The requested data safety information, as well as additional information
        about the request.
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
        response = __load_collection(
            collection_type=response["moreByDeveloperPage"]["type"],
            token=response["moreByDeveloperPage"]["token"],
        )

    # Try to load all similar apps
    if load_similar_apps and response.get("similarAppsPage", None) is not None:
        response = __load_collection(
            collection_type=response["similarAppsPage"]["type"],
            token=response["similarAppsPage"]["token"],
        )

    # Create list of adjacent targets
    adjacent_targets = [
        SlimTarget(kwargs={"app_id": __app_id, "lang": lang})
        for __app_id in set(
            response.get("similarApps", []) + response.get("moreByDeveloper", [])
        )
    ]

    return RequestResult(
        result=response,
        cost=(1 + load_similar_apps + load_more_apps_by_developer),
        adjacent_targets=adjacent_targets,
        target_exhausted=True,
    )
