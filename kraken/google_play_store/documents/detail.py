from datetime import datetime

from mongoengine.fields import (
    BooleanField,
    FloatField,
    ListField,
    StringField,
    IntField,
    DateTimeField,
)

from kraken.core.types import HistoricDocument
from kraken.utils import escape, hacky_datetime_parser

ICON_PREFIX = "https://play-lh.googleusercontent.com"
HEADER_IMAGE_PREFIX = "https://play-lh.googleusercontent.com"
VIDEO_IMAGE_PREFIX = "https://play-lh.googleusercontent.com"
SCREENSHOT_PREFIX = "https://play-lh.googleusercontent.com"


class Detail(HistoricDocument):
    """
    A class to represent a Google Play Store app detail page. It inherits from
    :class:`kraken.core.types.HistoricDocument` which in turn inherits from
    :class:`mongoengine.Document`, allowing it to be stored in a MongoDB database using
    a backwards delta encoding.
    """

    id: str = StringField(primary_key=True)
    """The ID used to store the document in the database. Per default this is a combination of :attr:`app_id` and :attr:`lang`."""
    app_id: str = StringField(unique_with="lang")
    """The ID of the app as used in the Google Play Store. Has a unique constraint with :attr:`lang`."""
    lang: str = StringField(unique_with="app_id")
    """The language of the app detail page. Has a unique constraint with :attr:`app_id`."""
    title: str = StringField()
    """The title of the app."""
    description: str = StringField()
    """The description of the app."""
    summary: str = StringField()
    """The summary of the app."""
    installs: str = StringField()
    """The magnitude of the installs of the app."""
    real_installs: int = IntField()
    """The number of installs of the app."""
    score: float = FloatField()
    """The score of the app."""
    ratings: int = IntField()
    """The number of ratings of the app."""
    reviews: int = IntField()
    """The number of reviews of the app."""
    histogram: list[int] = ListField()
    """The histogram of the ratings of the app."""
    price: int = IntField()
    """The price of the app."""
    free: bool = BooleanField()
    """Whether the app is free or not."""
    currency: str = StringField()
    """The currency an app is offered in."""
    sale: bool = BooleanField()
    """Whether the app is on sale or not."""
    sale_time: datetime = DateTimeField()
    """The time the app went on sale."""
    offers_iap: bool = BooleanField()
    """Whether the app offers in-app purchases or not."""
    in_app_product_price: str = StringField()
    """The price of the offered in-app purchases."""
    size: str = StringField()
    """The size of the app."""
    android_version: str = StringField()
    """The minimum Android version required to run the app."""
    android_version_text: str = StringField()
    """The minimum Android version required to run the app as a string."""
    developer_internal_id: str = StringField()
    """The Google Play Store internal ID of the developer of the app."""
    developer: str = StringField()
    """The name of the developer of the app."""
    developer_id: str = StringField()
    """The Google Play Store ID of the developer of the app."""
    developer_email: str = StringField()
    """The email address of the developer of the app."""
    developer_website: str = StringField()
    """The website of the developer of the app."""
    developer_address: str = StringField()
    """The address of the developer of the app."""
    privacy_policy: str = StringField()
    """The URL to the privacy policy of the app."""
    genre: str = StringField()
    """The genre of the app."""
    genre_id: str = StringField()
    """The Google Play Store ID of the genre of the app."""
    icon: str = StringField()
    """The URL to the icon of the app."""
    header_image: str = StringField()
    """The URL to the header image of the app."""
    screenshots: list[str] = ListField()
    """A list of URLs to screenshots of the app."""
    video: str = StringField()
    """The URL to a video of the app."""
    video_image: str = StringField()
    """The URL to the image of the video of the app."""
    content_rating: str = StringField()
    """The content rating of the app."""
    content_rating_description: str = StringField()
    """The description of the content rating of the app."""
    ad_supported: bool = BooleanField()
    """Whether the has ad support or not."""
    contains_ads: bool = BooleanField()
    """Whether the app contains ads or not."""
    released: datetime = StringField()
    """The release date of the app."""
    updated: datetime = DateTimeField()
    """The last update date of the app."""
    version: str = StringField()
    """The version of the app."""
    recent_changes: str = StringField()
    """The recent changes of the app."""
    similar_apps: list[str] = ListField()
    """A list of similar apps offered in the Google Play Store."""
    more_by_developer: list[str] = ListField()
    """A list of other apps by the same developer offered in the Google Play Store."""
    other_languages: list[str] = ListField()
    """A list of other languages the detail page is available in."""
    data_safety_short: list[dict[str, str]] = ListField()
    """A list of data safety informations of the app in a short version."""

    meta = {"collection": "gpc_detail", "indexes": [("app_id", "lang")]}

    def __init__(
        self,
        app_id: str,
        lang: str,
        id: str = None,
        compress: bool = False,
        *args,
        **kwargs
    ):
        """Constructor for a :class:`Detail` object. :attr:`app_id`, :attr:`lang` are required.
        All other fields are optional and can be set using keyword arguments. For a list of
        available fields, see :class:`Detail`.


        Parameters
        ----------
        app_id: str
            The Google Play Store ID of the app.
        lang: str
            The language of the app detail page.
        id: str, optional
            The ID under which the object is stored in the database.
            Defaults to a combination of :attr:`app_id` and :attr:`lang`.
        compress: bool, optional
            Whether the object should be compressed or not. Defaults to ``False``.
            For more information on compression, see :meth:`compress`.
        """

        # Add id if not already present
        id = id if id is not None else app_id + ":" + lang
        # Call super-constructor
        super(Detail, self).__init__(app_id=app_id, lang=lang, id=id, *args, **kwargs)
        # Compress if necessary
        if compress:
            self.compress()

    def weight(self) -> int:
        """Returns the weight of the object, often used for monitoring purposes
        as well as for resource allocation. The weight is equal to :attr:`real_installs`.

        Returns
        -------
        int
            The weight of the object.

        Raises
        ------
        AttributeError
            If :attr:`real_installs` is not set.
        """
        return self.real_installs

    @staticmethod
    def wcf_weights():
        """Returns the weights of the individual fields for the WCF algorithm. Check
        :class:`kraken.core.types.historic_document.HistoricDocument` for more information.

        Returns
        -------
        The weights for relevant fields for the WCF model.
        """

        return {
            "title": 10,
            "description": 10,
            "summary": 10,
            "installs": 10,
            "score": 10,
            "ratings": 1,
            "reviews": 1,
            "price": 5,
            "free": 5,
            "currency": 1,
            "sale": 10,
            "offers_iap": 10,
            "size": 5,
            "developer_internal_id": 10,
            "privacy_policy": 5,
            "genre_id": 10,
            "content_rating": 10,
            "ad_supported": 10,
            "contains_ads": 10,
            "updated": 30,
            "version": 10,
            "recent_changes": 10,
            "data_safety_short": 10,
        }

    def compress(self) -> None:
        """
        Compresses the object to reduce the memory footprint of the object
        when it is stored. The prefix "https://play-lh.googleusercontent.com" is
        removed from the following fields: :attr:`icon`, :attr:`header_image`,
        :attr:`video_image`, :attr:`screenshots` and static content is removed
        from :attr:`data_safety_short`. All other fields are not affected.
        """

        # compress icon
        if isinstance(self.icon, str):
            self.icon = self.icon.removeprefix(ICON_PREFIX)
        # compress header_image
        if isinstance(self.header_image, str):
            self.header_image = self.header_image.removeprefix(HEADER_IMAGE_PREFIX)
        # compress video_image
        if isinstance(self.video_image, str):
            self.video_image = self.video_image.removeprefix(VIDEO_IMAGE_PREFIX)
        # compress screenshot-urls
        if all(isinstance(element, str) for element in self.screenshots):
            self.screenshots = [
                screenshot.removeprefix(SCREENSHOT_PREFIX)
                for screenshot in self.screenshots
            ]
        # compress data_safety_short by removing static content
        if isinstance(self.data_safety_short, list):
            for element in self.data_safety_short:
                if element["summary"] is not None and "</a>" in element["summary"]:
                    element["summary"] = None

    @classmethod
    def from_response(cls, response: dict, compress: bool = False):
        """Creates a :class:`Detail` object from a dict returned by
        :func:`google_play_scraper.app`.

        Parameters
        ----------
        response: dict
            A dict returned by :func:`google_play_scraper.app`.
        compress: bool, optional
            Whether to compress the :class:`Detail` object or not. For more information, see
            :func:`Detail.__compress`. Defaults to False.

        Returns
        -------
        Detail
            A :class:`Detail` object.
        """

        # Parse timestamps to datetime
        if response.get("saleTime") is not None:
            response["saleTime"] = hacky_datetime_parser(response["saleTime"])
        if response.get("updated") is not None:
            response["updated"] = hacky_datetime_parser(response["updated"])

        detail = Detail(
            app_id=response["appId"],
            lang=response["lang"],
            title=escape(response.get("title", None)),
            description=escape(response.get("description", None)),
            summary=escape(response.get("summary", None)),
            installs=response.get("installs", None),
            real_installs=response.get("realInstalls", None),
            score=response.get("score", None),
            ratings=response.get("ratings", None),
            reviews=response.get("reviews", None),
            histogram=response.get("histogram", None),
            price=response.get("price", None),
            free=response.get("free", None),
            currency=response.get("currency", None),
            sale=response.get("sale", None),
            sale_time=response.get("saleTime", None),
            offers_iap=response.get("offersIAP", None),
            in_app_product_price=response.get("inAppProductPrice", None),
            size=response.get("size", None),
            android_version=response.get("androidVersion", None),
            android_version_text=response.get("androidVersionText", None),
            developer_internal_id=response.get("developerInternalID", None),
            developer=escape(response.get("developer", None)),
            developer_id=response.get("developerId", None),
            developer_email=escape(response.get("developerEmail", None)),
            developer_website=escape(response.get("developerWebsite", None)),
            developer_address=escape(response.get("developerAddress", None)),
            privacy_policy=escape(response.get("privacyPolicy", None)),
            genre=response.get("genre", None),
            genre_id=response.get("genreId", None),
            icon=response.get("icon", None),
            header_image=response.get("headerImage", None),
            screenshots=response.get("screenshots", None),
            video=response.get("video", None),
            video_image=response.get("videoImage", None),
            content_rating=response.get("contentRating", None),
            content_rating_description=response.get("contentRatingDescription", None),
            ad_supported=response.get("adSupported", None),
            contains_ads=response.get("containsAds", None),
            released=response.get("released", None),
            updated=response.get("updated", None),
            version=response.get("version", None),
            recent_changes=escape(response.get("recentChanges", None)),
            similar_apps=response.get("similarApps", None),
            more_by_developer=response.get("moreByDeveloper", None),
            other_languages=response.get("otherLanguages", None),
            data_safety_short=response.get("dataSafety", None),
        )

        if compress:
            detail.compress()

        return detail
