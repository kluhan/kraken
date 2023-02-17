import hashlib

from datetime import datetime

from mongoengine.fields import (
    FloatField,
    StringField,
    IntField,
    DateTimeField,
)

from kraken.core.types import HistoricDocument
from kraken.utils import escape, hacky_datetime_parser

USER_IMAGE_PREFIX = "https://play-lh.googleusercontent.com"
USER_NAME_TRIVIAL = [
    "Ein Google-Nutzer",
    "A Google user",
    "Un usuario de Google",
    "Un utilisateur de Google",
]


class Review(HistoricDocument):
    """
    A class for modelling and representing a user review of a Google Play Store app.
    It inherits from :class:`kraken.core.types.HistoricDocument`, allowing it
    to be stored in a MongoDB.
    """

    review_id: str = StringField(primary_key=True)
    """The ID used to store the document in the database."""
    replied_at: datetime = DateTimeField()
    """The date and time the review was replied to."""
    reply_content: str = StringField()
    """The content of the reply by the developer to the review."""
    app_id: str = StringField()
    """The ID of the app the review is for."""
    lang: str = StringField()
    """The language of the review."""
    at: datetime = DateTimeField()
    """The date and time the review was written."""
    content: str = StringField()
    """The content of the review."""
    review_created_version: str = StringField()
    """The version of the app the review was written for."""
    score: float = FloatField()
    """The score given by the user for the app."""
    thumbs_up_count: int = IntField()
    """The number of thumbs up the review has received."""
    user_image: str = StringField()
    """The URL of the user's profile picture."""
    user_name: str = StringField()
    """The name of the user who wrote the review."""

    meta = {"collection": "gps_review"}

    def __init__(
        self,
        review_id: str,
        compress: bool = False,
        *args,
        **kwargs,
    ):
        """
        Constructor for a :class:`Review` object. :attr:`review_id` is required.
        All other fields are optional and can be set using keyword arguments. For a list of
        available fields, see :class:`Review`.

        Parameters
        ----------
        review_id : str
            The ID used to store the document in the database.
        compress: bool, optional
            Whether the object should be compressed or not. Defaults to ``False``.
            For more information on compression, see :meth:`compress`.
        """

        # Call super-constructor
        super(Review, self).__init__(review_id=review_id, *args, **kwargs)
        # Compress if necessary
        if compress:
            self.compress()

    def weight(self):
        """
        Returns the weight of the object. The weight of an :class:`Review` is
        equal to its :attr:`thumbs_up_count`.

        Returns
        -------
        int
            The weight of the object.

        Raises
        ------
        AttributeError
            If :attr:`thumbs_up_count` is not set.
        """
        return self.thumbs_up_count

    @staticmethod
    def wcf_weights():

        return {
            "at": 1,
            "content": 5,
            "replied_at": 25,
            "reply_content": 25,
            "score": 10,
            "thumbs_up_count": 10,
        }

    def compress(self) -> None:
        """
        Compresses the object to reduce the memory footprint of the object
        when it is stored. Specifically, the following fields are compressed:

        - :attr:`review_id` is hashed using SHA256 as the review ID returned by the Google
            Play API is quite long.
        - :attr:`user_image` is shortened by removing the prefix "https://play-lh.googleusercontent.com".
        - :attr:`user_name` is set to ``None`` if it is equal to one of the default names used
            by Google Play Store for anonymous users.
        """

        # compress review_id
        if isinstance(self.review_id, str):
            self.review_id = hashlib.sha256(self.review_id.encode("utf-8")).hexdigest()

        # compress user_image
        if isinstance(self.user_image, str):
            self.user_image = self.user_image.removeprefix(USER_IMAGE_PREFIX)

        # compress user_name
        if isinstance(self.user_name, str):
            self.user_name = (
                None if self.user_name in USER_NAME_TRIVIAL else self.user_name
            )

    @classmethod
    def from_response(cls, response: dict, compress: bool = False):
        """
        Creates a :class:`Review` object from a dict returned by
        :func:`google_play_scraper.reviews`.

        Parameters
        ----------
        response: dict
            A dict returned by :func:`google_play_scraper.reviews`.
        compress: bool, optional
            Whether to compress the :class:`Review` object or not. For more information, see
            :func:`Review.__compress`. Defaults to False.

        Returns
        -------
        Review
            A :class:`Review` object.
        """

        # Parse timestamps to datetime if not already datetime
        if response.get("at") is not None:
            response["at"] = hacky_datetime_parser(response["at"])

        if response.get("repliedAt") is not None:
            response["repliedAt"] = hacky_datetime_parser(response["repliedAt"])

        review = Review(
            review_id=response["reviewId"],
            app_id=response["app_id"],
            lang=response["lang"],
            at=response["at"],
            content=escape(response["content"]),
            replied_at=response["repliedAt"],
            reply_content=escape(response["replyContent"]),
            review_created_version=response["reviewCreatedVersion"],
            score=response["score"],
            thumbs_up_count=response["thumbsUpCount"],
            user_image=response["userImage"],
            user_name=escape(response["userName"]),
        )

        if compress:
            review.compress()

        return review
