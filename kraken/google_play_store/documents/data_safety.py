from mongoengine.fields import DictField, StringField, ListField

from kraken.core.types import HistoricDocument


class DataSafety(HistoricDocument):
    """
    A class for modelling and representing the information of the data-safety
    page of a Google Play Store app.
    It inherits from :class:`kraken.core.types.HistoricDocument`, allowing it
    to be stored in a MongoDB.
    """

    id: str = StringField(primary_key=True)
    """The ID used to store the document in the database."""
    app_id: str = StringField(unique_with="lang")
    """The ID of the app as used in the Google Play Store. Has a unique constraint with :attr:`lang`."""
    lang: str = StringField(unique_with="app_id")
    """The language of the app detail page. Has a unique constraint with :attr:`app_id`."""
    data_collected: dict[str, list[dict[str, str]]] = DictField()
    """The data collected by the app."""
    data_shared: dict[str, list[dict[str, str]]] = DictField()
    """The data shared by the app."""
    security_practices: list[dict[str, str]] = ListField()
    """The security practices applied by the app."""

    meta = {"collection": "gpc_data_safety", "indexes": [("app_id", "lang")]}

    def __init__(
        self,
        app_id: str,
        lang: str,
        id: str = None,
        compress: bool = False,
        *args,
        **kwargs
    ):
        """
        Constructor for a :class:`DataSafety` object. :attr:`app_id`, :attr:`lang` are required.
        All other fields are optional and can be set using keyword arguments. For a list of
        available fields, see :class:`DataSafety`.

        Parameters
        ----------
        app_id : str
            The ID of the app as used in the Google Play Store.
        lang : str
            The language of the app data safety page.
        id : str, optional
            The ID used to store the document in the database. Per default this is a combination of :attr:`app_id` and :attr:`lang`.
        compress: bool, optional
            Whether the object should be compressed or not. Defaults to ``False``.
            For more information on compression, see :meth:`compress`.
        """

        # Add id if not already present
        id = id if id is not None else app_id + ":" + lang
        # Call super-constructor
        super(DataSafety, self).__init__(
            app_id=app_id, lang=lang, id=id, *args, **kwargs
        )
        # Compress if necessary
        if compress:
            self.compress()

    # TODO: Implement a proper weight function
    def weight(self) -> int:
        """
        Returns the weight of the object. The weight of a :class:`DataSafety`
        is allays equal to `1`.

        Returns
        -------
        int
            The weight of the object. Always 1.
        """

        return 1

    @staticmethod
    def wcf_weights() -> dict[str, int]:

        return {
            "data_collected": 1,
            "data_shared": 1,
            "security_practices": 1,
        }

    def compress(self) -> None:
        """
        Compresses the object to reduce the memory footprint of the object
        when it is stored. Currently, this method does nothing and only exists
        for compatibility reasons.
        """
        pass

    @classmethod
    def from_response(cls, response: dict, compress: bool = False):
        """
        Creates a :class:`DataSafety` object from a dict returned by
        :func:`google_play_scraper.data_safety`.

        Parameters
        ----------
        response: dict
            A dict returned by :func:`google_play_scraper.data_safety`.
        compress: bool, optional
            Whether to compress the :class:`DataSafety` object or not. For more information, see
            :func:`DataSafety.__compress`. Defaults to False.

        Returns
        -------
        DataSafety
            A :class:`DataSafety` object.
        """

        data_safety = DataSafety(
            app_id=response.get("app_id", None),
            lang=response.get("lang", None),
            data_collected=response.get("dataCollected", None),
            data_shared=response.get("dataShared", None),
            security_practices=response.get("securityPractices", None),
        )

        if compress:
            data_safety.compress()

        return data_safety
