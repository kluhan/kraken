from mongoengine.fields import (
    DictField,
    StringField,
)

from kraken.core.types import HistoricDocument


class Permission(HistoricDocument):
    """
    A class to represent a Google Play Store app permission page. It inherits from
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
    content: dict = DictField()
    """The permissions requested by the app."""

    meta = {"collection": "gpc_permission", "indexes": [("app_id", "lang")]}

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
        Constructor for a :class`Permission` object. :attr:`app_id`, :attr:`lang` are required.
        All other fields are optional and can be set using keyword arguments. For a list of
        available fields, see :class:`Permission`.

        Parameters
        ----------
        app_id : str
            The ID of the app as used in the Google Play Store.
        lang : str
            The language of the app permission page.
        id : str, optional
            The ID used to store the document in the database. Per default this is a combination of :attr:`app_id` and :attr:`lang`.
        compress: bool, optional
            Whether the object should be compressed or not. Defaults to ``False``.
            For more information on compression, see :meth:`compress`.
        """
        # Add id if not already present
        id = id if id is not None else app_id + ":" + lang
        # Call super-constructor
        super(Permission, self).__init__(
            app_id=app_id, lang=lang, id=id, *args, **kwargs
        )
        # Compress if necessary
        if compress:
            self.compress()

    # TODO: Implement a proper weight function
    def weight(self):
        """Returns the weight of the object, often used for monitoring purposes
        as well as for resource allocation. The weight is always 1.

        Returns
        -------
        int
            The weight of the object.
        """

        return 1

    @staticmethod
    def wcf_weights():
        """Returns the weights of the individual fields for the WCF algorithm. Check
        :class:`kraken.core.types.historic_document.HistoricDocument` for more information.

        Returns
        -------
        The weights for relevant fields for the WCF model.
        """

        return {
            "content": 1,
        }

    def compress(self) -> None:
        """Compresses the object to reduce the memory footprint of the object
        when it is stored. Currently, this method does nothing and is only intended
        for consistency with other classes.
        """

        pass

    @classmethod
    def from_response(cls, response: dict, compress: bool = False):
        """Creates a :class:`Permission` object from a dict returned by
        :func:`google_play_scraper.permissions`.

        Parameters
        ----------
        response: dict
            A dict returned by :func:`google_play_scraper.permissions`.
        compress: bool, optional
            Whether to compress the :class:`Permission` object or not. For more information, see
            :func:`Permission.__compress`. Defaults to False.

        Returns
        -------
        Permission
            A :class:`Permission` object.
        """
        permission = Permission(
            app_id=response.pop("app_id", None),
            lang=response.pop("lang", None),
            content={k: v for k, v in response.items() if v is not None},
        )
        if compress:
            permission.compress()

        return permission
