"""
This module contains the document classes to represent the information returned
by the Google Play Store. Each class represents a specific type of information and
can be used to store the information in a database. Check
`mongoengine.Document <http://docs.mongoengine.org/apireference.html#documents>`_
and "google_play_kraken.core.types.HistoricDocument" and for more information about
how to use the inherited methods and functions.
"""

from .data_safety import DataSafety
from .detail import Detail
from .permission import Permission
from .review import Review

from .base.document_type import DocumentType

__all__ = [
    "DataSafety",
    "Detail",
    "Permission",
    "Review",
    "DocumentType",
]
