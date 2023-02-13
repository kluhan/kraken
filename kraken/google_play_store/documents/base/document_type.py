from enum import Enum


class DocumentType(str, Enum):
    """An enum to represent the types :class:`DataSafety`, :class:`Detail`, :class:`Permission` and :class:`Review`."""

    DETAIL = "DETAIL"
    PERMISSION = "PERMISSION"
    REVIEW = "REVIEW"
    DATA_SAFETY = "DATA_SAFETY"
