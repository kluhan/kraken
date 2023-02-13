"""
This module contains the tasks that are used to request information from the
Google Play Store. The tasks are grouped into submodules, which are named after
the information they request.
"""

from . import detail_request
from . import permission_request
from . import review_request
from . import data_safety_request

from .detail_request import request_details
from .permission_request import request_permissions
from .review_request import request_reviews
from .data_safety_request import request_data_safety


__all__ = [
    "detail_request",
    "permission_request",
    "review_request",
    "data_safety_request",
    "request_details",
    "request_permissions",
    "request_reviews",
    "request_data_safety",
]
