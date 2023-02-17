from typing import Union
from dataclasses import dataclass

from dacite.core import from_dict

from .slim_target import SlimTarget


@dataclass
class RequestResult:
    result: Union[dict, list[dict]]
    """The result of a request. Can be a arbitrary dictionary or a list of dictionaries."""
    subsequent_kwargs: dict | None = None
    """Updated kwargs for the next request to the same target. Often used to crawl paginated results, like comments or posts."""
    batch: bool = False
    """Specifies if the result is a batch of results or a single result."""
    gain: int = 1
    """The gain of the request. This is typically the number of retrieved documents if the request was successful. If the request was not successful, the gain should be 0."""
    cost: int = 1
    """The cost of the request. This is typically the number of requests performed even if the request was not successful."""
    target_not_found: bool = False
    """Specifies if the target was not found."""
    target_exhausted: bool | None = None
    """Specifies if the target was exhausted. This is typically the case if the target is a paginated collection and the last page was reached."""
    adjacent_targets: list[SlimTarget] | None = None
    """A list of adjacent targets. Adjacent targets are all targets that could be constructed from the result of the request."""

    @classmethod
    def from_dict(cls, data: dict) -> "RequestResult":
        """
        Creates a RequestResult from a dictionary.

        Parameters
        ----------
        data : dict
            The dictionary from which the RequestResult should be created.

        Returns
        -------
        RequestResult
            The created RequestResult.
        """

        return from_dict(data_class=cls, data=data)
