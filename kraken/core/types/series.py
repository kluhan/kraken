import re
import json
from typing import List, Union

from mongoengine import ValidationError
from mongoengine.document import Document
from mongoengine.fields import (
    IntField,
    StringField,
    ListField,
    ReferenceField,
)

from .crawl import Crawl
from .stage import Stage

ARGS_VAL_ERROR = (
    "No positional arguments allowed, use keyword-arguments instead"  # noqa
)


# TODO: Add documentation
class Series(Document):
    """
    Base class to configure a series of crawls with the same configuration.

    Attributes:
        name (str): Unique name for the Series
        description (str): Description of the Series.
        stages (list[dict]): List of Stages, defining the structure for the Crawls of the Series.
        filter (dict): MongoDB filter, defining a subset of the known Targets for the Crawls of the Series. Do not use this attribute directly, use the get_filter() and set_filter() methods instead.
        crawls (list[Crawl]): List of Crawls in the Series.
        iterations (int): Number of already performed or started Crawls of the Series.

    Methods:
        get_filter(): Returns the filter for the series.
        set_filter(filter): Sets the filter for the series.
        new_crawl(auto_save=True): Returns a new crawl, based on the series.
    """

    name: str = StringField(unique=True)  # type: ignore
    description: str = StringField(default=None)  # type: ignore
    stages: list[dict] = ListField(default=list)  # type: ignore
    filter: str = StringField(default=str)  # type: ignore
    crawls: list = ListField(ReferenceField("Crawl"), default=list)  # type: ignore
    iterations: int = IntField(default=0)  # type: ignore

    def get_filter(self) -> dict:
        """Returns the filter for the series."""

        return json.loads(self.filter)

    def set_filter(self, filter: Union[dict, str]) -> None:
        """Sets the filter for the series. If the filter is a dict, it will be converted to a string."""

        if isinstance(filter, dict):
            self.filter = json.dumps(filter)
        elif isinstance(filter, str):
            self.filter = filter
        else:
            raise ValidationError(
                f"Invalid filter type. filter must be of type dict or str and not of type {type(filter)}."
            )

    def get_stages(self) -> List[Stage]:
        """Returns the stages for the series."""

        return [Stage.from_dict(stage) for stage in self.stages]

    def __init__(self, *args, **kwargs):
        # Check that no 'args' are supplied
        if len(args) >= 1:
            raise ValidationError(ARGS_VAL_ERROR)
        # Call super-constructor
        super(Series, self).__init__(**kwargs)
        # Set id-field if not set in advance
        if self.name is None:
            self.name = re.sub("[^0-9a-zA-Z]+", "_", self.description.lower())

    def new_crawl(self, auto_save=True) -> Crawl:
        """Initializes a new crawl document and updates the iterations"""

        self.iterations += 1
        crawl = Crawl(
            series=self.id,
            iteration=self.iterations,
            stages=self.stages,
            filter=self.filter,
        )
        self.crawls.append(crawl)

        if auto_save:
            crawl.save()
            self.save()

        return crawl
