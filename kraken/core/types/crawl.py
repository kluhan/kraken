import json
from typing import Union, List
from datetime import datetime

from mongoengine import ValidationError
from mongoengine.document import Document
from mongoengine.fields import (
    StringField,
    IntField,
    DateTimeField,
    LazyReferenceField,
    DictField,
    ListField,
)

from .stage import Stage


ARGS_VAL_ERROR = (
    "No positional arguments allowed, use keyword-arguments instead"  # noqa
)


# TODO: Add documentation
class Crawl(Document):
    # General Informations
    name: str = StringField(unique=True)  # type: ignore
    series = LazyReferenceField("Series", default=None)  # TODO: Add type hint
    iteration: int = IntField(default=0)  # type: ignore
    created: datetime = DateTimeField(default=datetime.now)  # type: ignore
    started: datetime = DateTimeField(default=datetime.now)  # type: ignore
    finished: datetime = DateTimeField(default=None)  # type: ignore
    stages: list[dict] = ListField(default=list)  # type: ignore
    filter: str = StringField(default_factory=str)  # type: ignore

    # Monitoring Informations
    targets_scheduled: int = IntField(default=0)  # type: ignore
    targets_finished: int = IntField(default=0)  # type: ignore
    targets_failed: int = IntField(default=0)  # type: ignore
    targets_retried: int = IntField(default=0)  # type: ignore
    progress: dict = DictField(default=dict)  # type: ignore
    expectations: dict = DictField(default=dict)  # type: ignore

    def has_started(self, date_time=None, auto_save=True):
        if date_time is None:
            date_time = datetime.utcnow()
        self.started = date_time
        self.save()

    def has_finished(self, date_time=None, auto_save=True):
        if date_time is None:
            date_time = datetime.utcnow()
        self.finished = date_time
        self.save()

    def __init__(self, *args, **kwargs):
        # Check that no 'args' are supplied
        if len(args) >= 1:
            raise ValidationError(ARGS_VAL_ERROR)
        # Call super-constructor
        super(Crawl, self).__init__(**kwargs)
        # Set crawl name if not already set
        if not self.name:
            # Raise ArgumentError if no series is set, since it is needed to derive the name
            if self.series is None:
                raise ValueError()  # TODO: Add error message
            # Derive name based on the parent series
            self.name = self.series.fetch().name + "_" + str(self.iteration)

    def get_filter(self) -> dict:
        """Returns the filter for the series."""

        return json.loads(self.filter) if json.loads(self.filter) else {}

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
