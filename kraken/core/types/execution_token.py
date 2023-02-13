from datetime import datetime
from mongoengine.document import Document
from mongoengine.fields import (
    DateTimeField,
    ReferenceField,
    ListField,
    IntField,
    StringField,
)


# TODO: Add documentation
class ExecutionToken(Document):
    stages: list = ListField()  # type: ignore
    crawl = ReferenceField("Crawl", default=None)  # TODO: Add type hint
    created: datetime = DateTimeField(default=datetime.utcnow)  # type: ignore
    started: datetime = DateTimeField(default=None)  # type: ignore
    finished: datetime = DateTimeField(default=None)  # type: ignore
    failed: datetime = DateTimeField(default=None)  # type: ignore
    retries: int = IntField(default=0)  # type: ignore
    retry_infos: list = ListField(default=list)  # type: ignore
    fail_info: str = StringField(default=None)  # type: ignore
    progress: list = ListField(default=list)  # type: ignore

    def start(self, time=None, save=True):
        if time is None:
            time = datetime.utcnow()
        self.started = time

        if save:
            self.save()

    def retry(self, info=None, save=True):
        self.retries += 1
        if info is not None:
            self.retry_infos.append(info)

        if save:
            self.save()

    def finish(self, time=None, save=True):
        if time is None:
            time = datetime.utcnow()
        self.finished = time

        if save:
            self.save()

    def fail(self, time=None, info=None, save=True):
        if time is None:
            time = datetime.utcnow()
        self.failed = time

        if info is not None:
            self.fail_info = info

        if save:
            self.save()

    def remove(self):
        self.delete()
