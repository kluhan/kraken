from celery import Task

from kraken.core.types import ExecutionToken, Crawl
from kraken.utils.mongodb import MongoEngineConnectionWrapper


class DatabaseTask(Task):
    # Opens a connection to the database
    __connection = MongoEngineConnectionWrapper.connect()

    @property
    def connection(self):
        if self.__connection is None:
            self.__connection = MongoEngineConnectionWrapper.connect()
        return self.__connection

    def task_prerun(self, task_id, args, kwargs):
        __ = self.connection


class CrawlTask(Task):
    # Max length of result representation used in logs and events
    resultrepr_maxsize = 8192
    # Opens a connection to the database
    __connection = MongoEngineConnectionWrapper.connect()

    @property
    def connection(self):
        if self.__connection is None:
            self.__connection = MongoEngineConnectionWrapper.connect()
        return self.__connection

    def task_prerun(self, task_id, args, kwargs):
        __ = self.connection

    def before_start(self, task_id, args, kwargs):
        # Check if executionToken is present
        if kwargs.get("execution_token_id", None) is not None:
            # Load and update Execution Token
            token: ExecutionToken = ExecutionToken.objects.get(
                pk=kwargs["execution_token_id"]
            )
            token.start()

    def on_retry(self, exc, task_id, args, kwargs, einfo):
        # Check if executionToken is present
        if kwargs.get("execution_token_id", None) is not None:
            # Load and update Execution Token
            token: ExecutionToken = ExecutionToken.objects.get(
                pk=kwargs["execution_token_id"]
            )
            token.retry(info=str(einfo))
        # Check if crawl_id is present
        if kwargs.get("crawl_id", None) is not None:
            # Update tasks retried counter
            Crawl.objects(id=kwargs["crawl_id"]).update(inc__targets_retried=1)

    def on_failure(self, exc, task_id, args, kwargs, einfo):
        # Check if executionToken is present
        if kwargs.get("execution_token_id", None) is not None:
            # Load and update Execution Token
            token: ExecutionToken = ExecutionToken.objects.get(
                pk=kwargs["execution_token_id"]
            )
            token.fail(info=str(einfo))
        # Check if crawl_id is present
        if kwargs.get("crawl_id", None) is not None:
            # Update tasks failed counter
            Crawl.objects(id=kwargs["crawl_id"]).update(inc__targets_failed=1)

    def on_success(self, retval, task_id, args, kwargs):
        # Delete token as the crawl exited successfully
        if kwargs.get("execution_token_id", None) is not None:
            # Load and update Execution Token
            token: ExecutionToken = ExecutionToken.objects.get(
                pk=kwargs["execution_token_id"]
            )
            token.remove()
        # Check if crawl_id is present
        if kwargs.get("crawl_id", None) is not None:
            # Update tasks failed counter
            Crawl.objects(id=kwargs["crawl_id"]).update(inc__targets_finished=1)
