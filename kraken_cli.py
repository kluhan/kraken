import json
import click
import re

from datetime import timedelta
from types import NoneType
from typing import List, Tuple, Union
from io import TextIOWrapper
from pathlib import Path
from itertools import product
from jsonschema import validate, ValidationError
from rich import print
from rich.progress import track
from rich.prompt import Confirm
from dotenv import load_dotenv
from pymongo.errors import OperationFailure
from mongoengine import DoesNotExist, MultipleObjectsReturned
from celery import signature

# load .env files for local execution
test_env = load_dotenv(Path("env/.local.env"), verbose=True)
worker_env = load_dotenv(Path("env/.worker.env"), verbose=True)
mongo_env = load_dotenv(Path("env/.mongodb.env"), verbose=True)

from kraken.core.types import Target, Series, Crawl  # noqa
from kraken.core.schedulers.static_scheduler import StaticScheduler  # noqa
from kraken.core.tasks import multi_stage_crawler  # noqa
from kraken.utils.mongodb import MongoEngineContextManager  # noqa
from kraken.utils.mongoengine import mongodb_key_sanitizer  # noqa


from kraken import celery_app  # noqa

BUCKET_SIZE = 10000
TASK_SCHEMA = {
    "type": "object",
    "properties": {
        "task": {"type": "string"},
        "args": {"type": "array"},
        "kwargs": {"type": "object"},
    },
}
STAGE_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "request": TASK_SCHEMA,
        "pipelines": {"type": "array", "items": TASK_SCHEMA},
        "terminators": {"type": "array", "items": TASK_SCHEMA},
        "callbacks": {"type": "array", "items": TASK_SCHEMA},
    },
}


def __load_app_ids(file: TextIOWrapper) -> List[str]:
    """Load the App IDs from the given file."""

    # Read the contents of the file
    print("[blue]Loading data from file...".ljust(45, " "), end="")
    try:
        data = json.load(file)
        print("[green]DONE")

    except json.JSONDecodeError:
        print("[red]FAILED")
        print("The file is not a valid JSON file!")
        exit(1)

    # Check if the loaded data is a list of strings
    print("[blue]Checking the data format...".ljust(45, " "), end="")
    if isinstance(data, list) and all(isinstance(item, str) for item in data):
        print("[green]DONE")
    else:
        print("[red]FAILED")
        print("The JSON file is not a list of strings! Please check the file!")
        exit(1)

    return data


def __write_targets_to_database(
    app_ids: List[str],
    languages: List[str],
    tags: List[str],
    upsert: bool = True,
    continue_on_error: bool = False,
    bucket_size: int = 10000,
) -> Tuple[int, int]:
    """Create Targets for the given App IDs and languages and add them to the database.

    Args:
        app_ids (list[str]): The App IDs for which to create Targets.
        languages (list[str]): The languages for which to create Targets.
        tags (list[str]): The tags to add to the Targets.
        upsert (bool, optional): If the tags should be upserted. Defaults to True.
        continue_on_error (bool, optional): If the script should continue on error. Defaults to False.
        bucket_size (int, optional): The size of the bucket. Defaults to 10000.

    Returns:
        tuple: The updated number of added, updated and skipped Targets as well as the number of errors.
    """

    bucket = []
    added = 0
    updated = 0
    skipped = 0
    errors = 0

    # Open database connection.
    with MongoEngineContextManager():
        # Use track to display a progress bar.
        for app_id, language in track(
            product(app_ids, languages),
            description="[blue]Writing to database...".ljust(44, " "),
            total=len(app_ids) * len(languages),
        ):

            try:
                target = Target.objects.get(
                    kwargs__app_id=app_id, kwargs__lang=language
                )
                # If the target already exists, update it if upsert is enabled
                if upsert:
                    # Add the tags to the target without creating duplicates but
                    # keep already existing duplicates.
                    target.tags.extend([x for x in tags if x not in target.tags])
                    target.save()
                    updated += 1
                else:
                    skipped += 1

            # If the target does not exist, add it to the bucket.
            except DoesNotExist:
                bucket.append(
                    Target(
                        tags=tags,
                        kwargs={"app_id": app_id, "lang": language},
                        discovered_at=None,
                    )
                )
                added += 1

            # If multiple Targets exist, abort.
            except MultipleObjectsReturned:
                if continue_on_error:
                    errors += 1
                else:
                    print(
                        f'[red]Multiple Targets detected for [app_id:"{app_id}", lang: "{language}"]!'
                    )
                    print("Check the database for inconsistency!")
                    print()
                    print("Aborted!")
                    exit(1)

            # If the bucket is full, insert it into the database.
            if len(bucket) % bucket_size == 0 and len(bucket) >= 1:
                Target.objects.insert(bucket)
                bucket = []

        # Insert the remaining Targets into the database.
        if len(bucket) >= 1:
            Target.objects.insert(bucket)

    print()
    print("Summary:")
    print(
        f"    Added: [green]{added}[/], Updated: [yellow]{updated}[/], Skipped: [dark_orange]{skipped}[/], Errors: [red]{errors}[/]"
    )
    print()


def __ensure_indexes():
    """Ensure the composite index on app_id and lang"""

    print("[blue]Ensuring indexes...".ljust(45, " "), end="")
    try:
        with MongoEngineContextManager():
            Target.create_index(["kwargs.app_id", "kwargs.lang"])
        print("[green]DONE[/]")

    except Exception as e:
        print("[red]FAILED[/]")
        print(f"Failed to create index: {e}")
        exit(1)


def __handle_task_not_known_exception(task: dict):
    """Handle the TaskNotKnownException by asking the user if he wants to continue. If not, the script will exit.

    Args:
        task (dict): The task that is not known.
    """
    print("[yellow]WARNING")
    if Confirm.ask(
        f'The task-type [red]{task["task"]}[/] is not known to any of the workers in the cluster. Continue?'
    ):
        print("[blue]Checking task types...".ljust(45, " "), end="")
        return True
    else:
        print()
        print("Aborted!")
        exit(1)


def __load_stage(file: TextIOWrapper, known_tasks_types: List[str]):
    """Load a stage from a file. The file must be a valid JSON file and must have the correct schema."""

    # Read the contents of the file
    print(f"Loading stage from {file.buffer.name}:")
    # Check if the loaded data can be parsed as JSON
    print("[blue]Parsing JSON...".ljust(45, " "), end="")
    try:
        data = json.load(file)
        print("[green]DONE")

    except json.JSONDecodeError:
        print("[red]FAILED")
        print("    The file is not a valid JSON file!")
        exit(1)

    # Check if the loaded data has the correct schema
    print("[blue]Checking schema...".ljust(45, " "), end="")
    try:
        validate(data, STAGE_SCHEMA)
        print("[green]DONE")
    except ValidationError as e:
        print("[red]FAILED")
        offending_element = "".join(str(node) + "." for node in list(e.path)).rstrip(
            "."
        )
        print(f"ERROR: {e.message}. Check the {offending_element} element!")
        exit(1)

    # Check if the loaded stage contains only known task types
    print("[blue]Checking task types...".ljust(45, " "), end="")
    for task in [
        data["request"],
        data["pipelines"],
        data["terminators"],
        data["callbacks"],
    ]:
        if isinstance(task, list):
            for sub_task in task:
                if sub_task["task"] not in known_tasks_types:
                    __handle_task_not_known_exception(sub_task)
        else:
            if task["task"] not in known_tasks_types:
                __handle_task_not_known_exception(task)
    print("[green]DONE")

    return data


def __write_series_to_database(
    name: Union[str, NoneType],
    description: Union[str, NoneType],
    stages: list[dict],
    filter: dict,
):
    """Write the series to the database

    Args:
        name (str): The name of the series.
        description (Union[str, NoneType]): The description of the series.
        stages (list[dict]): The stages of the series.
    """

    # Sanitize the name as it is used as the id
    name = mongodb_key_sanitizer(name)

    """Write the series to the database"""
    with MongoEngineContextManager():

        # Check if the series already exists
        print("[blue]Check if the series already exists...".ljust(45, " "), end="")
        try:
            _ = Series.objects.get(name=name)
            # If the series already exists, ask the user if he wants to overwrite it.
            print("[yellow]WARNING")
            if Confirm.ask(
                f"The series [yellow]{name}[/] already exists. Do you want to [red][u]overwrite[/][/] it?"
            ):
                pass
            else:
                print()
                print("Aborted!")
                exit(1)
        # If the target does not exist, continue.
        except DoesNotExist:
            print("[green]DONE")

        # Add the series to the database
        print("[blue]Writing series to database...".ljust(45, " "), end="")
        series = Series(
            name=name, description=description, stages=stages, filter=json.dumps(filter)
        )
        Series._get_collection().find_one_and_update(
            {"name": name},
            update={"$set": series.to_mongo()},
            upsert=True,
        )
        print("[green]DONE")
        print()
        print("Summary:")
        print(f"    ID: [green]{Series.objects.get(name=name).id}[/]")
        print()


def __load_filter(file: TextIOWrapper):
    """Load filter from file and check it is valid.

    Args:
        filters (TextIOWrapper): The filters to load and check.
    """

    # Read the contents of the file
    print(f"Loading filter from {file.buffer.name}:")
    # Check if the loaded data can be parsed as JSON
    print("[blue]Parsing JSON...".ljust(45, " "), end="")
    try:
        filter = json.load(file)
        print("[green]DONE")

    except json.JSONDecodeError:
        print("[red]FAILED")
        print("    The file is not a valid JSON file!")
        exit(1)

    # Check if the database accepts the filter
    print("[blue]Checking filter...".ljust(45, " "), end="")
    try:
        with MongoEngineContextManager():
            count = Target.objects(__raw__=filter).count()
        print("[green]DONE")

    except OperationFailure as e:
        print("[red]FAILED")
        print("Database does not accept the filter!")
        print(f"ERROR: {e._message}. Check the filter!")
        exit(1)

    print(f"Filter is valid. {count} Targets match the filter.")

    return filter


@click.group()
def cli():
    pass


@cli.command()
@click.argument(
    "app_id_file",
    type=click.File("r"),
    nargs=1,
    required=True,
)
@click.argument(
    "languages",
    nargs=-1,
    required=True,
)
@click.option(
    "--tag",
    multiple=True,
    help="The tag to add to the Targets. Can be supplied multiple times.",
)
@click.option(
    "-u",
    "--upsert_tags",
    is_flag=True,
    show_default=True,
    default=True,
    help="Upsert the specified tags. If this option is disabled, the tags of already present Targets will not be updated.",
)
@click.option(
    "-e",
    "--ensure_index",
    default=True,
    show_default=True,
    help="Ensure the composite index on 'app_id' and 'lang'.",
)
@click.option(
    "-c",
    "--continue_on_error",
    default=False,
    show_default=True,
    help="Continue inserting on error. If this option is disabled, the script will abort on the first error.",
)
@click.option(
    "-b",
    "--bucket_size",
    default=10000,
    show_default=True,
    help="The number of Targets to insert at once.",
)
def setup_targets(
    app_id_file: TextIOWrapper,
    languages: List[str],
    tag: List[str],
    upsert_tags: bool,
    ensure_index: bool,
    continue_on_error: bool,
    bucket_size: int,
):
    """
    Command to add Targets to the database.

    APP_ID_FILE must be a path to a JSON-file containing a list of app IDs. For
    each app ID, a Target will be created in all specified LANGUAGES.
    """
    # Load the app IDs from file
    app_ids = __load_app_ids(app_id_file)
    # Ensure the index before inserting
    if ensure_index:
        __ensure_indexes()
    # Add the Targets to the database
    __write_targets_to_database(
        app_ids=app_ids,
        languages=languages,
        tags=tag,
        upsert=upsert_tags,
        continue_on_error=continue_on_error,
        bucket_size=bucket_size,
    )


@cli.command()
def show_stage_schema():
    """
    Command to print the json-schema for a Stage to stdout. For further
    information see 'https://json-schema.org/'.
    """
    print(json.dumps(STAGE_SCHEMA, indent=4))


@cli.command()
@click.argument("name", nargs=1, required=True)
@click.option(
    "--description",
    default=None,
    help="Description of the Series.",
)
@click.option(
    "--stage",
    type=click.File("r"),
    multiple=True,
    help="The tag to add to the Targets. Can be supplied multiple times. Must be a valid JSON-file.",
)
@click.option(
    "--filter",
    type=click.File("r"),
    multiple=False,
    default=None,
    help="Filter to apply to the Targets. Must be a valid JSON-file. If not specified, no filter will be applied. For further information see 'https://docs.mongodb.com/manual/tutorial/query-documents/'.",
)
def setup_series(name, description, stage: List[TextIOWrapper], filter: List[str]):
    """
    Command to add a Series to the database. Stages
    passed via "--stage" must comply with a specific json-schema. The schema can
    be found in the documentation or via the "show-stage-schema" command.
    """

    # Initialize app
    app = celery_app.app
    # Connect to the cluster to load all known tasks
    known_tasks = app.control.inspect().registered()
    # Flatten the dict of known tasks
    known_tasks = list(set([task for tasks in known_tasks.values() for task in tasks]))
    # Remove all options from the task names
    known_tasks = [re.sub(r"\[.*?\]", "", task).strip() for task in known_tasks]

    # Load the passed stages
    stages = [__load_stage(s, known_tasks) for s in stage]

    print(f"Successfully loaded {len(stages)} stages!")

    if filter is not None:
        # Load the filter
        filter = __load_filter(filter)

    # Write the Series to the database
    __write_series_to_database(name, description, stages, filter)


@cli.command()
@click.argument("id", nargs=1, required=True)
@click.option(
    "--continue_crawl",
    is_flag=False,
    show_default=True,
    default=True,
    help="Continue last crawl ot the Series, if it was not finished. If this option is disabled, a new one will be started.",
)
def daemon(id, continue_crawl: bool):
    """
    Command to start a daemon that will execute a new Crawl for the specified Series.
    """

    # Initialize app
    app = celery_app.app

    with MongoEngineContextManager():

        # Load the Series
        series: Series = Series.objects.get(id=id)

        if continue_crawl and len(series.crawls) >= 1:
            # Get the last Crawl of the Series
            crawl: Crawl = series.crawls[-1]
        else:
            # Create a new Crawl
            crawl: Crawl = series.new_crawl()
            crawl.save()

        scheduler = StaticScheduler(
            crawl=crawl,
            step_size=5,
            step_period=timedelta(seconds=5),
            crawl_task=signature(multi_stage_crawler.name),
        )

        scheduler.start()


if __name__ == "__main__":
    cli()
