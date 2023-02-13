import os
import orjson


from kombu.serialization import register
from kombu import Exchange
from kombu import Queue


broker = os.environ.get("CELERY_BROKER_URL", None)
result_backend = os.environ.get("CELERY_BACKEND_URL", None)

include = [
    # register generic tasks
    "kraken.core.tasks.multi_stage_crawler",
    "kraken.core.tasks.single_stage_crawler",
    "kraken.core.tasks.pipelines.data_storage_pipeline",
    "kraken.core.tasks.pipelines.target_discovery_pipeline",
    "kraken.core.tasks.callbacks.crawl_monitor_callback",
    "kraken.core.tasks.callbacks.target_monitor_callback",
    "kraken.core.tasks.terminators.overlap_terminator",
    "kraken.core.tasks.terminators.budget_terminator",
    "kraken.core.tasks.terminators.static_terminator",
    # register google play store specific tasks
    "kraken.google_play_store.tasks.requests.detail_request",
    "kraken.google_play_store.tasks.requests.permission_request",
    "kraken.google_play_store.tasks.requests.review_request",
    "kraken.google_play_store.tasks.requests.data_safety_request",
    "kraken.google_play_store.tasks.sub_tasks.document_factory",
]

# register all tasks from kraken
imports = []

# register orjson into kombu
register(
    "orjson",
    orjson.dumps,
    orjson.loads,
    content_type="application/x-orjson",
    content_encoding="utf-8",
)

accept_content = ["json", "msgpack", "orjson"]
task_serializer = "orjson"
result_serializer = "orjson"

enable_utc = True
worker_send_task_events = True
task_create_missing_queues = True
task_send_sent_event = True
task_track_started = True
task_acks_late = True

task_queues = {
    Queue("crawler", Exchange("crawler"), routing_key="crawler", delivery_mode=1),
    Queue("pipeline", Exchange("crawler"), routing_key="pipeline", delivery_mode=1),
    Queue("callback", Exchange("callback"), routing_key="callback", delivery_mode=1),
    Queue(
        "terminator", Exchange("terminator"), routing_key="terminator", delivery_mode=1
    ),
    Queue("request", Exchange("request"), routing_key="request", delivery_mode=1),
}

task_routes = {
    "kraken.crawler.multi_stage": {"queue": "crawler", "routing_key": "crawler"},
    "kraken.crawler.single_stage": {"queue": "crawler", "routing_key": "crawler"},
    "kraken.pipeline.*": {"queue": "pipeline", "routing_key": "pipeline"},
    "kraken.callback.*": {"queue": "callback", "routing_key": "callback"},
    "kraken.terminator.*": {"queue": "terminator", "routing_key": "terminator"},
    "kraken.google_play_store.*": {"queue": "request", "routing_key": "request"},
}
