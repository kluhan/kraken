from typing import List
from json import loads, dumps

from dataclasses import dataclass, field

from celery.canvas import Signature
from celery import signature
from dacite.core import from_dict
from dacite.config import Config

from .target import SlimTarget
from .pipeline_result import PipelineResult


TERMINATOR_KEY_TARGET_NOT_FOUND = "target_not_found"
TERMINATOR_KEY_TARGET_EXHAUSTED = "target_exhausted"


@dataclass
class StageResult:
    cost: int = field(default=0)
    gain: int = field(default=0)
    pipeline_results: dict[str, PipelineResult] = field(default_factory=dict)
    terminated_by: dict[str, int] = field(default_factory=dict)


@dataclass
class Stage:
    name: str
    request: Signature
    target: SlimTarget = field(default_factory=lambda: SlimTarget())
    pipelines: List[Signature] = field(default_factory=list)
    terminators: List[Signature] = field(default_factory=list)
    callbacks: List[Signature] = field(default_factory=list)
    progress: StageResult = field(default_factory=lambda: StageResult())

    @classmethod
    def from_dict(cls, data: dict) -> "Stage":
        """
        Creates a new Stage object from a dictionary.
        """

        # This is a workaround for a bug between dacite and mongoengine.
        # To reproduce the bug:
        # 1. Comment out the next line.
        # 2. Create a Series object with at least one Stage and save it to the database.
        # 3. Load the object from the database and call the to_dict method.
        data = loads(dumps(data))

        # TODO: Move to utils as it is used in multiple places
        def signature_hook(s: dict):
            return signature(s["task"], args=s["args"], kwargs=s["kwargs"])

        return from_dict(
            data_class=cls,
            data=data,
            config=Config(type_hooks={Signature: signature_hook}),
        )
