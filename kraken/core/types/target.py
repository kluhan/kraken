from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from mongoengine.document import Document, EmbeddedDocument, DynamicEmbeddedDocument
from mongoengine.fields import (
    DictField,
    ListField,
    IntField,
    EmbeddedDocumentField,
    MapField,
    DateTimeField,
    EmbeddedDocumentListField,
    DynamicField,
    ReferenceField,
)


# TODO: Add documentation
class HistoricValue(EmbeddedDocument):
    value: any = DynamicField()  # type: ignore
    timestamp: datetime = DateTimeField(default=datetime.now)  # type: ignore


# TODO: Add documentation
class Statistic(EmbeddedDocument):
    # Current values, which may be aggregated versions of the corresponding historic values.
    cost: int = IntField(default=0)  # type: ignore
    gain: int = IntField(default=0)  # type: ignore
    weight: int = IntField(default=1)  # type: ignore
    metrics: dict[str, int] = MapField(IntField())  # type: ignore
    result: dict[str, int] = MapField(IntField())  # type: ignore

    # TODO: Implement
    rolling_metrics: dict[str, int] = MapField(IntField())  # type: ignore

    # Historic values used for aggregation and logging purposes.
    cost_history: list[HistoricValue] = EmbeddedDocumentListField(HistoricValue)  # type: ignore
    gain_history: list[HistoricValue] = EmbeddedDocumentListField(HistoricValue)  # type: ignore
    weight_history: list[HistoricValue] = EmbeddedDocumentListField(HistoricValue)  # type: ignore
    metrics_history: dict[str, list[HistoricValue]] = MapField(EmbeddedDocumentListField(HistoricValue))  # type: ignore

    # History of each stage-result.
    result_history: list[HistoricValue] = EmbeddedDocumentListField(HistoricValue)

    def latest(self):
        return {
            "cost": self.cost,
            "gain": self.gain,
            "weight": self.weight,
            "metrics": self.metrics,
            "result": self.result,
        }


# TODO: Add documentation
class Target(Document):
    tags: list[str] = ListField(default=list)  # type: ignore
    kwargs: dict[str, any] = DictField(default=dict, unique=True)  # type: ignore
    discovered_by: "Crawl" = ReferenceField("Crawl", default=None)  # type: ignore
    discovered_at: datetime = DateTimeField(default=datetime.now)  # type: ignore

    # Each Statistic contains the data for one Stage of a Series and can be accessed
    # via the id of the Series and the name of the Stage.
    statistics: dict[str, dict[str, Statistic]] = MapField(MapField(EmbeddedDocumentField(Statistic)))  # type: ignore

    # Lists containing the timestamps at which the Target was queued or processed.
    # Each list contains the timestamps for one Series and can be accessed via
    # the id of the Series.
    queued: dict[str, list] = MapField(ListField(default=list))  # type: ignore
    processed: dict[str, list] = MapField(ListField(default=list))  # type: ignore

    def latest_statistics(self, series_id: str, stage_name: Optional[str] = None):
        if stage_name:
            statistic = self.statistics.get(series_id, {}).get(stage_name, None)
            return statistic.latest() if statistic else None

        else:
            series = self.statistics.get(series_id, {})
            latest_stage_statistics = {
                stage_name: statistic.latest()
                for stage_name, statistic in series.items()
                if statistic
            }
            return latest_stage_statistics

    def slim(self):
        return SlimTarget(
            id=str(self.pk),
            tags=[tag for tag in self.tags],
            kwargs={k: v for (k, v) in self.kwargs.items()},
        )


# TODO: Add documentation
@dataclass
class SlimTarget:
    id: str = field(default="")
    tags: list[str] = field(default_factory=list)
    kwargs: dict = field(default_factory=dict)
