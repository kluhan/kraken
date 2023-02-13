from types import NoneType
from typing import Union

from dataclasses import dataclass, field

from kraken.utils.pipeline import combine_dicts_by_addition


@dataclass
class PipelineResult:
    weight: Union[int, NoneType] = field(default=None)
    statistics: Union[dict, NoneType] = field(default_factory=dict)
    metrics: Union[dict, NoneType] = field(default_factory=dict)

    def __getitem__(self, item):
        return getattr(self, item)

    # TODO: Add tests
    def __add__(self, other):

        return PipelineResult(
            statistics=combine_dicts_by_addition(self.statistics, other.statistics),
            metrics=combine_dicts_by_addition(self.metrics, other.metrics),
            weight=(self.weight or 0) + (other.weight or 0)
            if self.weight or other.weight
            else None,
        )

    # Allows to add a PipelineResult to a None value -> None + PR = PR
    def __radd__(self, other):
        if other is None:
            return self
        else:
            return self.__add__(other)
