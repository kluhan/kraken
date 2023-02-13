from dataclasses import dataclass


@dataclass
class RequestResult:
    result: dict
    subsequent_kwargs: dict | None = None
    batch: bool = False
    gain: int = 1
    cost: int = 1
    target_not_found: bool = False
    target_exhausted: bool | None = None
