from typing import NamedTuple

from ..enums import ActionType


class ProcessorAction(NamedTuple):
    symbol: str
    type: ActionType
    percent: float
