from typing import NamedTuple

from ..enums import ActionType


class Action(NamedTuple):
    symbol: str
    type: ActionType
    percent: float
    price: float
    processor: str
