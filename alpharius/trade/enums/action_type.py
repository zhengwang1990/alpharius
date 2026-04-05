from enum import Enum


class ActionType(Enum):
    BUY_TO_OPEN = 1
    SELL_TO_OPEN = 2
    BUY_TO_CLOSE = 3
    SELL_TO_CLOSE = 4

    def __str__(self):
        return self.name
