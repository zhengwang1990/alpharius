from enum import Enum


class TradingFrequency(Enum):
    FIVE_MIN = 1
    CLOSE_TO_CLOSE = 2
    CLOSE_TO_OPEN = 3

    def __str__(self):
        return self.name
