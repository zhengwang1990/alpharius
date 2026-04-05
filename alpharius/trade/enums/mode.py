from enum import Enum


class Mode(Enum):
    BACKTEST = 1
    TRADE = 2

    def __str__(self):
        return self.name
