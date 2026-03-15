import abc
import os
from enum import Enum
from typing import Dict, List

import pandas as pd

from alpharius.utils import TIME_ZONE

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
CACHE_DIR = os.path.join(BASE_DIR, 'cache')

DATA_COLUMNS = ['Open', 'High', 'Low', 'Close', 'Volume']


class TimeInterval(Enum):
    FIVE_MIN = 1
    HOUR = 2
    DAY = 3

    def __str__(self):
        return self.name


class DataError(Exception):
    """Error in data loading."""


class DataClient(abc.ABC):
    def get_daily(self, symbol: str, day: pd.Timestamp, time_interval: TimeInterval) -> pd.DataFrame:
        """Loads data of a given day."""
        start_time = pd.Timestamp(year=day.year, month=day.month, day=day.day, hour=0, minute=0).tz_localize(
            tz=TIME_ZONE
        )
        end_time = pd.Timestamp(year=day.year, month=day.month, day=day.day, hour=23, minute=59).tz_localize(
            tz=TIME_ZONE
        )
        return self.get_data(symbol, start_time, end_time, time_interval)

    @abc.abstractmethod
    def get_data(
        self, symbol: str, start_time: pd.Timestamp, end_time: pd.Timestamp, time_interval: TimeInterval
    ) -> pd.DataFrame:
        """Loads data with specified start and end time.

        start_time and end_time are inclusive.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_last_trades(self, symbols: List[str]) -> Dict[str, float]:
        """Gets the last trade prices of a list of symbols."""
        raise NotImplementedError()

    def __to_hash__(self) -> str:
        return self.__class__.__name__
