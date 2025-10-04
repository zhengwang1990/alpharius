import collections
import datetime
import functools
import logging
import os

from enum import Enum
from typing import List, Optional

import numpy as np
import pandas as pd


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
CACHE_DIR = os.path.join(BASE_DIR, 'cache')
OUTPUT_DIR = os.path.join(BASE_DIR, 'outputs')
DAYS_IN_A_WEEK = 5
DAYS_IN_A_MONTH = 20
CALENDAR_DAYS_IN_A_MONTH = 35
CALENDAR_DAYS_IN_A_YEAR = 365
DAYS_IN_A_QUARTER = 60
DAYS_IN_A_YEAR = 250
MARKET_OPEN = datetime.time(9, 30)
MARKET_CLOSE = datetime.time(16, 0)
SHORT_RESERVE_RATIO = 1
INTERDAY_LOOKBACK_LOAD = CALENDAR_DAYS_IN_A_YEAR
BID_ASK_SPREAD = 0.001


class ActionType(Enum):
    BUY_TO_OPEN = 1
    SELL_TO_OPEN = 2
    BUY_TO_CLOSE = 3
    SELL_TO_CLOSE = 4

    def __str__(self):
        return self.name


class Mode(Enum):
    BACKTEST = 1
    TRADE = 2

    def __str__(self):
        return self.name


class TradingFrequency(Enum):
    FIVE_MIN = 1
    CLOSE_TO_CLOSE = 2
    CLOSE_TO_OPEN = 3

    def __str__(self):
        return self.name


class PositionStatus(Enum):
    ACTIVE = 1
    PENDING = 2
    CLOSED = 3

    def __str__(self):
        return self.name


ProcessorAction = collections.namedtuple('ProcessorAction', ['symbol', 'type', 'percent'])
Action = collections.namedtuple('Action', ['symbol', 'type', 'percent', 'price', 'processor'])
Position = collections.namedtuple('Position', ['symbol', 'qty', 'entry_price', 'entry_time', 'entry_portion'])


def timestamp_to_index(index: pd.Index, timestamp: pd.Timestamp) -> Optional[int]:
    pd_timestamp = timestamp.timestamp()
    left, right = 0, len(index) - 1
    while left <= right:
        mid = (left + right) // 2
        mid_timestamp = index[mid].timestamp()
        if mid_timestamp == pd_timestamp:
            return mid
        elif mid_timestamp < pd_timestamp:
            left = mid + 1
        else:
            right = mid - 1
    return None


def get_unique_actions(actions: List[Action]) -> List[Action]:
    action_sets = set([(action.symbol, action.type) for action in actions])
    unique_actions = []
    for unique_action in action_sets:
        similar_actions = [action for action in actions if (action.symbol, action.type) == unique_action]
        action = similar_actions[0]
        for i in range(1, len(similar_actions)):
            if similar_actions[i].percent > action.percent:
                action = similar_actions[i]
        unique_actions.append(action)
    unique_actions.sort(key=lambda action: action.symbol)
    return unique_actions


@functools.lru_cache(maxsize=None)
def logging_config(logging_file=None, detail=True, name=None, timezone=None) -> logging.Logger:
    """Configuration for logging."""
    logger = logging.getLogger(name=name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    if detail:
        formatter = logging.Formatter(
            '[%(levelname)s] [%(asctime)s] [%(filename)s:%(lineno)d] %(message)s')
    else:
        formatter = logging.Formatter('%(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)
    stream_handler.setFormatter(formatter)
    if timezone:
        stream_handler.formatter.converter = lambda *args: datetime.datetime.now(tz=timezone).timetuple()
    logger.addHandler(stream_handler)
    if logging_file:
        file_handler = logging.FileHandler(logging_file)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        if timezone:
            file_handler.formatter.converter = lambda *args: datetime.datetime.now(tz=timezone).timetuple()
        logger.addHandler(file_handler)
    return logger


def get_header(title):
    header_left = '== [ %s ] ' % (title,)
    return header_left + '=' * (80 - len(header_left))


class Context:

    def __init__(self,
                 symbol: str,
                 current_time: pd.Timestamp,
                 current_price: float,
                 interday_lookback: pd.DataFrame,
                 intraday_lookback: Optional[pd.DataFrame],
                 mode: Optional[Mode] = None) -> None:
        self.symbol = symbol
        self.current_time = current_time
        self.current_price = current_price
        self.interday_lookback = interday_lookback
        self.intraday_lookback = intraday_lookback
        self.mode = mode
        self._market_open_index = None

    @property
    def prev_day_close(self) -> float:
        return self.interday_lookback['Close'].iloc[-1]

    @property
    def market_open_index(self) -> Optional[int]:
        if self._market_open_index is not None:
            return self._market_open_index
        for i in range(len(self.intraday_lookback)):
            if self.intraday_lookback.index[i].time() >= MARKET_OPEN:
                self._market_open_index = i
                return i
        return None

    @property
    def today_open(self) -> float:
        p = self.market_open_index
        return self.intraday_lookback['Open'].iloc[p] if p is not None else None

    @property
    def h2l_avg(self) -> float:
        key = 'h2l_avg'
        if key not in self.interday_lookback.attrs:
            interday_highs = self.interday_lookback['High'][-DAYS_IN_A_MONTH:]
            interday_lows = self.interday_lookback['Low'][-DAYS_IN_A_MONTH:]
            h2l = [l / h - 1 for h, l in zip(interday_highs, interday_lows)]
            h2l_avg = np.average(h2l)
            self.interday_lookback.attrs[key] = h2l_avg
        return self.interday_lookback.attrs[key]

    @property
    def h2l_std(self) -> float:
        key = 'h2l_std'
        if key not in self.interday_lookback.attrs:
            interday_highs = self.interday_lookback['High'][-DAYS_IN_A_MONTH:]
            interday_lows = self.interday_lookback['Low'][-DAYS_IN_A_MONTH:]
            h2l = [l / h - 1 for h, l in zip(interday_highs, interday_lows)]
            h2l_std = float(np.std(h2l))
            self.interday_lookback.attrs[key] = h2l_std
        return self.interday_lookback.attrs[key]

    @property
    def l2h_avg(self) -> float:
        key = 'l2h_avg'
        if key not in self.interday_lookback.attrs:
            interday_highs = self.interday_lookback['High'][-DAYS_IN_A_MONTH:]
            interday_lows = self.interday_lookback['Low'][-DAYS_IN_A_MONTH:]
            l2h = [h / l - 1 for h, l in zip(interday_highs, interday_lows)]
            l2h_avg = np.average(l2h)
            self.interday_lookback.attrs[key] = l2h_avg
        return self.interday_lookback.attrs[key]
