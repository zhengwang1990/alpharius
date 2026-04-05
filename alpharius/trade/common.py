import datetime
import functools
import logging
import os
from typing import List, Optional

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


def get_unique_actions(actions):
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
        formatter = logging.Formatter('[%(levelname)s] [%(asctime)s] [%(filename)s:%(lineno)d] %(message)s')
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
