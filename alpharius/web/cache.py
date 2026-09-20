"""In-memory cache for web data that stops changing once the market is closed."""

import collections
import copy
import datetime
import functools
import threading
from collections.abc import Callable
from typing import TypeVar, override

import pandas as pd

from alpharius.db import Db
from alpharius.utils import get_current_time

# From these NY times on a weekday until 03:00 the next morning, and all day on weekends, the data no longer changes.
# Transactions, orders and logs are final once trading is done, but portfolio value, market prices and charts keep
# moving with after-hours trading, which ends at 20:00.
AFTER_TRADING = datetime.time(17, 0)
AFTER_HOURS = datetime.time(21, 0)
_NEXT_DAY_START = datetime.time(3, 0)
_MAX_ENTRIES = 512

T = TypeVar('T')

_cache: collections.OrderedDict = collections.OrderedDict()
_lock = threading.Lock()


def get_stable_day(now: pd.Timestamp, stable_from: datetime.time) -> datetime.date | None:
    """Gets the trading day that the data at `now` belongs to, or None while the data can still change.

    The stretch from Friday evening to Monday 03:00 counts as one and the same Friday.
    """
    day = now.date()
    if now.time() < _NEXT_DAY_START:
        # Still the evening (or weekend) of the day before
        stable = True
        day -= datetime.timedelta(days=1)
    elif day.weekday() >= 5:
        stable = True
    else:
        stable = now.time() >= stable_from
    if not stable:
        return None
    while day.weekday() >= 5:
        day -= datetime.timedelta(days=1)
    return day


def clear_cache() -> None:
    with _lock:
        _cache.clear()


def stable_cache(stable_from: datetime.time) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Caches the results of a method, by its arguments, for as long as its data is stable (see get_stable_day).

    At other times the call goes straight through. Results are copied, so callers are free to modify them.
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs) -> T:
            day = get_stable_day(get_current_time(), stable_from)
            if day is None:
                return func(self, *args, **kwargs)
            key = (func.__qualname__, args, tuple(sorted(kwargs.items())))
            with _lock:
                entry = _cache.get(key)
                if entry is not None and entry[0] == day:
                    _cache.move_to_end(key)
                    return copy.deepcopy(entry[1])
            result = func(self, *args, **kwargs)
            with _lock:
                _cache[key] = (day, result)
                _cache.move_to_end(key)
                while len(_cache) > _MAX_ENTRIES:
                    _cache.popitem(last=False)
            return copy.deepcopy(result)

        return wrapper

    return decorator


class CachedDb(Db):
    """Db for the web pages, which reads from memory while the data cannot change."""

    @override
    @stable_cache(AFTER_TRADING)
    def list_transactions(self, *args, **kwargs):
        return super().list_transactions(*args, **kwargs)

    @override
    @stable_cache(AFTER_TRADING)
    def get_transaction_count(self, *args, **kwargs):
        return super().get_transaction_count(*args, **kwargs)

    @override
    @stable_cache(AFTER_TRADING)
    def list_aggregations(self, *args, **kwargs):
        return super().list_aggregations(*args, **kwargs)

    @override
    @stable_cache(AFTER_TRADING)
    def list_log_dates(self, *args, **kwargs):
        return super().list_log_dates(*args, **kwargs)

    @override
    @stable_cache(AFTER_TRADING)
    def get_logs(self, *args, **kwargs):
        return super().get_logs(*args, **kwargs)

    @override
    @stable_cache(AFTER_HOURS)
    def get_backtest(self, *args, **kwargs):
        return super().get_backtest(*args, **kwargs)
