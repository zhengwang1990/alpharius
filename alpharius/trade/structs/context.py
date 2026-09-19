import datetime
import functools

import numpy as np
import pandas as pd

from ..common import DAYS_IN_A_MONTH, MARKET_OPEN
from ..enums import Mode

_MARKET_OPEN_SECONDS = MARKET_OPEN.hour * 3600 + MARKET_OPEN.minute * 60 + MARKET_OPEN.second
# The market open bar is near the start of the lookback (after premarket), so scan the index in chunks.
_MARKET_OPEN_SCAN_CHUNK = 96


@functools.lru_cache(maxsize=1024)
def _market_open_time(date: datetime.date, tz: datetime.tzinfo | None) -> pd.Timestamp:
    open_time = pd.Timestamp(datetime.datetime.combine(date, MARKET_OPEN))
    return open_time.tz_localize(tz) if tz is not None else open_time


def _find_market_open_index(index: pd.DatetimeIndex) -> int | None:
    """Returns the position of the first bar whose time of day is not before market open."""
    n = len(index)
    if n == 0:
        return None
    first_date = index[0].date()
    if index.is_monotonic_increasing and index[-1].date() == first_date:
        # A sorted index within one day: time of day is non-decreasing, so binary search works.
        i = int(index.searchsorted(_market_open_time(first_date, index.tz)))
        return i if i < n else None
    for start in range(0, n, _MARKET_OPEN_SCAN_CHUNK):
        chunk = index[start : start + _MARKET_OPEN_SCAN_CHUNK]
        seconds = np.asarray(chunk.hour * 3600 + chunk.minute * 60 + chunk.second)
        hits = np.flatnonzero(seconds >= _MARKET_OPEN_SECONDS)
        if len(hits) > 0:
            return start + int(hits[0])
    return None


class Context:
    def __init__(
        self,
        symbol: str,
        current_time: pd.Timestamp,
        current_price: float,
        interday_lookback: pd.DataFrame,
        intraday_lookback: pd.DataFrame | None,
        mode: Mode | None = None,
    ) -> None:
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
    def market_open_index(self) -> int | None:
        if self._market_open_index is not None:
            return self._market_open_index
        self._market_open_index = _find_market_open_index(self.intraday_lookback.index)
        return self._market_open_index

    @property
    def today_open(self) -> float | None:
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
