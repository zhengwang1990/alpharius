from typing import Optional

import numpy as np
import pandas as pd

from ..common import DAYS_IN_A_MONTH, MARKET_OPEN
from ..enums import Mode


class Context:
    def __init__(
        self,
        symbol: str,
        current_time: pd.Timestamp,
        current_price: float,
        interday_lookback: pd.DataFrame,
        intraday_lookback: Optional[pd.DataFrame],
        mode: Optional['Mode'] = None,
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
    def market_open_index(self) -> Optional[int]:
        if self._market_open_index is not None:
            return self._market_open_index
        for i in range(len(self.intraday_lookback)):
            if self.intraday_lookback.index[i].time() >= MARKET_OPEN:
                self._market_open_index = i
                return i
        return None

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
