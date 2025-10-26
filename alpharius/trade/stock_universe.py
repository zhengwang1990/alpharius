import abc
import datetime
import functools
import inspect
import json
import os
from typing import List

import alpaca.trading as trading
import numpy as np
import pandas as pd

from alpharius.data import DataClient, load_interday_dataset
from alpharius.utils import ALPACA_API_KEY_ENV, ALPACA_SECRET_KEY_ENV, TIME_ZONE, hash_str, get_all_symbols
from .common import (
    DAYS_IN_A_QUARTER, DAYS_IN_A_MONTH, CACHE_DIR, timestamp_to_index,
)
from .constants import COMPANY_SYMBOLS

_STOCK_UNIVERSE_CACHE_ROOT = os.path.join(CACHE_DIR, 'stock_universe')


class BaseStockUniverse:
    """Stock universe returns all tradable symbols."""

    def __init__(self,
                 lookback_start_date: pd.Timestamp,
                 lookback_end_date: pd.Timestamp) -> None:
        api_key = os.environ[ALPACA_API_KEY_ENV]
        secret_key = os.environ[ALPACA_SECRET_KEY_ENV]
        trading_client = trading.TradingClient(api_key, secret_key)
        calendar = trading_client.get_calendar(
            filters=trading.GetCalendarRequest(
                start=lookback_start_date.date(),
                end=lookback_end_date.date(),
            ))
        self._lookback_start_date = lookback_start_date
        self._lookback_end_date = lookback_end_date
        self._market_dates = [day.date for day in calendar]
        self._cache_dir = None

    @functools.lru_cache(maxsize=100)
    def get_prev_day(self, view_time: pd.Timestamp) -> pd.Timestamp:
        prev_day = view_time.date() - datetime.timedelta(days=1)
        while prev_day not in self._market_dates:
            prev_day -= datetime.timedelta(days=1)
            if prev_day < self._market_dates[0]:
                raise ValueError(f'{view_time} is too early')
        return pd.Timestamp(prev_day).tz_localize(TIME_ZONE)

    def get_stock_universe(self, view_time: pd.Timestamp) -> List[str]:
        return get_all_symbols()


class CachedStockUniverse(BaseStockUniverse):
    """Cache mixin for stock universe."""

    def get_source(self) -> str:

        def get_nested(cls) -> str:
            content = inspect.getsource(cls)
            for base_cls in cls.__bases__:
                if base_cls.__module__ == cls.__module__:
                    content += get_nested(base_cls)
            return content

        return get_nested(self.__class__)

    def get_cache_dir(self) -> str:
        if self._cache_dir:
            return self._cache_dir
        content = self.get_source()
        class_name = self.__class__.__name__
        for attr in sorted(self.__dict__.keys()):
            value = self.__dict__[attr]
            if isinstance(value, CachedStockUniverse):
                value = value.get_cache_dir()
            if isinstance(value, (set, list)):
                value = sorted(value)
            if isinstance(value, dict):
                value = sorted(value.keys())
            value = repr(value)
            content += f'\n{attr}={value}'
        cache_name = class_name + '_' + hash_str(content)
        self._cache_dir = os.path.join(_STOCK_UNIVERSE_CACHE_ROOT, cache_name)
        os.makedirs(self._cache_dir, exist_ok=True)
        return self._cache_dir

    def get_stock_universe(self, view_time: pd.Timestamp) -> List[str]:
        cache_file = os.path.join(self.get_cache_dir(),
                                  view_time.strftime('%F') + '.json')
        if os.path.isfile(cache_file):
            with open(cache_file, 'r') as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    pass
        stock_universe = self.get_stock_universe_impl(view_time)
        with open(cache_file, 'w') as f:
            json.dump(stock_universe, f)
        return stock_universe

    @abc.abstractmethod
    def get_stock_universe_impl(self, view_time: pd.Timestamp) -> List[str]:
        raise NotImplementedError()


class DataBasedStockUniverse(BaseStockUniverse):

    def __init__(self,
                 lookback_start_date: pd.Timestamp,
                 lookback_end_date: pd.Timestamp,
                 data_client: DataClient) -> None:
        super().__init__(lookback_start_date, lookback_end_date)
        self._historical_data = load_interday_dataset(get_all_symbols(),
                                                      lookback_start_date,
                                                      lookback_end_date,
                                                      data_client)


class TopVolumeUniverse(DataBasedStockUniverse, CachedStockUniverse):

    def __init__(self,
                 lookback_start_date: pd.Timestamp,
                 lookback_end_date: pd.Timestamp,
                 data_client: DataClient,
                 num_stocks: int = 100) -> None:
        super().__init__(lookback_start_date, lookback_end_date, data_client)
        self._company_symbols = set(COMPANY_SYMBOLS)
        self._num_stocks = num_stocks

    def _get_dollar_volume(self, symbol: str, prev_day_ind: int) -> float:
        hist = self._historical_data[symbol]
        pv = [hist['Close'].iloc[i] * hist['Volume'].iloc[i]
              for i in range(max(prev_day_ind - DAYS_IN_A_MONTH + 1, 0), prev_day_ind + 1)]
        return np.average(pv) if pv else 0

    def get_stock_universe_impl(self, view_time: pd.Timestamp) -> List[str]:
        prev_day = self.get_prev_day(view_time)
        dollar_volumes = []
        for symbol, hist in self._historical_data.items():
            if symbol not in self._company_symbols:
                continue
            if prev_day not in hist.index:
                continue
            prev_day_ind = timestamp_to_index(hist.index, prev_day)
            if prev_day_ind < DAYS_IN_A_MONTH:
                continue
            prev_close = hist['Close'].iloc[prev_day_ind]
            if prev_close < 5:
                continue
            dollar_volumes.append((symbol, self._get_dollar_volume(symbol, prev_day_ind)))
        dollar_volumes.sort(key=lambda s: s[1], reverse=True)
        return [s[0] for s in dollar_volumes[:self._num_stocks]]


class IntradayVolatilityStockUniverse(DataBasedStockUniverse, CachedStockUniverse):

    def __init__(self,
                 lookback_start_date: pd.Timestamp,
                 lookback_end_date: pd.Timestamp,
                 data_client: DataClient,
                 num_stocks: int = 50,
                 num_top_volume: int = 500):
        super().__init__(lookback_start_date, lookback_end_date, data_client)
        self._top_volume = TopVolumeUniverse(lookback_start_date, lookback_end_date, data_client, num_top_volume)
        self._company_symbols = set(COMPANY_SYMBOLS)
        self._num_stocks = num_stocks

    def _get_intraday_range(self, symbol: str, prev_day_ind: int) -> float:
        hist = self._historical_data[symbol]
        res = []
        for i in range(max(prev_day_ind - DAYS_IN_A_MONTH + 1, 1), prev_day_ind + 1):
            h = hist['High'].iloc[i]
            l = hist['Low'].iloc[i]
            c = hist['Close'].iloc[i - 1]
            res.append((h - l) / c)
        return np.average(res) if res else 0

    def get_stock_universe_impl(self, view_time: pd.Timestamp) -> List[str]:
        prev_day = self.get_prev_day(view_time)
        intraday_volatility_list = []
        top_volume_symbols = set(self._top_volume.get_stock_universe(view_time))
        for symbol, hist in self._historical_data.items():
            if symbol not in self._company_symbols:
                continue
            if symbol not in top_volume_symbols:
                continue
            if prev_day not in hist.index:
                continue
            prev_day_ind = timestamp_to_index(hist.index, prev_day)
            if prev_day_ind < DAYS_IN_A_MONTH:
                continue
            prev_close = hist['Close'].iloc[prev_day_ind]
            start_ind = max(prev_day_ind - DAYS_IN_A_QUARTER, 0)
            if prev_close < 0.4 * np.max(hist['Close'][start_ind:prev_day_ind + 1]):
                continue
            intraday_volatility = self._get_intraday_range(symbol, prev_day_ind)
            intraday_volatility_list.append((symbol, intraday_volatility))

        intraday_volatility_list.sort(key=lambda s: s[1], reverse=True)
        return [s[0] for s in intraday_volatility_list[:self._num_stocks]]


class L2hVolatilityStockUniverse(DataBasedStockUniverse, CachedStockUniverse):

    def __init__(self,
                 lookback_start_date: pd.Timestamp,
                 lookback_end_date: pd.Timestamp,
                 data_client: DataClient,
                 num_top_volume: int = 2000):
        super().__init__(lookback_start_date, lookback_end_date, data_client)
        self._top_volume = TopVolumeUniverse(lookback_start_date, lookback_end_date, data_client, num_top_volume)
        self._company_symbols = set(COMPANY_SYMBOLS)

    def _get_l2h_avg(self, symbol: str, prev_day_ind: int) -> float:
        hist = self._historical_data[symbol]
        res = []
        for i in range(max(prev_day_ind - DAYS_IN_A_MONTH + 1, 1), prev_day_ind + 1):
            h = hist['High'].iloc[i]
            l = hist['Low'].iloc[i]
            res.append(l / h - 1)
        return np.average(res) if res else 0

    def get_stock_universe_impl(self, view_time: pd.Timestamp) -> List[str]:
        prev_day = self.get_prev_day(view_time)
        top_volume_symbols = set(self._top_volume.get_stock_universe(view_time))
        symbols = []
        for symbol, hist in self._historical_data.items():
            if symbol not in self._company_symbols:
                continue
            if symbol not in top_volume_symbols:
                continue
            if prev_day not in hist.index:
                continue
            prev_day_ind = timestamp_to_index(hist.index, prev_day)
            if prev_day_ind < DAYS_IN_A_MONTH:
                continue
            prev_close = hist['Close'].iloc[prev_day_ind]
            start_ind = max(prev_day_ind - DAYS_IN_A_QUARTER, 0)
            if prev_close < 0.4 * np.max(hist['Close'][start_ind:prev_day_ind + 1]):
                continue
            l2h_avg = self._get_l2h_avg(symbol, prev_day_ind)
            if l2h_avg < -0.05:
                symbols.append((symbol, l2h_avg))
        symbols.sort(key=lambda x: x[1])
        symbols = symbols[:100]
        return [s[0] for s in symbols]
