"""Utility functions shared by multiple modules."""

import base64
import datetime
import functools
import hashlib
import math
import os
import re
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

import alpaca.trading as trading
import numpy as np
import pandas as pd

TIME_ZONE = ZoneInfo('America/New_York')
ALPACA_API_KEY_ENV = 'APCA_API_KEY_ID'
ALPACA_SECRET_KEY_ENV = 'APCA_API_SECRET_KEY'


@dataclass
class Transaction:
    symbol: str
    is_long: bool
    processor: Optional[str]
    entry_price: float
    exit_price: Optional[float]
    entry_time: pd.Timestamp
    exit_time: Optional[pd.Timestamp]
    qty: float
    gl: Optional[float]
    gl_pct: Optional[float]
    slippage: Optional[float]
    slippage_pct: Optional[float]

    def __post_init__(self):
        # Type conversion to convert np.float32 types to float so that it is compatible with DB
        for k, v in self.__dict__.items():
            if isinstance(v, np.float32):
                self.__dict__[k] = float(v)


def get_colored_value(value: str, color: str, with_arrow: bool = False) -> str:
    """Returns HTML of a value with color annotations.

    params:
      value: The string value to be colored.
      color: The color applied to the value. Can be green or red.
      with_arrow: If an arrow icon is attached to the front of the value.
    """
    arrow = ''
    if with_arrow:
        if color == 'green':
            arrow = '<i class="uil uil-arrow-up"></i>'
        else:
            arrow = '<i class="uil uil-arrow-down"></i>'
    return f'<span style="color:{color};">{arrow}{value}</span>'


def get_signed_percentage(value: float, with_arrow: bool = False) -> str:
    """Returns HTML of a percentage value with sign and color annotations.

    params:
      value: The float value to be annotated with sign and color.
      with_arrow: If an arrow icon is attached to the front of the value.
    """
    color = 'green' if value >= 0 else 'red'
    return get_colored_value(f'{value * 100:+.2f}%', color, with_arrow)


def get_current_time() -> pd.Timestamp:
    """Gets current time of NY time zone."""
    # Use time.time() instead of 'now' so that it can be mocked in test.
    return pd.to_datetime(time.time(), utc=True, unit='s').tz_convert(TIME_ZONE)


def get_today() -> pd.Timestamp:
    """Gets a datetime object of today at 00:00 NY time."""
    # Mocking time.time() will change the behavior of the method.
    return pd.to_datetime(pd.Timestamp.combine(get_current_time().date(),
                                               datetime.time(0, 0))).tz_localize(TIME_ZONE)


def get_latest_day() -> datetime.date:
    """Gets the latest day in regular sense.

    If the time is before pre-market open it returns previous day.
    """
    latest_day = get_current_time()
    if latest_day.time() < datetime.time(4, 0):
        latest_day -= datetime.timedelta(days=1)
    return latest_day.date()


def compute_risks(
        values: List[float],
        market_values: List[float],
) -> Tuple[Optional[float], Optional[float], float]:
    """Computes alpha, beta and sharpe ratio risk factors.

    params:
      values: The values of the target.
      market_values: The market value. Typically, this is S&P 500 values.

    returns:
      A tuple consists of alpha, beta and sharpe ratio, in order.
    """
    profits = [values[k + 1] / values[k] -
               1 for k in range(len(values) - 1)]
    r = np.average(profits)
    std = np.std(profits)
    s = r / std * np.sqrt(252) if std > 0 else math.nan
    a, b = math.nan, math.nan
    if len(values) == len(market_values) and len(values) > 2:
        market_profits = [market_values[k + 1] / market_values[k] - 1
                          for k in range(len(market_values) - 1)]
        mr = np.average(market_profits)
        mvar = np.var(market_profits)
        b = np.cov(market_profits, profits, bias=True)[0, 1] / mvar
        a = (r - b * mr) * np.sqrt(252)
    return a, b, s


def compute_drawdown(values: List[float]) -> Tuple[float, int, int]:
    """Computes drawdown of the target."""
    h = values[0]
    ci = 0
    d = 0
    hi, li = 0, 0
    for i, v in enumerate(values):
        new_d = v / h - 1
        if new_d <= d:
            d = new_d
            hi = ci
            li = i
        elif v > h:
            h = v
            ci = i
    return d, hi, li


def construct_charts_link(symbol: str, date: str) -> str:
    """Constructs link to charts page for the given symbol on a given day."""
    start_date = (pd.to_datetime(date) - datetime.timedelta(days=92)).strftime('%F')
    return f'charts?date={date}&start_date={start_date}&end_date={date}&symbol={symbol}'


def compute_bernoulli_ci95(p: float, n: int) -> float:
    """Gets 95% confidence interval for Bernoulli sampling."""
    z = 1.96
    return z * math.sqrt(p * (1 - p) / n)


def get_trading_client() -> trading.TradingClient:
    api_key = os.environ[ALPACA_API_KEY_ENV]
    secret_key = os.environ[ALPACA_SECRET_KEY_ENV]
    return trading.TradingClient(api_key, secret_key)


@functools.lru_cache()
def get_all_symbols() -> List[str]:
    """Gets all symbols that can be traded."""
    api_key = os.environ[ALPACA_API_KEY_ENV]
    secret_key = os.environ[ALPACA_SECRET_KEY_ENV]
    trading_client = trading.TradingClient(api_key, secret_key)
    assets = trading_client.get_all_assets(
        filter=trading.GetAssetsRequest(status=trading.AssetStatus.ACTIVE,
                                        asset_class=trading.AssetClass.US_EQUITY,
                                        attributes='options_enabled'),
    )
    symbols = [asset.symbol for asset in assets if
               asset.easy_to_borrow and asset.fractionable and asset.marginable
               and asset.tradable and asset.shortable
               or asset.symbol in ['QQQ', 'SPY', 'TQQQ', 'SQQQ', 'UCO', 'NUGT']]
    symbols = [symbol for symbol in symbols if symbol.isalpha()]
    return symbols


def hash_str(value: str) -> str:
    """Hashes a string into a short representation."""
    d = hashlib.md5(value.encode('utf-8')).digest()
    b = base64.b64encode(d).decode('utf-8')
    res = re.sub(r'[^a-zA-Z\d]', '', b)
    return res[:10]
