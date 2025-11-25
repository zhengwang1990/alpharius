import datetime
import functools
import os
import sys
from concurrent import futures
from typing import Dict, Callable, Iterable, List, Optional

import alpaca.trading as trading
import cachetools
import pandas as pd
import retrying
from tqdm import tqdm

from alpharius.utils import Transaction, TIME_ZONE, hash_str, get_trading_client
from .base import DataClient, CACHE_DIR, TimeInterval
from .fmp_client import FmpClient

_MAX_WORKERS = 10
_interday_dataset_cache = cachetools.LRUCache(maxsize=2)


@retrying.retry(stop_max_attempt_number=2,
                wait_exponential_multiplier=500,
                retry_on_exception=lambda e: isinstance(e, IOError))
def _load_cached_symbol(symbol: str,
                        cache_dir: str,
                        load_func: Callable[[str], pd.DataFrame]) -> pd.DataFrame:
    cache_file = os.path.join(cache_dir, f'history_{symbol}.pickle')
    if os.path.isfile(cache_file):
        hist = pd.read_pickle(cache_file)
    else:
        hist = load_func(symbol)
        hist.to_pickle(cache_file)
    return hist


def get_default_data_client():
    return FmpClient()


def load_interday_dataset(symbols: Iterable[str],
                          start_time: pd.Timestamp,
                          end_time: pd.Timestamp,
                          data_client: DataClient) -> Dict[str, pd.DataFrame]:
    if end_time.isoweekday() == 7:  # Improve cache hit
        end_time = end_time - datetime.timedelta(days=1)
    cache_key = hash_str(','.join(sorted(symbols)) + start_time.strftime('%F') + end_time.strftime('%F'))
    if cache_key in _interday_dataset_cache:
        return _interday_dataset_cache[cache_key]
    cache_dir = os.path.join(CACHE_DIR, str(TimeInterval.DAY),
                             start_time.strftime('%F'), end_time.strftime('%F'))
    os.makedirs(cache_dir, exist_ok=True)
    res = {}
    tasks = {}
    load_func = functools.partial(data_client.get_data,
                                  start_time=start_time,
                                  end_time=end_time,
                                  time_interval=TimeInterval.DAY)
    with futures.ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        for symbol in symbols:
            t = pool.submit(_load_cached_symbol, symbol, cache_dir, load_func)
            tasks[symbol] = t
        iterator = tqdm(tasks.items(), ncols=80) if sys.stdout.isatty() else tasks.items()
        for symbol, t in iterator:
            res[symbol] = t.result()
    _interday_dataset_cache[cache_key] = res
    return res


def load_intraday_dataset(symbols: Iterable[str],
                          day: pd.Timestamp,
                          data_client: DataClient) -> Dict[str, pd.DataFrame]:
    cache_dir = os.path.join(CACHE_DIR, str(TimeInterval.FIVE_MIN), day.strftime('%F'))
    os.makedirs(cache_dir, exist_ok=True)
    res = {}
    tasks = {}
    load_func = functools.partial(data_client.get_daily,
                                  day=day,
                                  time_interval=TimeInterval.FIVE_MIN)
    with futures.ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        for symbol in symbols:
            t = pool.submit(_load_cached_symbol, symbol, cache_dir, load_func)
            tasks[symbol] = t
        for symbol, t in tasks.items():
            res[symbol] = t.result()
    return res


@retrying.retry(stop_max_attempt_number=3,
                wait_exponential_multiplier=5000)
def get_transactions(start_date: Optional[str], data_client: DataClient) -> List[Transaction]:
    """Gets transactions from start date until today.

    params:
      start_date: The transactions after this date are fetched. Inclusive.
    """

    def round_time(t: pd.Timestamp) -> pd.Timestamp:
        if t.second > 30:
            t = t + datetime.timedelta(minutes=1)
        return pd.to_datetime(t.strftime('%F %H:%M:00%z'))

    def get_historical_price(symbol: str, t: pd.Timestamp) -> Optional[float]:
        df = data_client.get_data(symbol, t - datetime.timedelta(minutes=5), t, TimeInterval.FIVE_MIN)
        if not len(df) or pd.to_datetime(df.index[0]).timestamp() != t.timestamp() - 300:
            return None
        return df['Close'].iloc[0]

    trading_client = get_trading_client()

    chunk_size = 500
    orders = []
    start_time = (pd.to_datetime(start_date) - datetime.timedelta(days=7)).tz_localize(TIME_ZONE)
    end_time = pd.to_datetime('now', utc=True).tz_convert(TIME_ZONE)
    check_for_more_orders = True
    order_ids = set()
    while check_for_more_orders:
        order_chunk = trading_client.get_orders(
            filter=trading.GetOrdersRequest(
                status=trading.QueryOrderStatus.CLOSED,
                after=start_time,
                until=end_time,
                direction=trading.Sort.DESC,
                limit=chunk_size,
            )
        )
        for order in order_chunk:
            if order.id not in order_ids:
                orders.append(order)
                order_ids.add(order.id)
        if len(order_chunk) == chunk_size:
            end_time = orders[-3].submitted_at
        else:
            check_for_more_orders = False

    positions = trading_client.get_all_positions()
    orders_used = [False] * len(orders)
    position_symbols = set([position.symbol for position in positions])
    cut_time = pd.to_datetime(start_date).tz_localize(TIME_ZONE)
    transactions = []
    for i in range(len(orders)):
        order = orders[i]
        used = orders_used[i]
        if order.filled_at is None or used:
            continue
        filled_at = pd.to_datetime(order.filled_at.astimezone(TIME_ZONE))
        if filled_at < cut_time:
            break
        entry_time = round_time(filled_at)
        entry_price = float(order.filled_avg_price)
        qty = float(order.filled_qty)
        exit_time = None
        exit_price = None
        gl = None
        gl_pct = None
        slippage = None
        slippage_pct = None
        is_long = order.side == 'buy'
        if order.symbol in position_symbols:
            position_symbols.remove(order.symbol)
        else:
            for j in range(i + 1, len(orders)):
                prev_order = orders[j]
                if prev_order.filled_at is None or prev_order.symbol != order.symbol:
                    continue
                prev_filled_at = pd.to_datetime(prev_order.filled_at.astimezone(TIME_ZONE))
                if prev_filled_at < filled_at and prev_order.side != order.side:
                    exit_price = entry_price
                    entry_price = float(prev_order.filled_avg_price)
                    gl = (exit_price - entry_price) * qty
                    gl_pct = exit_price / entry_price - 1
                    is_long = prev_order.side == 'buy'
                    if not is_long:
                        gl *= -1
                        gl_pct *= -1
                    exit_time = entry_time
                    entry_time = round_time(prev_filled_at)
                    theory_entry_price = get_historical_price(order.symbol, entry_time)
                    theory_exit_price = get_historical_price(order.symbol, exit_time)
                    if theory_entry_price and theory_exit_price:
                        theory_gl_pct = theory_exit_price / theory_entry_price - 1
                        if not is_long:
                            theory_gl_pct *= -1
                        slippage_pct = gl_pct - theory_gl_pct
                        slippage = slippage_pct * qty * entry_price
                    orders_used[j] = True
                    break
        transactions.append(
            Transaction(order.symbol, is_long, None, entry_price, exit_price, entry_time,
                        exit_time, qty, gl, gl_pct, slippage, slippage_pct))
    return transactions
