import datetime
import functools
import os
import sys
from collections.abc import Callable, Iterable
from concurrent import futures

import cachetools
import pandas as pd
import tenacity
from alpaca import trading
from tqdm import tqdm

from alpharius.utils import TIME_ZONE, Transaction, get_trading_client, hash_str

from .base import CACHE_DIR, DataClient, TimeInterval
from .fmp_client import FmpClient

_MAX_WORKERS = 10
_interday_dataset_cache: dict[str, dict[str, pd.DataFrame]] = cachetools.LRUCache(maxsize=2)


@tenacity.retry(
    stop=tenacity.stop_after_attempt(2),
    wait=tenacity.wait_exponential(multiplier=0.5, min=0.5, max=10),
    retry=tenacity.retry_if_exception_type((IOError, EOFError)),
    reraise=True,
)
def _load_cached_symbol(
    symbol: str,
    cache_dir: str,
    load_func: Callable[[str], pd.DataFrame],
    read_only_cache_dir: str | None = None,
    filter_func: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
) -> pd.DataFrame:
    filename = f'history_{symbol}.pickle'
    cache_file = os.path.join(cache_dir, filename)
    if os.path.isfile(cache_file):
        df: pd.DataFrame = pd.read_pickle(cache_file)
    elif (
        read_only_cache_dir is not None
        and filter_func is not None
        and os.path.isfile(os.path.join(read_only_cache_dir, filename))
    ):
        df: pd.DataFrame = pd.read_pickle(os.path.join(read_only_cache_dir, filename))
        df = filter_func(df)
    else:
        df = load_func(symbol)
        if len(df) == 0:
            df.index = pd.DatetimeIndex([], tz=TIME_ZONE)
        df.to_pickle(cache_file)
    return df


def get_default_data_client():
    return FmpClient()


def load_interday_dataset(
    symbols: Iterable[str], start_time: pd.Timestamp, end_time: pd.Timestamp, data_client: DataClient
) -> dict[str, pd.DataFrame]:
    if end_time.isoweekday() == 7:  # Improve cache hit
        end_time = end_time - datetime.timedelta(days=1)
    cache_key = hash_str(','.join(sorted(symbols)) + start_time.strftime('%F') + end_time.strftime('%F'))
    if cache_key in _interday_dataset_cache:
        return _interday_dataset_cache[cache_key]
    if not start_time.tzinfo:
        start_time = start_time.tz_localize(TIME_ZONE)
    if not end_time.tzinfo:
        end_time = end_time.tz_localize(TIME_ZONE)
    cache_dir = os.path.join(
        os.path.join(CACHE_DIR, str(TimeInterval.DAY)), start_time.strftime('%F'), end_time.strftime('%F')
    )
    read_only_cache_dir = _get_read_only_cache_dir(start_time, end_time)
    filter_func = None
    if read_only_cache_dir is not None:
        filter_func = lambda df: df[(df.index >= start_time) & (df.index <= end_time)]
    os.makedirs(cache_dir, exist_ok=True)
    res: dict[str, pd.DataFrame] = {}
    tasks = {}
    load_func = functools.partial(
        data_client.get_data, start_time=start_time, end_time=end_time, time_interval=TimeInterval.DAY
    )
    with futures.ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        for symbol in symbols:
            t = pool.submit(_load_cached_symbol, symbol, cache_dir, load_func, read_only_cache_dir, filter_func)
            tasks[symbol] = t
        iterator = tqdm(tasks.items(), ncols=80) if sys.stdout.isatty() else tasks.items()
        for symbol, t in iterator:
            res[symbol] = t.result()
    _interday_dataset_cache[cache_key] = res
    return res


def _get_read_only_cache_dir(start_time: pd.Timestamp, end_time: pd.Timestamp) -> str | None:
    daily_cache_dir = os.path.join(CACHE_DIR, str(TimeInterval.DAY))
    if not os.path.isdir(daily_cache_dir):
        return None
    start_dirs = os.listdir(daily_cache_dir)
    candidate_dir = None
    for start_dir in sorted(start_dirs, reverse=True):
        start_dir_time = pd.Timestamp(start_dir).tz_localize(TIME_ZONE)
        if start_dir_time > start_time:
            continue
        if not os.path.isdir(os.path.join(daily_cache_dir, start_dir)):
            continue
        end_dirs = os.listdir(os.path.join(daily_cache_dir, start_dir))
        for end_dir in sorted(end_dirs):
            end_dir_time = pd.Timestamp(end_dir).tz_localize(TIME_ZONE)
            if end_dir_time < end_time:
                continue
            end_dir_path = os.path.join(daily_cache_dir, start_dir, end_dir)
            if not os.path.isdir(end_dir_path):
                continue
            num_files = len(os.listdir(end_dir_path))
            if candidate_dir is None or num_files > candidate_dir[1]:
                candidate_dir = (end_dir_path, num_files)
    return candidate_dir[0] if candidate_dir else None


def load_intraday_dataset(
    symbols: Iterable[str], day: pd.Timestamp, data_client: DataClient
) -> dict[str, pd.DataFrame]:
    cache_dir = os.path.join(CACHE_DIR, str(TimeInterval.FIVE_MIN), day.strftime('%F'))
    os.makedirs(cache_dir, exist_ok=True)
    res = {}
    tasks = {}
    load_func = functools.partial(data_client.get_daily, day=day, time_interval=TimeInterval.FIVE_MIN)
    with futures.ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        for symbol in symbols:
            t = pool.submit(_load_cached_symbol, symbol, cache_dir, load_func)
            tasks[symbol] = t
        for symbol, t in tasks.items():
            res[symbol] = t.result()
    return res


@tenacity.retry(stop=tenacity.stop_after_attempt(3), wait=tenacity.wait_exponential(multiplier=0.5), reraise=True)
def get_transactions(start_date: str, data_client: DataClient) -> list[Transaction]:
    """Gets transactions from start date until today.

    params:
      start_date: The transactions after this date are fetched. Inclusive.
      data_client: The data client to use for fetching historical prices.
    """

    def round_time(t: pd.Timestamp) -> pd.Timestamp:
        if t.second > 30:
            t = t + datetime.timedelta(minutes=1)
        return pd.to_datetime(t.strftime('%F %H:%M:00%z'))

    def get_historical_price(symbol: str, t: pd.Timestamp) -> float | None:
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
    position_symbols = {position.symbol for position in positions}
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
        is_long = order.side == trading.OrderSide.BUY
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
                    is_long = prev_order.side == trading.OrderSide.BUY
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
                    if float(prev_order.filled_qty) >= 0.95 * qty:
                        break
                    else:
                        qty -= float(prev_order.filled_qty)
        transactions.append(
            Transaction(
                order.symbol,
                is_long,
                None,
                entry_price,
                exit_price,
                entry_time,
                exit_time,
                qty,
                gl,
                gl_pct,
                slippage,
                slippage_pct,
            )
        )
    return transactions
