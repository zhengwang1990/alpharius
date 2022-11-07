import datetime
from typing import Optional

import alpaca_trade_api as tradeapi
import pandas as pd
import pytz
import recordclass

TIME_ZONE = pytz.timezone('America/New_York')
Transaction = recordclass.recordclass(
    'Transaction',
    ['symbol', 'is_long', 'processor', 'entry_price', 'exit_price', 'entry_time', 'exit_time',
     'qty', 'gl', 'gl_pct', 'slippage', 'slippage_pct'])


def get_transactions(start_date: Optional[str]):
    """Gets transactions from start date until today.

    Start date is inclusive.
    """

    def round_time(t: pd.Timestamp):
        if t.second > 30:
            t = t + datetime.timedelta(minutes=1)
        return pd.to_datetime(t.strftime('%F %H:%M:00%z'))

    def get_historical_price(symbol, t) -> Optional[float]:
        timeframe = tradeapi.TimeFrame(5, tradeapi.TimeFrameUnit.Minute)
        t_str = (t - datetime.timedelta(minutes=5)).isoformat()
        bars = alpaca.get_bars(symbol, timeframe, t_str, t.isoformat())
        if not bars or pd.to_datetime(bars[0].t).timestamp() != t.timestamp() - 300:
            return None
        return bars[0].c

    alpaca = tradeapi.REST()

    chunk_size = 500
    orders = []
    start_time_str = (pd.to_datetime(start_date) - datetime.timedelta(days=1)).tz_localize(TIME_ZONE).isoformat()
    end_time = pd.to_datetime('now', utc=True)
    check_for_more_orders = True
    order_ids = set()
    while check_for_more_orders:
        order_chunk = alpaca.list_orders(status='closed',
                                         after=start_time_str,
                                         until=end_time.isoformat(),
                                         direction='desc',
                                         limit=chunk_size)
        for order in order_chunk:
            if order.id not in order_ids:
                orders.append(order)
                order_ids.add(order.id)
        if len(order_chunk) == chunk_size:
            end_time = orders[-3].submitted_at
        else:
            check_for_more_orders = False

    positions = alpaca.list_positions()
    orders_used = [False] * len(orders)
    position_symbols = set([position.symbol for position in positions])
    cut_time = pd.to_datetime(start_date).tz_localize(TIME_ZONE)
    transactions = []
    for i in range(len(orders)):
        order = orders[i]
        used = orders_used[i]
        if order.filled_at is None or used:
            continue
        filled_at = order.filled_at.tz_convert(TIME_ZONE)
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
                prev_filled_at = prev_order.filled_at.tz_convert(TIME_ZONE)
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
