import collections
import contextlib
import datetime
import itertools
import time
import unittest.mock as mock
import uuid
from typing import List, Dict, Optional

import alpaca.trading as trading
import numpy as np
import pandas as pd

from alpharius import trade
from alpharius.data import DataClient, TimeInterval, DATA_COLUMNS
from alpharius.utils import TIME_ZONE

Asset = collections.namedtuple('Asset', ['symbol', 'name', 'tradable', 'marginable',
                                         'shortable', 'easy_to_borrow', 'fractionable'])
Account = collections.namedtuple('Account', ['id', 'equity', 'cash', 'daytrading_buying_power'])
Order = collections.namedtuple('Order', ['id', 'symbol', 'side', 'qty', 'notional',
                                         'filled_qty', 'filled_at', 'filled_avg_price',
                                         'submitted_at', 'status'])
Bar = collections.namedtuple('Bar', ['t', 'o', 'h', 'l', 'c', 'vw', 'v'])
History = collections.namedtuple('History', ['equity', 'timestamp'])
Trade = collections.namedtuple('Trade', ['p'])


def _to_timestamp(t) -> int:
    time_obj = pd.to_datetime(t)
    if not time_obj.tzinfo:
        time_obj = time_obj.tz_localize(TIME_ZONE)
    return int(time_obj.timestamp())


class FakeAlpaca:

    def __init__(self):
        self.get_account_call_count = 0
        self.list_assets_call_count = 0
        self.get_asset_call_count = 0
        self.get_clock_call_count = 0
        self.list_orders_call_count = 0
        self.list_positions_call_count = 0
        self.submit_order_call_count = 0
        self.cancel_order_call_count = 0
        self.get_portfolio_history_call_count = 0
        self.get_bars_call_count = 0
        self.get_calendar_call_count = 0
        self.get_latest_trades_call_count = 0
        self.get_order_call_count = 0
        self._value_cycle = itertools.cycle([42, 40, 41, 43, 42, 41.5, 40,
                                             41, 42, 38, 41, 42])

    def get_account(self):
        self.get_account_call_count += 1
        return Account(uuid.uuid4(), '2000', '2000', '8000')

    def list_assets(self):
        self.list_assets_call_count += 1
        return [Asset(symbol, symbol, True, True, True, True, True)
                for symbol in ['QQQ', 'SPY', 'DIA', 'TQQQ', 'GOOG', 'AAPL', 'MSFT']]

    def get_asset(self, symbol):
        self.get_asset_call_count += 1
        return Asset(symbol, symbol, True, True, True, True, True)

    def list_positions(self):
        self.list_positions_call_count += 1
        params = {
            'exchange': trading.AssetExchange.NASDAQ,
            'asset_class': trading.AssetClass.US_EQUITY,
            'avg_entry_price': '10.0',
            'market_value': '99',
            'cost_basis': '100',
            'unrealized_pl': '-1',
            'unrealized_plpc': '1',
            'unrealized_intraday_pl': '-1',
            'unrealized_intraday_plpc': '1',
            'current_price': '9.9',
            'lastday_price': '10.0',
            'change_today': '-0.1',
        }
        return [trading.Position(asset_id=uuid.uuid4(), symbol='QQQ', qty='10',
                                 side=trading.PositionSide.LONG, **params),
                trading.Position(asset_id=uuid.uuid4(), symbol='GOOG', qty='-10',
                                 side=trading.PositionSide.SHORT, **params)]

    def get_clock(self):
        self.get_clock_call_count += 1
        current = pd.to_datetime(1615987000, utc=True, unit='s')
        next_open = pd.to_datetime(1615987800, utc=True, unit='s')
        next_close = pd.to_datetime(1616011200, utc=True, unit='s')
        return trading.Clock(timestamp=current, next_open=next_open, next_close=next_close)

    def get_order(self, order_id):
        self.get_order_call_count += 1
        filled_at = pd.to_datetime('2021-03-17T10:14:57.0Z')
        status = 'filled'
        if self.get_order_call_count % 3 == 0:
            filled_at = None
            status = 'accepted'
        return Order(order_id, 'QQQ', 'sell', '14', None, '0', filled_at,
                     '12', pd.to_datetime('2021-03-17T10:14:57.0Z'), status)

    def list_orders(self, status=None, direction=None, *args, **kwargs):
        self.list_orders_call_count += 1
        status = 'filled' if status == 'closed' else 'accepted'
        orders = [Order('ORDER122', 'DIA', 'sell', '14', None, '0',
                        pd.to_datetime('2021-03-17T10:14:57.0Z'), '12',
                        pd.to_datetime('2021-03-17T10:14:57.0Z'), status),
                  Order('ORDER124', 'SPY', 'buy', '12', None, '1',
                        pd.to_datetime('2021-03-17T10:20:00.0Z'), '13',
                        pd.to_datetime('2021-03-17T10:20:00.0Z'), status),
                  Order('ORDER123', 'DIA', 'buy', '14', None, '0',
                        pd.to_datetime('2021-03-17T10:15:57.0Z'), '9',
                        pd.to_datetime('2021-03-17T10:15:57.0Z'), status),
                  Order('ORDER125', 'QQQ', 'buy', None, '100.1', '10',
                        pd.to_datetime(time.time() - 3, utc=True, unit='s'), '9.1',
                        pd.to_datetime(time.time() - 4, utc=True, unit='s'), status),
                  Order('ORDER126', 'QQQ', 'sell', None, '100.1', '10',
                        pd.to_datetime(time.time() - 1, utc=True, unit='s'), '9.2',
                        pd.to_datetime(time.time() - 2, utc=True, unit='s'), status),
                  Order('ORDER127', 'QQQ', 'buy', None, '100.1', '10',
                        pd.to_datetime(time.time(), utc=True, unit='s'), '9.1',
                        pd.to_datetime(time.time(), utc=True, unit='s'), status)]
        if direction == 'desc':
            orders = orders[::-1]
        return orders

    def submit_order(self, symbol, side, *args, **kwargs):
        self.submit_order_call_count += 1
        return Order('ORDER123', symbol, side, None, '100.1', '10', None, '21',
                     pd.to_datetime('2021-03-17T10:20:00.0Z'), 'accepted')

    def cancel_order(self, *args, **kwargs):
        self.cancel_order_call_count += 1

    def get_portfolio_history(self, date_start, date_end, timeframe, *args, **kwargs):
        self.get_portfolio_history_call_count += 1
        if timeframe == '1D':
            time_interval = 86400
        elif timeframe == '1H':
            time_interval = 3600
        elif timeframe == '5Min':
            time_interval = 300
        else:
            raise ValueError('Time frame must be 5Min, 1H or 1D.')
        start_time = _to_timestamp(date_start)
        end_time = _to_timestamp(date_end) + time_interval
        timestamps = [t for t in range(start_time, end_time, time_interval)
                      if pd.to_datetime(t, unit='s', utc=True).tz_convert(TIME_ZONE).isoweekday() < 6]
        if len(timestamps) > 10:
            equity = [0] * 10 + [i * (-1) ** i + len(timestamps) + 1 for i in range(len(timestamps) - 10)]
        else:
            equity = [i * (-1) ** i + len(timestamps) + 1 for i in range(len(timestamps))]
        return History(equity, timestamps)

    def get_bars(self, symbol, timeframe, start, end, *args, **kwargs):
        self.get_bars_call_count += 1
        if timeframe.value == '1Day':
            time_interval = 86400
        elif timeframe.value == '1Hour':
            time_interval = 3600
        elif timeframe.value == '5Min':
            time_interval = 300
        else:
            raise ValueError('Time frame must be 5 min, 1 hour or 1 day.')
        start_timestamp = _to_timestamp(start)
        end_timestamp = _to_timestamp(end) + time_interval
        return [Bar(pd.to_datetime(t, unit='s', utc=True),
                    42, 50, 35, next(self._value_cycle), 40.123, 10)
                for t in range(start_timestamp, end_timestamp, time_interval)
                if pd.to_datetime(t, unit='s', utc=True).tz_convert(TIME_ZONE).isoweekday() < 6]

    def get_calendar(self, start, end, *args, **kwargs):
        self.get_calendar_call_count += 1
        start_date = pd.Timestamp(start)
        end_date = pd.Timestamp(end)
        calendar = []
        date = start_date
        while date <= end_date:
            if date.isoweekday() < 6:
                calendar.append(trading.Calendar(date=date.strftime('%F'),
                                                 open='09:30',
                                                 close='16:00'))
            date += datetime.timedelta(days=1)
        return calendar

    def get_latest_trades(self, symbols, *args, **kwargs):
        self.get_latest_trades_call_count += 1
        value = next(self._value_cycle) + 10 * (-1) ** self.get_latest_trades_call_count
        return {symbol: Trade(value) for symbol in symbols}


def get_order(symbol: str,
              order_side: trading.OrderSide,
              order_id: Optional[str] = None,
              filled_at: Optional[pd.Timestamp] = None,
              qty: Optional[str] = None):
    submitted_at = (filled_at - datetime.timedelta(seconds=3)
                    if filled_at else pd.to_datetime('2021-03-17T10:14:59.0Z'))
    return trading.Order(
        id=uuid.UUID(order_id) if isinstance(order_id, str) else uuid.uuid4(),
        client_order_id=str(uuid.uuid4()),
        created_at=submitted_at - datetime.timedelta(seconds=2),
        updated_at=submitted_at - datetime.timedelta(seconds=1),
        submitted_at=submitted_at,
        filled_at=filled_at,
        asset_id=uuid.uuid4(),
        symbol=symbol,
        asset_class=trading.AssetClass.US_EQUITY,
        qty=qty,
        filled_qty=qty if filled_at else None,
        filled_avg_price='11.1' if filled_at else None,
        order_class=trading.OrderClass.SIMPLE,
        order_type=trading.OrderType.MARKET,
        type=trading.OrderType.MARKET,
        side=order_side,
        time_in_force=trading.TimeInForce.DAY,
        status=trading.OrderStatus.FILLED if filled_at else trading.OrderStatus.ACCEPTED,
        extended_hours=False)


class FakeTradingClient:
    def __init__(self):
        self.get_calendar_call_count = 0
        self.list_assets_call_count = 0
        self.get_clock_call_count = 0
        self.get_all_positions_call_count = 0
        self.get_account_call_count = 0
        self.get_order_call_count = 0
        self.submit_order_call_count = 0
        self.get_orders_call_count = 0

    def get_account(self):
        self.get_account_call_count += 1
        return Account(uuid.uuid4(), '2000', '2000', '8000')

    def get_calendar(self, filters: trading.GetCalendarRequest) -> List[trading.Calendar]:
        self.get_calendar_call_count += 1
        start_date = pd.Timestamp(filters.start)
        end_date = pd.Timestamp(filters.end)
        calendar = []
        date = start_date
        while date <= end_date:
            if date.isoweekday() < 6:
                calendar.append(trading.Calendar(date=date.strftime('%F'),
                                                 open='09:30',
                                                 close='16:00'))
            date += datetime.timedelta(days=1)
        return calendar

    def get_all_assets(self, filter: trading.GetAssetsRequest):
        self.list_assets_call_count += 1
        return [trading.Asset(id=uuid.uuid4(),
                              exchange=trading.AssetExchange.NASDAQ,
                              symbol=symbol,
                              status=trading.AssetStatus.ACTIVE,
                              tradable=True,
                              marginable=True,
                              shortable=True,
                              easy_to_borrow=True,
                              fractionable=True,
                              **{'class': trading.AssetClass.US_EQUITY})
                for symbol in ['QQQ', 'SPY', 'DIA', 'TQQQ', 'GOOG', 'AAPL', 'MSFT']]

    def get_clock(self):
        self.get_clock_call_count += 1
        current = pd.to_datetime(1615987000, utc=True, unit='s')
        next_open = pd.to_datetime(1615987800, utc=True, unit='s')
        next_close = pd.to_datetime(1616011200, utc=True, unit='s')
        return trading.Clock(timestamp=current, next_open=next_open, next_close=next_close, is_open=False)

    def get_all_positions(self):
        self.get_all_positions_call_count += 1
        params = {
            'exchange': trading.AssetExchange.NASDAQ,
            'asset_class': trading.AssetClass.US_EQUITY,
            'avg_entry_price': '10.0',
            'market_value': '99',
            'cost_basis': '100',
            'unrealized_pl': '-1',
            'unrealized_plpc': '1',
            'unrealized_intraday_pl': '-1',
            'unrealized_intraday_plpc': '1',
            'current_price': '9.9',
            'lastday_price': '10.0',
            'change_today': '-0.1',
        }
        return [trading.Position(asset_id=uuid.uuid4(), symbol='QQQ', qty='10',
                                 side=trading.PositionSide.LONG, **params),
                trading.Position(asset_id=uuid.uuid4(), symbol='GOOG', qty='-10',
                                 side=trading.PositionSide.SHORT, **params)]

    def get_order_by_id(self, order_id: str):
        self.get_order_call_count += 1
        filled_at = pd.to_datetime('2021-03-17T10:14:57.0Z')
        if self.get_order_call_count % 3 == 0:
            filled_at = None
        return get_order('QQQ', trading.OrderSide.BUY, order_id, filled_at, '12')

    def submit_order(self, order_data: trading.OrderRequest):
        self.submit_order_call_count += 1
        return get_order(order_data.symbol, order_data.side, qty=str(order_data.qty))

    def get_orders(self, filter: trading.GetOrdersRequest):
        self.get_orders_call_count += 1
        want_filled_at = filter.status == trading.QueryOrderStatus.CLOSED
        orders = [
            get_order('DIA',
                      order_side=trading.OrderSide.SELL,
                      filled_at=pd.Timestamp('2021-03-17T10:14:57.0Z') if want_filled_at else None,
                      qty='12'),
            get_order('SPY',
                      order_side=trading.OrderSide.BUY,
                      filled_at=pd.Timestamp('2021-03-17T10:20:00.0Z') if want_filled_at else None,
                      qty='13'),
            get_order('DIA',
                      order_side=trading.OrderSide.BUY,
                      filled_at=pd.Timestamp('2021-03-17T10:15:57.0Z') if want_filled_at else None,
                      qty='12'),
            get_order('QQQ',
                      order_side=trading.OrderSide.BUY,
                      filled_at=pd.to_datetime(time.time() - 10, utc=True, unit='s') if want_filled_at else None,
                      qty='10'),
            get_order('QQQ',
                      order_side=trading.OrderSide.SELL,
                      filled_at=pd.to_datetime(time.time() - 5, utc=True, unit='s') if want_filled_at else None,
                      qty='10'),
            get_order('QQQ',
                      order_side=trading.OrderSide.BUY,
                      filled_at=pd.to_datetime(time.time(), utc=True, unit='s') if want_filled_at else None,
                      qty='10'),
        ]
        if filter.direction == trading.Sort.DESC:
            orders = orders[::-1]
        return orders


class FakeProcessor(trade.Processor):
    def __init__(self, trading_frequency):
        super().__init__('fake_output_dir')
        self.get_stock_universe_call_count = 0
        self.process_data_call_count = 0
        self.trading_frequency = trading_frequency

    def get_trading_frequency(self):
        return self.trading_frequency

    def get_stock_universe(self, view_time):
        self.get_stock_universe_call_count += 1
        return ['QQQ', 'SPY', 'DIA']

    def process_data(self, context):
        self.process_data_call_count += 1
        if context.current_time.time() == datetime.time(9, 35) and context.symbol == 'QQQ':
            return trade.ProcessorAction('QQQ', trade.ActionType.BUY_TO_OPEN, 1)
        if context.current_time.time() == datetime.time(10, 0) and context.symbol == 'DIA':
            return trade.ProcessorAction('DIA', trade.ActionType.BUY_TO_OPEN, 1)
        if context.current_time.time() == datetime.time(11, 30) and context.symbol == 'QQQ':
            return trade.ProcessorAction('QQQ', trade.ActionType.SELL_TO_CLOSE, 0.5)
        if context.current_time.time() == datetime.time(12, 0) and context.symbol == 'DIA':
            return trade.ProcessorAction('DIA', trade.ActionType.SELL_TO_CLOSE, 1)
        if context.current_time.time() == datetime.time(13, 0) and context.symbol == 'DIA':
            return trade.ProcessorAction('DIA', trade.ActionType.SELL_TO_OPEN, 1)
        if context.current_time.time() == datetime.time(13, 10) and context.symbol == 'DIA':
            return trade.ProcessorAction('DIA', trade.ActionType.BUY_TO_CLOSE, 1)
        if context.current_time.time() == datetime.time(16, 0) and context.symbol == 'SPY':
            return trade.ProcessorAction('SPY', trade.ActionType.BUY_TO_OPEN, 1)


class FakeDbEngine:
    def __init__(self):
        self.conn = mock.MagicMock()
        self.disconnect_cnt = 0

    @contextlib.contextmanager
    def connect(self):
        try:
            yield self.conn
        finally:
            self.disconnect_cnt += 1


class FakeDataClient(DataClient):
    def __init__(self, data: Optional[List[float]] = None):
        self.get_data_call_count = 0
        self.get_last_trades_call_count = 0
        data = data or [42, 40, 41, 43, 42, 41.5, 40, 41, 42, 38, 41, 42]
        self._value_cycle = itertools.cycle(data)

    def get_data(self,
                 symbol: str,
                 start_time: pd.Timestamp,
                 end_time: pd.Timestamp,
                 time_interval: TimeInterval) -> pd.DataFrame:
        self.get_data_call_count += 1
        if time_interval == TimeInterval.DAY:
            time_seconds = 86400
        elif time_interval == TimeInterval.HOUR:
            time_seconds = 3600
        elif time_interval == TimeInterval.FIVE_MIN:
            time_seconds = 300
        else:
            raise ValueError(f'time_interval {time_interval} not supported')
        if not start_time.tzinfo:
            start_time = start_time.tz_localize(TIME_ZONE)
        if not end_time.tzinfo:
            end_time = end_time.tz_localize(TIME_ZONE)
        start_timestamp = int(start_time.timestamp())
        end_timestamp = int(end_time.timestamp())
        index = []
        data = []
        for t in range(start_timestamp, end_timestamp, time_seconds):
            pd_timestamp = pd.to_datetime(t, unit='s', utc=True).tz_convert(TIME_ZONE)
            if pd_timestamp.isoweekday() < 6:
                index.append(pd_timestamp)
                open_price = next(self._value_cycle)
                close_price = next(self._value_cycle)
                high_price = max(open_price, close_price) * 1.01
                low_price = min(open_price, close_price) * 0.99
                data.append([np.float32(open_price), high_price, low_price,
                             np.float32(close_price), np.uint32(123)])
        return pd.DataFrame(data, index=index, columns=DATA_COLUMNS)

    def get_last_trades(self, symbols: List[str]) -> Dict[str, float]:
        self.get_last_trades_call_count += 1
        value = next(self._value_cycle) + 10 * (-1) ** self.get_last_trades_call_count
        return {symbol: value for symbol in symbols}
