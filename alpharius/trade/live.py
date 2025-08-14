import collections
import datetime
import os
import socket
import threading
import time
from concurrent import futures
from typing import List, Optional

import alpaca.trading as trading
import pandas as pd
import numpy as np
import pytz
import retrying
from alpaca.common import APIError
from sqlalchemy import exc

from alpharius.data import (
    DATA_COLUMNS,
    DataClient,
    TimeInterval,
    get_transactions,
    load_interday_dataset,
)
from alpharius.db import Db
from alpharius.utils import (
    get_today,
    get_all_symbols,
    TIME_ZONE,
    get_trading_client,
)
from .common import (
    Action, ActionType, ProcessorFactory, TradingFrequency, Context, Mode,
    Position, MARKET_OPEN, INTERDAY_LOOKBACK_LOAD, OUTPUT_DIR,
    SHORT_RESERVE_RATIO, logging_config, get_unique_actions)

_MAX_WORKERS = 10


class Live:

    def __init__(self,
                 processor_factories: List[ProcessorFactory],
                 data_client: DataClient,
                 logging_timezone: Optional[pytz.timezone] = None) -> None:
        self._output_dir = os.path.join(OUTPUT_DIR, 'live',
                                        datetime.datetime.now().strftime('%F'))
        os.makedirs(self._output_dir, exist_ok=True)
        self._logger = logging_config(os.path.join(self._output_dir, 'trading.txt'),
                                      detail=True,
                                      name='live',
                                      timezone=logging_timezone)
        self._logger.info('Trading is running on [%s]', socket.gethostname())
        self._equity, self._cash = 0, 0
        self._cash_reserve = float(os.environ.get('CASH_RESERVE', 0))
        self._today = get_today()
        self._processor_factories = processor_factories
        self._alpaca = get_trading_client()
        self._db = Db()
        self._update_account()
        self._update_positions()
        self._processors = []
        self._frequency_to_processor = collections.defaultdict(list)
        self._processor_stock_universes = dict()
        self._stock_universe = collections.defaultdict(set)
        self._interday_data = dict()
        self._intraday_data = dict()
        self._latest_trades = dict()
        self._db_thread = None
        self._data_client = data_client
        clock = self._alpaca.get_clock()
        self._market_open = clock.next_open.timestamp()
        self._market_close = clock.next_close.timestamp()
        if self._market_open > self._market_close:
            self._market_open = pd.to_datetime(
                pd.Timestamp.combine(self._today.date(), MARKET_OPEN)).tz_localize(TIME_ZONE).timestamp()

    @retrying.retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
    def _update_account(self) -> None:
        account = self._alpaca.get_account()
        self._equity = float(account.equity)
        self._cash = float(account.cash)
        self._logger.info('Account updated: equity [%s]; cash [%s]; day trading bp [%s].',
                          self._equity, self._cash, account.daytrading_buying_power)

    def _update_positions(self) -> None:
        alpaca_positions = self._alpaca.get_all_positions()
        self._positions = [Position(position.symbol, float(position.qty),
                                    float(position.avg_entry_price), None, None)
                           for position in alpaca_positions]
        self._logger.info('Positions updated: [%d] open positions.', len(self._positions))

    def _init_processors(self, history_start: pd.Timestamp) -> None:
        self._processors = []
        for factory in self._processor_factories:
            processor = factory.create(lookback_start_date=history_start,
                                       lookback_end_date=self._today,
                                       data_client=self._data_client,
                                       output_dir=self._output_dir)
            self._processors.append(processor)
            self._frequency_to_processor[processor.get_trading_frequency()].append(processor)
        for processor in self._processors:
            processor.setup(self._positions, self._today)
        self._logger.info('Initialized processors: %s',
                          [processor.name for processor in self._processors])

    def _init_stock_universe(self) -> None:
        for processor in self._processors:
            processor_name = processor.name
            processor_stock_universe = processor.get_stock_universe(self._today)
            self._processor_stock_universes[processor_name] = processor_stock_universe
            self._stock_universe[processor.get_trading_frequency()].update(processor_stock_universe)
        self._logger.info('FIVE_MIN stock universe: %s',
                          sorted(self._stock_universe.get(TradingFrequency.FIVE_MIN, [])))

    def run(self) -> None:
        # Check if today is a trading day
        calendar = self._alpaca.get_calendar(trading.GetCalendarRequest(start=self._today.date(),
                                                                        end=self._today.date()))
        if not calendar or calendar[0].date != self._today.date():
            self._logger.info('Market does not open on [%s]', self._today.date())
            return
        if time.time() < self._market_open - 3600:
            self._logger.info('Market open is more than one hour away')
            return

        # Initialize
        history_start = self._today - datetime.timedelta(days=INTERDAY_LOOKBACK_LOAD)
        self._interday_data = load_interday_dataset(get_all_symbols(),
                                                    history_start,
                                                    self._today,
                                                    self._data_client)
        self._init_processors(history_start)
        self._init_stock_universe()
        self._upload_log()

        # Wait for market open
        while time.time() < self._market_open:
            time.sleep(10)

        # Process
        processed = []
        while time.time() < self._market_close:
            current_time = pd.to_datetime(pd.Timestamp(int(time.time()), unit='s', tz=TIME_ZONE))
            next_minute = current_time + datetime.timedelta(minutes=1)
            if int(current_time.minute) % 5 == 4:
                checkpoint_time = pd.to_datetime(
                    pd.Timestamp.combine(self._today.date(),
                                         datetime.time(int(next_minute.hour),
                                                       int(next_minute.minute)))).tz_localize(TIME_ZONE)
                trigger_seconds = 50
                if checkpoint_time.timestamp() == self._market_close:
                    trigger_seconds -= 15
                if current_time.second > trigger_seconds and checkpoint_time not in processed:
                    self._process(checkpoint_time)
                    processed.append(checkpoint_time)
            time.sleep(1)

        self._upload_log()
        if self._db_thread:
            self._db_thread.join(timeout=100)

    def _process(self, checkpoint_time: pd.Timestamp) -> None:
        self._logger.info('Process starts for [%s]', checkpoint_time.time())
        frequency_to_process = [TradingFrequency.FIVE_MIN]
        if checkpoint_time.timestamp() == self._market_open + 300:
            frequency_to_process = [TradingFrequency.FIVE_MIN,
                                    TradingFrequency.CLOSE_TO_OPEN]
        elif checkpoint_time.timestamp() == self._market_close:
            frequency_to_process = [TradingFrequency.FIVE_MIN,
                                    TradingFrequency.CLOSE_TO_OPEN,
                                    TradingFrequency.CLOSE_TO_CLOSE]

        self._update_intraday_data(frequency_to_process, checkpoint_time)

        contexts = dict()
        for frequency, symbols in self._stock_universe.items():
            if frequency not in frequency_to_process:
                continue
            for symbol in symbols:
                intraday_lookback = self._intraday_data[symbol]
                interday_lookback = self._interday_data.get(symbol)
                if interday_lookback is None:
                    self._logger.warning('[%s] interday data not available', symbol)
                    continue
                if not len(intraday_lookback):
                    self._logger.warning('[%s] intraday data not available', symbol)
                    continue
                current_price = intraday_lookback['Close'].iloc[-1]
                context = Context(symbol=symbol,
                                  current_time=checkpoint_time,
                                  current_price=current_price,
                                  interday_lookback=interday_lookback,
                                  intraday_lookback=intraday_lookback,
                                  mode=Mode.TRADE)
                contexts[symbol] = context
        self._logger.info('Contexts prepared for [%s] symbols.', len(contexts))

        actions = []
        for processor in self._processors:
            if processor.get_trading_frequency() not in frequency_to_process:
                continue
            processor_name = processor.name
            stock_universe = self._processor_stock_universes[processor_name]
            processor_contexts = []
            for symbol in stock_universe:
                context = contexts.get(symbol)
                if context:
                    processor_contexts.append(context)
            processor_actions = processor.process_all_data(processor_contexts)
            actions.extend([Action(pa.symbol, pa.type, pa.percent,
                                   contexts[pa.symbol].current_price,
                                   processor)
                            for pa in processor_actions])
        self._logger.info('Got [%d] actions to process.', len(actions))

        executed_closes = self._trade(actions)
        self._db_thread = threading.Thread(target=self._update_db,
                                           args=(executed_closes,))
        self._db_thread.start()

    def _update_intraday_data(self,
                              frequency_to_process: List[TradingFrequency],
                              checkpoint_time: pd.Timestamp) -> None:
        update_start = time.time()
        tasks = dict()
        all_symbols = []
        for frequency, symbols in self._stock_universe.items():
            if frequency not in frequency_to_process:
                continue
            all_symbols.extend(symbols)
        all_symbols = list(set(all_symbols))
        with futures.ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
            for symbol in all_symbols:
                t = pool.submit(self._data_client.get_daily,
                                symbol, self._today, TimeInterval.FIVE_MIN)
                tasks[symbol] = t
            for symbol, t in tasks.items():
                self._intraday_data[symbol] = t.result()
        latest_trades = self._data_client.get_last_trades(all_symbols)
        expected_index = checkpoint_time - datetime.timedelta(minutes=5)
        for symbol, price in latest_trades.items():
            intraday_lookback = self._intraday_data[symbol]
            last_index = intraday_lookback.index[-1] if len(intraday_lookback) else None
            if not last_index or last_index != expected_index:
                self._logger.info('[%s] intraday data not available. Expect last index [%s], but got [%s]',
                                  symbol,
                                  expected_index.strftime('%H:%M:%S'),
                                  last_index.strftime('%H:%M:%S') if last_index else None)
                self._intraday_data[symbol] = pd.concat(
                    [intraday_lookback,
                     pd.DataFrame([[price if c != 'Volume' else 0 for c in DATA_COLUMNS]],
                                  index=[expected_index],
                                  columns=DATA_COLUMNS)])
            else:
                old_value = intraday_lookback['Close'].iloc[-1]
                if abs(price / old_value - 1) > 0.01:
                    self._logger.debug('[%s] Current price is updated from [%.5g] to [%.5g]',
                                       symbol, old_value, price)
                intraday_lookback.at[intraday_lookback.index[-1], 'Close'] = np.float32(price)
        self._logger.info('Intraday data updated for [%d] symbols. Time elapsed [%.2fs]',
                          len(tasks), time.time() - update_start)

    def _get_position(self, symbol: str) -> Optional[Position]:
        for position in self._positions:
            if symbol == position.symbol:
                return position
        return None

    def _trade(self, actions: List[Action]) -> List[Action]:
        if not actions:
            return []

        unique_actions = get_unique_actions(actions)

        close_actions = [action for action in unique_actions
                         if action.type in [ActionType.BUY_TO_CLOSE, ActionType.SELL_TO_CLOSE]]
        executed_closes = self._close_positions(close_actions)

        open_actions = [action for action in unique_actions
                        if action.type in [ActionType.BUY_TO_OPEN, ActionType.SELL_TO_OPEN]]
        self._open_positions(open_actions)

        return executed_closes

    def _close_positions(self, actions: List[Action]) -> List[Action]:
        """Closes positions instructed by input actions."""
        self._update_positions()
        executed_closes = []
        order_ids = []
        for action in actions:
            assert action.type in [ActionType.BUY_TO_CLOSE, ActionType.SELL_TO_CLOSE]
            symbol = action.symbol
            current_position = self._get_position(symbol)
            if current_position is None:
                self._logger.info('Position for [%s] does not exist. Skipping close.', symbol)
                continue
            if action.type == ActionType.BUY_TO_CLOSE and current_position.qty > 0:
                self._logger.info('Position for [%s] is already long-side. Skipping close.', symbol)
                continue
            if action.type == ActionType.SELL_TO_CLOSE and current_position.qty < 0:
                self._logger.info('Position for [%s] is already short-side. Skipping close.', symbol)
                continue
            qty = abs(current_position.qty) * action.percent
            side = 'buy' if action.type == ActionType.BUY_TO_CLOSE else 'sell'
            order_id = self._place_order(symbol=symbol, side=side, qty=qty)
            if order_id:
                order_ids.append(order_id)
            executed_closes.append(action)

        self._wait_for_order_to_fill(order_ids)
        return executed_closes

    def _open_positions(self, actions: List[Action]) -> None:
        """Opens positions instructed by input actions."""
        self._update_account()
        self._update_positions()
        tradable_cash = self._cash - self._cash_reserve
        order_ids = []
        for position in self._positions:
            if position.qty < 0:
                tradable_cash += position.entry_price * position.qty * (1 + SHORT_RESERVE_RATIO)
        action_cnt = collections.defaultdict(int)
        for action in actions:
            action_cnt[action.symbol] += 1
        for action in actions:
            assert action.type in [ActionType.BUY_TO_OPEN, ActionType.SELL_TO_OPEN]
            symbol = action.symbol
            # Avoid controversial actions for the same symbol
            if action_cnt[symbol] > 1:
                continue
            cash_to_trade = min(tradable_cash / len(actions),
                                tradable_cash * action.percent)
            if cash_to_trade < (self._equity - self._cash_reserve) * 0.01:
                self._logger.info('Cash [%s] too small to open position [%s]. Skip open.',
                                  cash_to_trade, symbol)
                continue
            if action.type == ActionType.BUY_TO_OPEN:
                side = 'buy'
                qty = None
                # Only use 99% to avoid exceeding daytrading_buying_power
                notional = int(cash_to_trade * 99) / 100
            else:
                side = 'sell'
                qty = int(cash_to_trade * 0.99 / action.price)
                notional = None
            order_id = self._place_order(symbol=symbol, side=side, qty=qty, notional=notional)
            if order_id:
                order_ids.append(order_id)
            action.processor.ack(symbol)

        self._wait_for_order_to_fill(order_ids)

    @retrying.retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
    def _place_order(self, symbol: str, side: str,
                     qty: Optional[float] = None,
                     notional: Optional[float] = None,
                     limit_price: Optional[float] = None) -> Optional[str]:
        order_type = trading.OrderType.MARKET if limit_price is None else trading.OrderType.LIMIT
        self._logger.info('Placing order for [%s]: side [%s]; qty [%s]; notional [%s]; type [%s].',
                          symbol, side, qty, notional, order_type)
        side_map = {'buy': trading.OrderSide.BUY, 'sell': trading.OrderSide.SELL}
        try:
            params = {'symbol': symbol, 'qty': qty, 'side': side_map[side],
                      'time_in_force': trading.TimeInForce.DAY, 'notional': notional}
            if limit_price:
                request = trading.LimitOrderRequest(**params, limit_price=limit_price)
            else:
                request = trading.MarketOrderRequest(**params)
            order = self._alpaca.submit_order(request)
            return str(order.id)
        except APIError as e:
            self._logger.error('Failed to place [%s] order for [%s]: %s: %s',
                               side, symbol, type(e).__name__, e)

    @retrying.retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
    def _wait_for_order_to_fill(self, order_ids: List[str], timeout: int = 10) -> None:
        def _update_open_orders(open_orders):
            remaining = []
            for order_id in open_orders:
                order = self._alpaca.get_order_by_id(order_id)
                if order.status != trading.OrderStatus.FILLED:
                    remaining.append(order_id)
            return remaining

        orders = _update_open_orders(order_ids)
        if not orders:
            self._logger.info('[%s] orders filled', len(order_ids))
            return
        wait_time = 0
        while orders:
            self._logger.info('Waiting for orders to fill. [%d] open orders remaining.', len(orders))
            time.sleep(2)
            wait_time += 2
            if wait_time >= timeout:
                break
            orders = _update_open_orders(orders)
        if not orders:
            self._logger.info('All orders are filled')
        else:
            self._logger.warning('[%d] orders not filled: %s', len(orders), orders)

    def _update_db(self, executed_closes: List[Action]) -> None:
        current_time = time.time()
        self._upload_log()
        time.sleep(15)
        if executed_closes:
            transactions = get_transactions(self._today.strftime('%F'), self._data_client)
            actions = {action.symbol: action for action in executed_closes}
            for transaction in transactions:
                symbol = transaction.symbol
                if transaction.gl_pct is None:
                    continue
                if symbol not in actions or current_time - transaction.exit_time.timestamp() > 100:
                    continue
                transaction.processor = actions[symbol].processor.name
                try:
                    self._db.insert_transaction(transaction)
                except exc.SQLAlchemyError as e:
                    self._logger.error('[%s] Transaction inserting encountered an error\n%s', symbol, e)
            try:
                self._db.update_aggregation(self._today.strftime('%F'))
            except exc.SQLAlchemyError as e:
                self._logger.error('Aggregation updating encountered an error\n%s', e)

    def _upload_log(self):
        try:
            self._db.update_log(self._today.strftime('%F'), self._output_dir)
        except exc.SQLAlchemyError as e:
            self._logger.error('Log updating encountered an error\n%s', e)
