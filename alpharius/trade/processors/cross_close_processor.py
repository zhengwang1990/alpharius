import datetime
from typing import List, Optional

import numpy as np
import pandas as pd

from alpharius.data import DataClient
from .processor import Processor
from ..common import (
    ActionType, Context, TradingFrequency, Position, PositionStatus,
    ProcessorAction, Mode, DAYS_IN_A_MONTH)
from ..stock_universe import IntradayVolatilityStockUniverse

NUM_UNIVERSE_SYMBOLS = 20


def _round_num(num: float):
    magnitude = 10 ** (np.round(np.log10(num)) - 1)
    return np.round(num / magnitude) * magnitude


class CrossCloseProcessor(Processor):
    """Strategy acting on 5-min bar crossing previous day close."""

    def __init__(self,
                 lookback_start_date: pd.Timestamp,
                 lookback_end_date: pd.Timestamp,
                 data_client: DataClient,
                 output_dir: str) -> None:
        super().__init__(output_dir)
        self._positions = dict()
        self._stock_universe = IntradayVolatilityStockUniverse(lookback_start_date,
                                                               lookback_end_date,
                                                               data_client,
                                                               num_stocks=NUM_UNIVERSE_SYMBOLS)

    def get_trading_frequency(self) -> TradingFrequency:
        return TradingFrequency.FIVE_MIN

    def setup(self, hold_positions: List[Position], current_time: Optional[pd.Timestamp]) -> None:
        to_remove = [symbol for symbol, position in self._positions.items()
                     if position['status'] != PositionStatus.ACTIVE]
        for symbol in to_remove:
            self._positions.pop(symbol)

    def get_stock_universe(self, view_time: pd.Timestamp) -> List[str]:
        return list(set(self._stock_universe.get_stock_universe(view_time) +
                        list(self._positions.keys()) + ['TQQQ']))

    def process_data(self, context: Context) -> Optional[ProcessorAction]:
        if self.is_active(context.symbol):
            return self._close_position(context)
        elif context.symbol not in self._positions:
            action = self._open_break_short_position(context)
            if action:
                return action
            action = self._open_break_long_position(context)
            if action:
                return action
            action = self._open_reject_short_position(context)
            return action

    def _open_break_short_position(self, context: Context) -> Optional[ProcessorAction]:
        t = context.current_time.time()
        if not (t < datetime.time(10, 0) or
                datetime.time(10, 30) <= t < datetime.time(11, 0)):
            return
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        intraday_opens = context.intraday_lookback['Open'].tolist()[market_open_index:]
        intraday_closes = context.intraday_lookback['Close'].tolist()[market_open_index:]
        if len(intraday_closes) < 3:
            return
        if abs(context.current_price / context.prev_day_close - 1) > 0.5:
            return
        if context.current_price >= intraday_closes[-2]:
            return
        prev_loss = intraday_closes[-2] / intraday_closes[-3] - 1
        threshold = context.h2l_avg * 0.45
        is_cross = intraday_opens[-2] > context.prev_day_close > intraday_closes[-1]
        is_trade = prev_loss < threshold and is_cross
        if is_trade or (context.mode == Mode.TRADE and prev_loss < threshold * 0.8 and is_cross):
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               f'Prev loss: {prev_loss * 100:.2f}%. '
                               f'Threshold: {threshold * 100:.2f}%. '
                               f'Current price {context.current_price}. '
                               'Side: short.')
        if is_trade:
            self._positions[context.symbol] = {'entry_time': context.current_time,
                                               'status': PositionStatus.PENDING,
                                               'side': 'short'}
            return ProcessorAction(context.symbol, ActionType.SELL_TO_OPEN, 1)

    def _open_break_long_position(self, context: Context) -> Optional[ProcessorAction]:
        n_long = 6
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        intraday_closes = context.intraday_lookback['Close'].tolist()[market_open_index:]
        if len(intraday_closes) < n_long + 1:
            return
        level = None
        if intraday_closes[-2] < context.prev_day_close < intraday_closes[-1]:
            level = context.prev_day_close
        elif (context.symbol != 'TQQQ' and
              intraday_closes[-2] < _round_num(context.current_price) < intraday_closes[-1]):
            level = _round_num(context.current_price)
        if level is None:
            return
        for i in range(-n_long, 0):
            if intraday_closes[i] < intraday_closes[i - 1]:
                return
        if context.current_price != np.max(intraday_closes):
            return
        if intraday_closes[-n_long] > 0.6 * np.max(intraday_closes) + 0.4 * np.min(intraday_closes):
            return
        intraday_opens = context.intraday_lookback['Open'][market_open_index:]
        for i in range(len(intraday_closes) - n_long):
            if intraday_opens[i] < level < intraday_closes[i]:
                break
        else:
            return
        self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                           f'Level: {level}. '
                           f'Current price: {context.current_price}. Side: long.')
        self._positions[context.symbol] = {'entry_time': context.current_time,
                                           'status': PositionStatus.PENDING,
                                           'side': 'long'}
        return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)

    def _open_reject_short_position(self, context: Context) -> Optional[ProcessorAction]:
        n_long = 6
        t = context.current_time.time()
        if t >= datetime.time(11, 0):
            return
        interday_closes = list(context.interday_lookback['Close'])
        if len(interday_closes) < DAYS_IN_A_MONTH or interday_closes[-1] < 0.5 * interday_closes[-DAYS_IN_A_MONTH]:
            return
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        intraday_highs = context.intraday_lookback['High'].tolist()[market_open_index:]
        intraday_closes = context.intraday_lookback['Close'].tolist()[market_open_index:]
        if len(intraday_closes) < n_long + 1:
            return
        level = None
        if intraday_closes[-2] < intraday_closes[-1] < context.prev_day_close < intraday_highs[-1]:
            level = context.prev_day_close
        elif (context.symbol != 'TQQQ' and
              intraday_closes[-2] < intraday_closes[-1] < _round_num(context.current_price) < intraday_highs[-1]):
            level = _round_num(context.current_price)
        if level is None:
            return
        prev_gain = intraday_closes[-2] / intraday_closes[-n_long] - 1
        if prev_gain < context.l2h_avg * 0.5:
            return
        intraday_opens = context.intraday_lookback['Open'][market_open_index:]
        for i in range(-1, -n_long - 1, -1):
            if intraday_opens[i] > intraday_closes[i]:
                break
        else:
            return
        self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                           f'Prev gain: {prev_gain * 100:.2f}%. L2h: {context.l2h_avg * 100:.2f}%. '
                           f'Level: {level}. High: {intraday_highs[-1]}. '
                           f'Current price: {context.current_price}. Side: short.')
        self._positions[context.symbol] = {'entry_time': context.current_time,
                                           'status': PositionStatus.PENDING,
                                           'side': 'short'}
        return ProcessorAction(context.symbol, ActionType.SELL_TO_OPEN, 1)

    def _close_position(self, context: Context) -> Optional[ProcessorAction]:
        position = self._positions[context.symbol]
        side = position['side']
        if side == 'short':
            intraday_closes = context.intraday_lookback['Close']
            take_profit = (context.current_time == position['entry_time'] + datetime.timedelta(minutes=5)
                           and context.current_price < intraday_closes[-2])
            # Stop when there is an up bar
            stop_loss = (context.current_time >= position['entry_time'] + datetime.timedelta(minutes=10)
                         and context.current_price > intraday_closes[-2])
            is_close = (take_profit or stop_loss or
                        context.current_time >= position['entry_time'] + datetime.timedelta(minutes=20) or
                        context.current_time.time() >= datetime.time(16, 0))
        else:
            is_close = (context.current_time >= position['entry_time'] + datetime.timedelta(minutes=60)
                        or context.current_time.time() >= datetime.time(16, 0))
        if is_close:
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               f'Closing position. Current price {context.current_price}.')
            position['status'] = PositionStatus.CLOSED
            action_type = ActionType.BUY_TO_CLOSE if side == 'short' else ActionType.SELL_TO_CLOSE
            return ProcessorAction(context.symbol, action_type, 1)
