import datetime
from typing import List, Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from alpharius.data import DataClient
from .processor import Processor
from ..common import (
    ActionType, Context, TradingFrequency,  Position, ProcessorAction,
    PositionStatus, Mode, DAYS_IN_A_WEEK)
from ..stock_universe import IntradayVolatilityStockUniverse

NUM_UNIVERSE_SYMBOLS = 20


class AbcdProcessor(Processor):

    def __init__(self,
                 lookback_start_date: pd.Timestamp,
                 lookback_end_date: pd.Timestamp,
                 data_client: DataClient,
                 output_dir: str,
                 logging_timezone: Optional[ZoneInfo] = None) -> None:
        super().__init__(output_dir, logging_timezone)
        self._positions = dict()
        self._stock_universe = IntradayVolatilityStockUniverse(lookback_start_date,
                                                               lookback_end_date,
                                                               data_client,
                                                               num_stocks=NUM_UNIVERSE_SYMBOLS)

    def get_trading_frequency(self) -> TradingFrequency:
        return TradingFrequency.FIVE_MIN

    def setup(self, hold_positions: List[Position], current_time: Optional[pd.Timestamp]) -> None:
        to_remove = [symbol for symbol, position in self._positions.items()
                     if position['status'] != 'active']
        for symbol in to_remove:
            self._positions.pop(symbol)

    def get_stock_universe(self, view_time: pd.Timestamp) -> List[str]:
        return list(set(self._stock_universe.get_stock_universe(view_time) +
                        list(self._positions.keys()) + ['TQQQ']))

    def process_data(self, context: Context) -> Optional[ProcessorAction]:
        if self.is_active(context.symbol):
            return self._close_position(context)
        elif context.symbol not in self._positions:
            return self._open_position(context)

    def _open_position(self, context: Context) -> Optional[ProcessorAction]:
        last_week_closes = context.interday_lookback['Close'].iloc[-DAYS_IN_A_WEEK:]
        if (abs(context.current_price / min(last_week_closes) - 1) > 0.5
                or abs(context.current_price / max(last_week_closes) - 1) > 0.5):
            return
        t = context.current_time.time()
        if (datetime.time(10, 30) <= t <= datetime.time(15, 30) and
                context.current_price > context.prev_day_close):
            if context.symbol == 'TQQQ':
                return self._open_long_position(context, 0.7, 0.4)
            else:
                return self._open_long_position(context, 1, 0.6)
        if (datetime.time(12, 0) <= t <= datetime.time(15, 30) and
                context.current_price < context.prev_day_close and
                context.symbol != 'TQQQ'):
            return self._open_short_position(context)

    def _open_long_position(self, context: Context, a: float, b: float) -> Optional[ProcessorAction]:
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        open_price = context.intraday_lookback['Open'].tolist()[market_open_index]
        intraday_closes = context.intraday_lookback['Close'].tolist()[market_open_index:]
        if len(intraday_closes) < 10:
            return
        if intraday_closes[-1] >= intraday_closes[-2]:
            return
        if abs(intraday_closes[-3] - intraday_closes[-2]) < abs(intraday_closes[-2] - intraday_closes[-1]):
            return
        if open_price / context.interday_lookback['Close'].iloc[-1] - 1 > 0.1:
            return
        max_i = np.argmax(intraday_closes)
        intraday_high = intraday_closes[max_i]
        if not context.prev_day_close < open_price < context.current_price < intraday_high:
            return
        l2h = context.l2h_avg
        if intraday_high / open_price - 1 < a * l2h:
            return
        if intraday_high / context.current_price - 1 < b * l2h:
            return
        if len(intraday_closes) > max_i + 15 or len(intraday_closes) < max_i + 10:
            return
        self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                           f'L2h: {l2h}. Intraday high: {intraday_high}. Current price: {context.current_price}.')
        self._positions[context.symbol] = {'entry_time': context.current_time,
                                           'status': PositionStatus.PENDING,
                                           'side': 'long'}
        return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)

    def _open_short_position(self, context: Context) -> Optional[ProcessorAction]:
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        open_price = context.intraday_lookback['Open'].iloc[market_open_index]
        intraday_closes = context.intraday_lookback['Close'].tolist()[market_open_index:]
        if len(intraday_closes) < 10:
            return
        intraday_low = np.min(intraday_closes)
        if not context.prev_day_close > open_price > context.current_price > intraday_low:
            return
        h2l = context.h2l_avg
        if intraday_low / open_price - 1 > 0.7 * h2l:
            return
        if intraday_low / context.current_price - 1 > 0.5 * h2l:
            return
        min_i = np.argmin(intraday_closes)
        if len(intraday_closes) > min_i + 10 or len(intraday_closes) < min_i + 5:
            return
        if context.mode == Mode.TRADE:
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               f'Checking bar shape conditions. Current price: {context.current_price}.')
        for i in range(-5, 0):
            if intraday_closes[i] < intraday_closes[i - 1]:
                break
        else:
            return
        if abs(intraday_closes[-3] - intraday_closes[-2]) < abs(intraday_closes[-2] - intraday_closes[-1]):
            return
        if intraday_closes[-1] <= intraday_closes[-2]:
            return
        self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                           f'H2l: {h2l}. Intraday low: {intraday_low}. Current price: {context.current_price}.')
        self._positions[context.symbol] = {'entry_time': context.current_time,
                                           'status': PositionStatus.PENDING,
                                           'side': 'short'}
        return ProcessorAction(context.symbol, ActionType.SELL_TO_OPEN, 1)

    def _close_position(self, context: Context) -> Optional[ProcessorAction]:
        position = self._positions[context.symbol]
        side = position['side']
        intraday_closes = context.intraday_lookback['Close'].to_numpy()
        take_profit = (context.current_time == position['entry_time'] + datetime.timedelta(minutes=30)
                       and len(intraday_closes) >= 7)
        if side == 'long':
            take_profit = take_profit and context.current_price > intraday_closes[-7]
        else:
            take_profit = take_profit and context.current_price < intraday_closes[-7]
        is_close = (take_profit or
                    context.current_time >= position['entry_time'] + datetime.timedelta(minutes=40)
                    or context.current_time.time() >= datetime.time(16, 0))
        self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                           f'Closing position: {is_close}. Current price {context.current_price}.')
        if is_close:
            position['status'] = PositionStatus.CLOSED
            action_type = ActionType.SELL_TO_CLOSE if side == 'long' else ActionType.BUY_TO_CLOSE
            return ProcessorAction(context.symbol, action_type, 1)
