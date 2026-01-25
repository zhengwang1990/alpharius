import datetime
from typing import List, Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from alpharius.data import DataClient
from .processor import Processor
from ..common import (
    ActionType, Context, TradingFrequency, Position, PositionStatus,
    ProcessorAction, Mode, DAYS_IN_A_WEEK, DAYS_IN_A_QUARTER)
from ..stock_universe import IntradayVolatilityStockUniverse

EXIT_TIME = datetime.time(16, 0)
# 3 hours only works if 1 hour and 2 hours are not triggered
PARAMS = [(10, 1), (13, 1.15), (25, 1.75), (30, 2.25), (37, 2.25)]


class H2lHourProcessor(Processor):

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
                                                               num_stocks=10,
                                                               num_top_volume=50)

    def get_trading_frequency(self) -> TradingFrequency:
        return TradingFrequency.FIVE_MIN

    def setup(self, hold_positions: List[Position], current_time: Optional[pd.Timestamp]) -> None:
        to_remove = [symbol for symbol, position in self._positions.items()
                     if position['status'] != PositionStatus.ACTIVE]
        for symbol in to_remove:
            self._positions.pop(symbol)

    def get_stock_universe(self, view_time: pd.Timestamp) -> List[str]:
        return list(set(self._stock_universe.get_stock_universe(view_time) +
                        list(self._positions.keys())))

    def process_data(self, context: Context) -> Optional[ProcessorAction]:
        if self.is_active(context.symbol):
            return self._close_position(context)
        elif (context.symbol not in self._positions or
              self._positions[context.symbol]['status'] == PositionStatus.PENDING):
            return self._open_position(context)

    def _open_position(self, context: Context) -> Optional[ProcessorAction]:
        t = context.current_time.time()
        if t >= EXIT_TIME:
            return
        interday_closes = context.interday_lookback['Close'].tolist()
        interday_week_changes = [interday_closes[i] / interday_closes[i - 1] - 1 for i in range(-DAYS_IN_A_WEEK, 0)]
        if any(abs(c) > 0.4 for c in interday_week_changes):
            return
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        intraday_closes = context.intraday_lookback['Close'].tolist()[market_open_index:]
        if len(intraday_closes) < PARAMS[0][0]:
            return
        if context.current_price > np.min(intraday_closes):
            return
        if abs(context.current_price / context.prev_day_close - 1) > 0.25:
            return
        intraday_opens = context.intraday_lookback['Open'].tolist()[market_open_index:]
        if intraday_opens[-1] > context.prev_day_close > intraday_closes[-1]:
            return
        if interday_closes[-1] > 10 * np.min(interday_closes[-DAYS_IN_A_QUARTER:]):
            return
        bar_sizes = [abs(intraday_closes[i] - intraday_opens[i]) for i in range(-min(30, len(intraday_opens)), 0)]
        if bar_sizes[-1] > 3 * np.median(bar_sizes):
            return
        if abs(context.prev_day_close - intraday_opens[0]) > abs(intraday_opens[0] - context.current_price):
            return
        n_open_down = sum(intraday_closes[i] < intraday_opens[i] for i in range(6))
        zd = max(n_open_down - 4, 0) * 0.05
        h2l_avg = context.h2l_avg
        h2l_std = context.h2l_std
        lower_threshold = max(h2l_avg - 3 * h2l_std, -0.5)
        for n, z in PARAMS:
            if len(intraday_closes) < n:
                continue
            current_loss = context.current_price / intraday_closes[-n] - 1
            z0 = 0.15 if t < datetime.time(11, 0) or t > datetime.time(15, 0) else 0
            if context.current_price < context.prev_day_close < intraday_opens[-n]:
                z0 += 0.05
            upper_threshold = h2l_avg - (z + z0 + zd) * h2l_std
            is_trade = lower_threshold < current_loss < upper_threshold
            if is_trade or (context.mode == Mode.TRADE and current_loss < upper_threshold * 0.8):
                self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                                   f'Current loss: {current_loss * 100:.2f}%. N: {n}. '
                                   f'Threshold: {lower_threshold * 100:.2f}% ~ {upper_threshold * 100:.2f}%. '
                                   f'Current price {context.current_price}.')
            if is_trade:
                self._positions[context.symbol] = {'entry_time': context.current_time,
                                                   'status': PositionStatus.PENDING, 'n': n}
                return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)

    def _close_position(self, context: Context) -> Optional[ProcessorAction]:
        position = self._positions[context.symbol]
        if (context.current_time >= position['entry_time'] + datetime.timedelta(minutes=30) or
                context.current_time.time() >= EXIT_TIME):
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               f'Closing position. Current price {context.current_price}.')
            position['status'] = PositionStatus.CLOSED
            return ProcessorAction(context.symbol, ActionType.SELL_TO_CLOSE, 1)
