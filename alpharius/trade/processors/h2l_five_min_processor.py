import datetime
from typing import List, Optional

import numpy as np
import pandas as pd

from alpharius.data import DataClient
from .processor import Processor
from ..common import (
    ActionType, Context, TradingFrequency, Position, PositionStatus,
    ProcessorAction, Mode, DAYS_IN_A_QUARTER)
from ..stock_universe import IntradayVolatilityStockUniverse

NUM_UNIVERSE_SYMBOLS = 40


class H2lFiveMinProcessor(Processor):

    def __init__(self,
                 lookback_start_date: pd.Timestamp,
                 lookback_end_date: pd.Timestamp,
                 data_client: DataClient,
                 output_dir: str) -> None:
        super().__init__(output_dir)
        self._stock_universe = IntradayVolatilityStockUniverse(lookback_start_date,
                                                               lookback_end_date,
                                                               data_client,
                                                               num_stocks=NUM_UNIVERSE_SYMBOLS)
        self._memo = dict()

    def get_trading_frequency(self) -> TradingFrequency:
        return TradingFrequency.FIVE_MIN

    def setup(self, hold_positions: List[Position], current_time: Optional[pd.Timestamp]) -> None:
        to_remove = [symbol for symbol, position in self._positions.items()
                     if position['status'] != PositionStatus.ACTIVE]
        for symbol in to_remove:
            self._positions.pop(symbol)
        self._memo = dict()

    def get_stock_universe(self, view_time: pd.Timestamp) -> List[str]:
        return list(set(self._stock_universe.get_stock_universe(view_time) +
                        list(self._positions.keys())))

    def process_data(self, context: Context) -> Optional[ProcessorAction]:
        if self.is_active(context.symbol):
            return self._close_position(context)
        elif context.symbol not in self._positions:
            return self._open_position(context)

    def _get_quarterly_high(self, context: Context) -> float:
        key = context.symbol + ':qh:' + context.current_time.strftime('%F')
        if key not in self._memo:
            quarterly_high = np.max(context.interday_lookback['Close'][-DAYS_IN_A_QUARTER:])
            self._memo[key] = quarterly_high
        return self._memo[key]

    def _open_position(self, context: Context) -> Optional[ProcessorAction]:
        t = context.current_time.time()
        if not (datetime.time(10, 0) <= t < datetime.time(10, 30) or
                datetime.time(13, 0) <= t < datetime.time(15, 30)):
            return
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        if context.current_price < 0.4 * self._get_quarterly_high(context):
            return
        intraday_closes = context.intraday_lookback['Close'].tolist()[market_open_index:]
        if len(intraday_closes) < 2:
            return
        if context.current_price > np.min(intraday_closes):
            return
        if abs(context.current_price / context.prev_day_close - 1) > 0.5:
            return
        intraday_opens = context.intraday_lookback['Open'].tolist()[market_open_index:]
        if intraday_opens[-2] > context.prev_day_close > intraday_closes[-1]:
            return
        prev_loss = intraday_closes[-2] / intraday_opens[-2] - 1
        current_loss = context.current_price / intraday_closes[-2] - 1
        lower_threshold = context.h2l_avg * 1.5
        upper_threshold = context.h2l_avg * 0.5
        is_trade = lower_threshold < prev_loss < upper_threshold and prev_loss < current_loss < 0
        if is_trade or (context.mode == Mode.TRADE and prev_loss < upper_threshold * 0.8):
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               f'Prev loss: {prev_loss * 100:.2f}%. '
                               f'Current loss: {current_loss * 100:.2f}%. '
                               f'Threshold: {lower_threshold * 100:.2f}% ~ {upper_threshold * 100:.2f}%. '
                               f'Current price: {context.current_price}. '
                               f'Prev open/close price: {intraday_opens[-2]}/{intraday_closes[-2]}.')
        if is_trade:
            self._positions[context.symbol] = {'entry_time': context.current_time,
                                               'status': PositionStatus.PENDING}
            return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)

    def _close_position(self, context: Context) -> Optional[ProcessorAction]:
        position = self._positions[context.symbol]
        intraday_closes = context.intraday_lookback['Close']
        take_profit = len(intraday_closes) >= 2 and context.current_price > intraday_closes[-2]
        is_close = (take_profit or
                    context.current_time >= position['entry_time'] + datetime.timedelta(minutes=10))
        self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                           f'Closing position: {is_close}. Current price {context.current_price}.')
        if is_close:
            position['status'] = PositionStatus.CLOSED
            return ProcessorAction(context.symbol, ActionType.SELL_TO_CLOSE, 1)
