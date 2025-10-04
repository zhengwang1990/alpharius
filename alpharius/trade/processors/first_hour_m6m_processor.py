import datetime
from typing import List, Optional

import numpy as np
import pandas as pd

from alpharius.data import DataClient
from .processor import Processor
from ..common import (
    ActionType, Context, TradingFrequency, PositionStatus, ProcessorAction,
    DAYS_IN_A_MONTH, DAYS_IN_A_QUARTER)
from ..stock_universe import IntradayVolatilityStockUniverse

NUM_UNIVERSE_SYMBOLS = 20


class FirstHourM6mProcessor(Processor):

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

    def get_stock_universe(self, view_time: pd.Timestamp) -> List[str]:
        return list(set(self._stock_universe.get_stock_universe(view_time) +
                        list(self._positions.keys())))

    def process_data(self, context: Context) -> Optional[ProcessorAction]:
        if self.is_active(context.symbol):
            return self._close_position(context)
        elif context.symbol not in self._positions:
            t = context.current_time.time()
            if t != datetime.time(10, 0):
                return
            action = self._open_long_position(context)
            if action:
                return action
            action = self._open_short_position(context)
            if action:
                return action

    def _open_long_position(self, context: Context) -> Optional[ProcessorAction]:
        market_open_index = context.market_open_index
        intraday_highs = context.intraday_lookback['High'].tolist()[market_open_index:]
        intraday_closes = context.intraday_lookback['Close'].tolist()[market_open_index:]
        interday_closes = context.interday_lookback['Close'].tolist()
        interday_opens = context.interday_lookback['Open'].tolist()
        if len(interday_closes) < DAYS_IN_A_QUARTER:
            return
        if interday_closes[-1] < 1.3 * interday_closes[-DAYS_IN_A_MONTH]:
            return
        if interday_closes[-DAYS_IN_A_MONTH] < 1.1 * interday_closes[-DAYS_IN_A_QUARTER]:
            return
        if interday_closes[-1] < max(np.max(interday_closes[-DAYS_IN_A_MONTH:]),
                                     np.max(interday_opens[-DAYS_IN_A_MONTH:])) * 0.8:
            return
        if intraday_closes[0] >= context.prev_day_close:
            return
        if len(intraday_highs) != 6:
            return
        current_gain = context.current_price / context.prev_day_close - 1
        if current_gain > context.l2h_avg:
            return
        for i in range(len(intraday_highs) - 1):
            if intraday_highs[i] > intraday_highs[i + 1] and intraday_closes[i] > intraday_closes[i + 1]:
                return
        self._positions[context.symbol] = {'status': PositionStatus.PENDING,
                                           'entry_time': context.current_time,
                                           'side': 'long'}
        self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                           f'Current gain: {current_gain * 100:.2f}%. L2h: {context.l2h_avg * 100:.2f}%. '
                           f'Side: long. Current price {context.current_price}.')
        return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)

    def _open_short_position(self, context: Context) -> Optional[ProcessorAction]:
        t = context.current_time.time()
        if t != datetime.time(10, 0):
            return
        market_open_index = context.market_open_index
        intraday_highs = context.intraday_lookback['High'].tolist()[market_open_index:]
        intraday_lows = context.intraday_lookback['Low'].tolist()[market_open_index:]
        intraday_closes = context.intraday_lookback['Close'].tolist()[market_open_index:]
        intraday_opens = context.intraday_lookback['Open'].tolist()[market_open_index:]
        interday_closes = context.interday_lookback['Close'].tolist()
        if len(interday_closes) < DAYS_IN_A_QUARTER:
            return
        if interday_closes[-1] > 0.8 * interday_closes[-DAYS_IN_A_MONTH]:
            return
        if interday_closes[-DAYS_IN_A_MONTH] > 0.95 * interday_closes[-DAYS_IN_A_QUARTER]:
            return
        if intraday_closes[0] >= context.prev_day_close:
            return
        if len(intraday_lows) != 6:
            return
        current_loss = context.current_price / context.prev_day_close - 1
        if current_loss < 0.5 * context.h2l_avg:
            return
        if np.max(intraday_highs) < context.prev_day_close:
            return
        max_up, min_down = 0, 0
        for i in range(len(intraday_lows) - 1):
            if intraday_lows[i] < intraday_lows[i + 1] and intraday_closes[i] < intraday_closes[i + 1]:
                return
            max_up = max(max_up, intraday_closes[i] - intraday_opens[i])
            min_down = min(min_down, intraday_closes[i] - intraday_opens[i])
        if max_up > -min_down:
            return
        self._positions[context.symbol] = {'status': PositionStatus.PENDING,
                                           'entry_time': context.current_time,
                                           'side': 'short'}
        self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                           f'Current loss: {current_loss * 100:.2f}%. H2l: {context.h2l_avg * 100:.2f}%. '
                           f'Side: short. Current price {context.current_price}.')
        return ProcessorAction(context.symbol, ActionType.SELL_TO_OPEN, 1)

    def _close_position(self, context: Context) -> Optional[ProcessorAction]:
        position = self._positions[context.symbol]
        side = position['side']
        wait_minutes = 20 if side == 'long' else 10
        if context.current_time >= position['entry_time'] + datetime.timedelta(minutes=wait_minutes):
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               f'Closing position. Current price {context.current_price}.')
            self._positions.pop(context.symbol)
            action_type = ActionType.SELL_TO_CLOSE if side == 'long' else ActionType.BUY_TO_CLOSE
            return ProcessorAction(context.symbol, action_type, 1)
