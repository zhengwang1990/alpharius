import datetime
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from alpharius.data import DataClient
from .processor import Processor
from ..common import (
    ProcessorAction, ActionType, Context, TradingFrequency,
    Position, PositionStatus, DAYS_IN_A_MONTH)
from ..stock_universe import IntradayVolatilityStockUniverse

ENTRY_TIME = datetime.time(10, 0)
EXIT_TIME = datetime.time(14, 0)
NUM_STOCKS = 15
CONFIG = {'TQQQ': 8, 'UCO': 9, 'FAS': 9, 'NUGT': 9}
OTHER_N = 11


class BearMomentumProcessor(Processor):
    """Momentum strategy that works in a bear market."""

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
                                                               num_stocks=NUM_STOCKS)
        self._memo = dict()

    def get_trading_frequency(self) -> TradingFrequency:
        return TradingFrequency.FIVE_MIN

    def get_stock_universe(self, view_time: pd.Timestamp) -> List[str]:
        return list(set(self._stock_universe.get_stock_universe(view_time) +
                        list(CONFIG.keys()) +
                        list(self._positions.keys())))

    def setup(self, hold_positions: List[Position], current_time: Optional[pd.Timestamp]) -> None:
        self._memo = dict()

    def process_data(self, context: Context) -> Optional[ProcessorAction]:
        if self.is_active(context.symbol):
            return self._close_position(context)
        else:
            return self._open_position(context)

    def _get_interday_min_max(self, context: Context) -> Tuple[float, float]:
        key = context.symbol + context.current_time.strftime('%F')
        if key not in self._memo:
            interday_closes = context.interday_lookback['Close'].iloc[-DAYS_IN_A_MONTH * 2:]
            min_value = np.min(interday_closes)
            max_value = np.max(interday_closes)
            self._memo[key] = (min_value, max_value)
        return self._memo[key]

    def _open_position(self, context: Context) -> Optional[ProcessorAction]:
        t = context.current_time.time()
        if t <= ENTRY_TIME or t >= EXIT_TIME:
            return
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        intraday_closes = context.intraday_lookback['Close'].iloc[market_open_index:]
        n = CONFIG.get(context.symbol, OTHER_N)
        if len(intraday_closes) < n + 1:
            return
        if len(context.interday_lookback['Close']) < DAYS_IN_A_MONTH * 2:
            return
        interday_min, interday_max = self._get_interday_min_max(context)
        if context.current_price >= interday_max * 0.7:
            return
        no_up, no_down = 0, 0
        intraday_high = list(context.intraday_lookback['High'])
        intraday_low = list(context.intraday_lookback['Low'])
        for i in range(-1, -n - 1, -1):
            if intraday_low[i] >= intraday_low[i - 1]:
                no_down += 1
            if intraday_high[i] <= intraday_high[i - 1]:
                no_up += 1
            if no_down > 0 and no_up > 0:
                return
        up = n - no_up
        down = n - no_down
        self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                           f'Up count [{up} / {n}]. Down count [{down} / {n}]. '
                           f'Current price {context.current_price}.')
        if down == n and context.current_price < context.prev_day_close:
            self._positions[context.symbol] = {'entry_time': context.current_time,
                                               'side': 'short'}
            return ProcessorAction(context.symbol, ActionType.SELL_TO_OPEN, 1)
        if up == n and context.current_price > context.prev_day_close:
            self._positions[context.symbol] = {'entry_time': context.current_time,
                                               'status': PositionStatus.PENDING,
                                               'side': 'long'}
            return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)

    def _close_position(self, context: Context) -> Optional[ProcessorAction]:
        def _exit_action():
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               f'Closing position. Current price {context.current_price}.')
            position['status'] = PositionStatus.CLOSED
            return action

        position = self._positions[context.symbol]
        action_type = ActionType.SELL_TO_CLOSE if position['side'] == 'long' else ActionType.BUY_TO_CLOSE
        action = ProcessorAction(context.symbol, action_type, 1)
        wait_minutes = 60 if context.symbol in CONFIG else 90
        if context.current_time >= position['entry_time'] + datetime.timedelta(minutes=wait_minutes):
            return _exit_action()
        intraday_closes = context.intraday_lookback['Close']
        if (context.symbol not in CONFIG and
                position['side'] == 'long' and
                len(intraday_closes) >= 2 and
                intraday_closes[-2] < context.prev_day_close and
                context.current_price < context.prev_day_close):
            return _exit_action()
        entry_index = len(intraday_closes) - 1 - (context.current_time - position['entry_time']).seconds // 300
        if (context.symbol not in CONFIG and
                position['side'] == 'short' and
                len(intraday_closes) > max(entry_index, 3) and
                intraday_closes[-1] > intraday_closes[-2] > intraday_closes[-3] and
                context.current_price > context.prev_day_close > intraday_closes[entry_index]):
            return _exit_action()

