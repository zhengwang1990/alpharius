import datetime
from typing import List, Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from alpharius.data import DataClient
from .processor import Processor
from ..common import (
    ActionType, Context, TradingFrequency, Position, PositionStatus,
    ProcessorAction, DAYS_IN_A_MONTH, DAYS_IN_A_WEEK)
from ..stock_universe import IntradayVolatilityStockUniverse

NUM_UNIVERSE_SYMBOLS = 20
ENTRY_TIME = datetime.time(10, 0)
EXIT_TIME = datetime.time(16, 0)


class OpenHighProcessor(Processor):

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

    def get_stock_universe(self, view_time: pd.Timestamp) -> List[str]:
        return list(set(self._stock_universe.get_stock_universe(view_time) +
                        list(self._positions.keys())))

    def setup(self, hold_positions: List[Position], current_time: Optional[pd.Timestamp]) -> None:
        to_remove = [symbol for symbol, position in self._positions.items()
                     if position['status'] != PositionStatus.ACTIVE]
        for symbol in to_remove:
            self._positions.pop(symbol)

    def process_data(self, context: Context) -> Optional[ProcessorAction]:
        if self.is_active(context.symbol):
            return self._close_position(context)
        elif context.symbol not in self._positions:
            return self._open_position(context)

    def _open_position(self, context: Context) -> Optional[ProcessorAction]:
        t = context.current_time.time()
        if not ENTRY_TIME <= t < EXIT_TIME:
            return
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        interday_closes = context.interday_lookback['Close'].tolist()
        month_low = np.min(interday_closes[-DAYS_IN_A_MONTH:])
        week_low = np.min(interday_closes[-DAYS_IN_A_WEEK:])
        month_gain = interday_closes[-1] / month_low - 1
        if month_gain < 0.4:
            return
        # If 80% of the monthly growth is contirbued by last week
        if week_low / month_low - 1 < 0.2 * month_gain:
            return
        intraday_opens = context.intraday_lookback['Open'].tolist()[market_open_index:]
        open_price = intraday_opens[0]
        open_gain = open_price / context.prev_day_close - 1
        if open_gain < context.l2h_avg:
            return
        if context.current_price < context.prev_day_close:
            return
        intraday_closes = context.intraday_lookback['Close'].tolist()[market_open_index:]
        n = 4
        if len(intraday_closes) < n:
            return
        for i in range(-1, -n - 1, -1):
            if intraday_closes[i] >= intraday_opens[i]:
                return
        drop = context.current_price / intraday_opens[-n] - 1
        threshold = 1.3 * context.h2l_avg
        self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                           f'Current drop: {drop * 100:.2f}%. Drop threshold: {threshold * 100:.2f}%. '
                           f'Current price: {context.current_price}.')
        if drop > threshold:
            self._positions[context.symbol] = {'entry_time': context.current_time,
                                               'status': 'pending'}
            return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)

    def _close_position(self, context: Context) -> Optional[ProcessorAction]:
        position = self._positions[context.symbol]
        stop_loss = context.current_price < context.prev_day_close
        is_close = (stop_loss or
                    context.current_time >= position['entry_time'] + datetime.timedelta(minutes=15)
                    or context.current_time.time() >= EXIT_TIME)
        self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                           f'Closing position: {is_close}. Current price {context.current_price}.')
        if is_close:
            position['status'] = 'inactive'
            return ProcessorAction(context.symbol, ActionType.SELL_TO_CLOSE, 1)
