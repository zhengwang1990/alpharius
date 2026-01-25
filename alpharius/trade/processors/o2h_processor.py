import datetime
import pytz
from typing import List, Optional

import numpy as np
import pandas as pd

from alpharius.data import DataClient
from .processor import Processor
from ..common import (
    ActionType, Context, TradingFrequency, Position, PositionStatus,
    ProcessorAction, Mode, DAYS_IN_A_MONTH)
from ..stock_universe import IntradayVolatilityStockUniverse

NUM_UNIVERSE_SYMBOLS = 15
EXIT_TIME = datetime.time(11, 0)


class O2hProcessor(Processor):

    def __init__(self,
                 lookback_start_date: pd.Timestamp,
                 lookback_end_date: pd.Timestamp,
                 data_client: DataClient,
                 output_dir: str,
                 logging_timezone: Optional[pytz.timezone] = None) -> None:
        super().__init__(output_dir, logging_timezone)
        self._stock_universe = IntradayVolatilityStockUniverse(lookback_start_date,
                                                               lookback_end_date,
                                                               data_client,
                                                               num_stocks=NUM_UNIVERSE_SYMBOLS)
        self._positions = dict()
        self._memo = dict()

    def get_trading_frequency(self) -> TradingFrequency:
        return TradingFrequency.FIVE_MIN

    def setup(self, hold_positions: List[Position], current_time: Optional[pd.Timestamp]) -> None:
        to_remove = [symbol for symbol, position in self._positions.items()
                     if position['status'] != 'active']
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

    def _open_position(self, context: Context) -> Optional[ProcessorAction]:
        t = context.current_time.time()
        if t >= EXIT_TIME:
            return
        interday_closes = context.interday_lookback['Close'].tolist()[-DAYS_IN_A_MONTH:]
        if (context.current_price < 0.8 * interday_closes[-DAYS_IN_A_MONTH] or
                context.current_price > 1.5 * interday_closes[-DAYS_IN_A_MONTH]):
            return
        if interday_closes[-1] / interday_closes[-2] - 1 > 2.25 * context.l2h_avg:
            return
        key = context.symbol + context.current_time.strftime('%F')
        if key in self._memo:
            o2h_avg, o2h_std = self._memo[key]
        else:
            interday_opens = context.interday_lookback['Open'].iloc[-DAYS_IN_A_MONTH:]
            interday_highs = context.interday_lookback['High'].iloc[-DAYS_IN_A_MONTH:]
            o2h_gains = [h / o - 1 for o, h in zip(interday_opens, interday_highs)]
            o2h_avg = np.average(o2h_gains)
            o2h_std = np.std(o2h_gains)
            self._memo[key] = (o2h_avg, o2h_std)
        market_open_price = context.today_open
        if market_open_price is None:
            return
        intraday_closes = context.intraday_lookback['Close'].tolist()
        if len(intraday_closes) < 3:
            return
        if context.current_price < context.prev_day_close:
            return
        current_gain = context.current_price / market_open_price - 1
        z_score = (current_gain - o2h_avg) / (o2h_std + 1E-7)
        bar_diff = abs(intraday_closes[-3] - intraday_closes[-2]) - abs(intraday_closes[-2] - intraday_closes[-1])
        is_trade = bar_diff > 0
        is_trade = is_trade and 3.5 > z_score > 2
        if is_trade or (context.mode == Mode.TRADE and z_score > 1.5):
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               f'Current gain: {current_gain * 100:.2f}%. Z-score: {z_score:.2f}. '
                               f'Expected z-score range: 2 ~ 3.5. Bar diff: {bar_diff:.2f}. '
                               f'Open price: {market_open_price}. Current price: {context.current_price}.')
        if is_trade:
            self._positions[context.symbol] = {'entry_time': context.current_time,
                                               'status': PositionStatus.PENDING}
            return ProcessorAction(context.symbol, ActionType.SELL_TO_OPEN, 1)

    def _close_position(self, context: Context) -> Optional[ProcessorAction]:
        position = self._positions[context.symbol]
        if (context.current_time >= position['entry_time'] + datetime.timedelta(minutes=35) or
                context.current_time.time() >= EXIT_TIME):
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               f'Closing position. Current price {context.current_price}.')
            position['status'] = PositionStatus.CLOSED
            return ProcessorAction(context.symbol, ActionType.BUY_TO_CLOSE, 1)
