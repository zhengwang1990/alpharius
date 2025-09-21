import datetime
import pytz
from typing import List, Optional

import numpy as np
import pandas as pd

from alpharius.data import DataClient
from ..common import (
    ActionType, Context, Processor, ProcessorFactory, TradingFrequency,
    Position, PositionStatus, ProcessorAction, Mode, DAYS_IN_A_MONTH, DAYS_IN_A_YEAR,
    DAYS_IN_A_WEEK)
from ..stock_universe import IntradayVolatilityStockUniverse

NUM_UNIVERSE_SYMBOLS = 25
EXIT_TIME = datetime.time(16, 0)


class L2hProcessor(Processor):

    def __init__(self,
                 lookback_start_date: pd.Timestamp,
                 lookback_end_date: pd.Timestamp,
                 data_client: DataClient,
                 output_dir: str,
                 logging_timezone: Optional[pytz.timezone] = None) -> None:
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
        interday_closes = context.interday_lookback['Close']
        if len(interday_closes) < DAYS_IN_A_YEAR:
            return
        if (context.current_price < 1.2 * interday_closes.iloc[-DAYS_IN_A_MONTH] or
                context.current_price > interday_closes.iloc[-DAYS_IN_A_MONTH] * 2):
            return
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        intraday_closes = context.intraday_lookback['Close'].tolist()[market_open_index:]
        intraday_opens = context.intraday_lookback['Open'].tolist()[market_open_index:]
        if len(intraday_closes) < 10:
            return
        last_week_closes = context.interday_lookback['Close'].iloc[-DAYS_IN_A_WEEK:]
        if (abs(context.current_price / min(last_week_closes) - 1) > 0.4
                or abs(context.current_price / max(last_week_closes) - 1) > 0.4):
            return
        if context.current_price < np.max(intraday_closes):
            return
        for i in range(-1, -5, -1):
            if intraday_closes[i] <= intraday_closes[i - 1] and intraday_closes[i] <= intraday_opens[i]:
                break
        else:
            return

        bar_sizes = [intraday_closes[i] - intraday_opens[i] for i in range(len(intraday_closes))
                     if intraday_closes[i] > intraday_opens[i]]
        if bar_sizes[-1] < np.median(bar_sizes):
            return

        current_gain = context.current_price / intraday_closes[-10] - 1
        threshold = context.l2h_avg * 0.75
        is_trade = threshold < current_gain
        if is_trade or (context.mode == Mode.TRADE and current_gain > threshold * 0.8):
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               f'Current gain: {current_gain * 100:.2f}%. '
                               f'Threshold: {threshold * 100:.2f}%. '
                               f'Current price {context.current_price}.')
        if is_trade:
            self._positions[context.symbol] = {'entry_time': context.current_time,
                                               'status': PositionStatus.PENDING}
            return ProcessorAction(context.symbol, ActionType.SELL_TO_OPEN, 1)

    def _close_position(self, context: Context) -> Optional[ProcessorAction]:
        position = self._positions[context.symbol]
        intraday_closes = list(context.intraday_lookback['Close'])
        take_profit = (context.current_time == position['entry_time'] + datetime.timedelta(minutes=10) and
                       len(intraday_closes) >= 3 and intraday_closes[-1] < intraday_closes[-3])
        # If profit isn't taken after 10 min and the last bar still increases
        stop_loss = (context.current_time == position['entry_time'] + datetime.timedelta(minutes=15) and
                     len(intraday_closes) >= 2 and intraday_closes[-1] > intraday_closes[-2])
        is_close = (take_profit or stop_loss or
                    context.current_time >= position['entry_time'] + datetime.timedelta(minutes=20) or
                    context.current_time.time() >= EXIT_TIME)
        if is_close:
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               f'Closing position. Current price {context.current_price}.')
            position['status'] = PositionStatus.CLOSED
            return ProcessorAction(context.symbol, ActionType.BUY_TO_CLOSE, 1)


class L2hProcessorFactory(ProcessorFactory):
    processor_class = L2hProcessor
