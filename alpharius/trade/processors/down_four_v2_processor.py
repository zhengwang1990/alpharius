import datetime
from typing import List, Optional

import numpy as np
import pandas as pd

from alpharius.data import DataClient
from ..common import (
    ActionType, Context, Processor, ProcessorFactory, TradingFrequency,
    Position, PositionStatus, ProcessorAction, Mode)
from ..stock_universe import L2hVolatilityStockUniverse

N = 4


class DownFourV2Processor(Processor):

    def __init__(self,
                 lookback_start_date: pd.Timestamp,
                 lookback_end_date: pd.Timestamp,
                 data_client: DataClient,
                 output_dir: str) -> None:
        super().__init__(output_dir)
        self._positions = dict()
        self._stock_universe = L2hVolatilityStockUniverse(lookback_start_date,
                                                          lookback_end_date,
                                                          data_client)

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
        elif context.symbol not in self._positions:
            return self._open_position(context)

    def _open_position(self, context: Context) -> Optional[ProcessorAction]:
        t = context.current_time.time()
        if t >= datetime.time(15, 40):
            return
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        if context.current_price > context.prev_day_close * 1.15:
            return

        intraday_closes = context.intraday_lookback['Close'].tolist()[market_open_index:]
        intraday_opens = context.intraday_lookback['Open'].tolist()[market_open_index:]
        intraday_vols = context.intraday_lookback['Volume'].tolist()[market_open_index:]
        h2l_avg = context.h2l_avg

        # Filters
        if len(intraday_closes) < N + 1:
            return
        try:
            bar_losses = np.array([intraday_closes[i] / intraday_opens[i] - 1 for i in range(-N, 0)])
        except ZeroDivisionError:
            return
        if any(bar_losses > 0):
            return
        if any(bar_losses < -0.05):
            return
        if intraday_vols[-2] > intraday_vols[-3]:
            return
        try:
            all_bars = np.array([abs(intraday_closes[i] / intraday_closes[i - 1] - 1)
                                 for i in range(1, len(intraday_closes))])
        except ZeroDivisionError:
            return
        if all_bars[-1] > np.percentile(all_bars, 95) * 4:
            return

        current_bar_loss = bar_losses[-1]
        current_high = context.intraday_lookback['High'].iloc[-1]
        current_low = context.intraday_lookback['Low'].iloc[-1]
        current_bar_range = current_low / current_high - 1
        prev_bar_loss = bar_losses[-2]

        is_trade = (0.03 < current_bar_loss / h2l_avg < 0.1
                    and current_bar_range / h2l_avg < 0.35
                    and prev_bar_loss / h2l_avg > 0.2)
        if is_trade or (context.mode == Mode.TRADE and prev_bar_loss / h2l_avg > 0.18):
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               f'Prev loss: {prev_bar_loss * 100:.2f}%. '
                               f'Current loss: {current_bar_loss * 100:.2f}%. '
                               f'H2l: {h2l_avg * 100:.2f}%. Current price {context.current_price}.')
        if is_trade:
            self._positions[context.symbol] = {'entry_time': context.current_time,
                                               'status': PositionStatus.PENDING}
            return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)

    def _close_position(self, context: Context) -> Optional[ProcessorAction]:
        position = self._positions[context.symbol]
        intraday_closes = context.intraday_lookback['Close'].tolist()
        is_close = (context.current_time >= position['entry_time'] + datetime.timedelta(minutes=15) and
                    len(intraday_closes) >= 4 and
                    (intraday_closes[-1] < intraday_closes[-2] or (intraday_closes[-1] >= intraday_closes[-4])))
        is_close = is_close or context.current_time >= position['entry_time'] + datetime.timedelta(minutes=20)
        self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                           f'Closing position: {is_close}. Current price {context.current_price}.')
        if is_close:
            position['status'] = PositionStatus.CLOSED
            return ProcessorAction(context.symbol, ActionType.SELL_TO_CLOSE, 1)


class DownFourV2ProcessorFactory(ProcessorFactory):
    processor_class = DownFourV2Processor
