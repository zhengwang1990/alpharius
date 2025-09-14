import datetime
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

from alpharius.data import DataClient
from ..common import (
    ActionType, Context, Processor, ProcessorFactory, TradingFrequency,
    Position, PositionStatus, ProcessorAction, Mode, DAYS_IN_A_MONTH)
from ..stock_universe import IntradayVolatilityStockUniverse

NUM_UNIVERSE_SYMBOLS = 30
EXIT_TIME = datetime.time(13, 0)


class O2lProcessor(Processor):
    """Open to low processor predicts intraday lows based on market open prices."""

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

    def _get_thresholds(self, context: Context) -> Tuple[float, float]:
        key = context.symbol + context.current_time.strftime('%F')
        if key in self._memo:
            return self._memo[key]
        interday_opens = context.interday_lookback['Open'][-DAYS_IN_A_MONTH:]
        interday_lows = context.interday_lookback['Low'][-DAYS_IN_A_MONTH:]
        o2l_losses = [l / o - 1 for o, l in zip(interday_opens, interday_lows)]
        o2l_avg = np.average(o2l_losses)
        o2l_std = np.std(o2l_losses)
        upper_threshold = o2l_avg - 4 * o2l_std
        lower_threshold = max(o2l_avg - 5.5 * o2l_std, -0.4)
        res = (lower_threshold, upper_threshold)
        return res

    def _open_position(self, context: Context) -> Optional[ProcessorAction]:
        t = context.current_time.time()
        if t >= EXIT_TIME:
            return
        interday_closes = context.interday_lookback['Close']
        if context.current_price < interday_closes.iloc[-DAYS_IN_A_MONTH]:
            return
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        intraday_opens = context.intraday_lookback['Open'].to_numpy()[market_open_index:]
        market_open_price = intraday_opens[0]
        intraday_closes = context.intraday_lookback['Close'].to_numpy()[market_open_index:]
        if intraday_closes[-1] > np.min(intraday_closes):
            return
        if context.current_price < context.prev_day_close < market_open_price and t <= datetime.time(10, 0):
            return
        for i in range(len(intraday_closes) - 1):
            if intraday_closes[i] > intraday_opens[i]:
                break
        else:
            return
        current_loss = context.current_price / market_open_price - 1
        lower_threshold, upper_threshold = self._get_thresholds(context)
        is_trade = lower_threshold < current_loss < upper_threshold
        if is_trade or (context.mode == Mode.TRADE and current_loss < upper_threshold * 0.8):
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               f'Current loss: {current_loss * 100:.2f}%. '
                               f'Threshold: {lower_threshold * 100:.2f}% ~ {upper_threshold * 100:.2f}%. '
                               f'Current price: {context.current_price}.')
        if is_trade:
            self._positions[context.symbol] = {'entry_time': context.current_time,
                                               'status': PositionStatus.PENDING}
            return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)

    def _close_position(self, context: Context) -> Optional[ProcessorAction]:
        position = self._positions[context.symbol]
        intraday_closes = context.intraday_lookback['Close'].tolist()
        elapsed_fifteen = context.current_time == position['entry_time'] + datetime.timedelta(minutes=15)
        take_profit = elapsed_fifteen and len(intraday_closes) >= 4 and intraday_closes[-1] > intraday_closes[-4]
        early_stop = (elapsed_fifteen and len(intraday_closes) >= 4 and
                      intraday_closes[-4] < context.prev_day_close < context.today_open)
        is_close = (take_profit or early_stop or
                    context.current_time >= position['entry_time'] + datetime.timedelta(minutes=30) or
                    context.current_time.time() >= EXIT_TIME)
        self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                           f'Closing position: {is_close}. Current price: {context.current_price}.')
        if is_close:
            position['status'] = PositionStatus.CLOSED
            return ProcessorAction(context.symbol, ActionType.SELL_TO_CLOSE, 1)


class O2lProcessorFactory(ProcessorFactory):
    processor_class = O2lProcessor
