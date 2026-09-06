import datetime
from typing import override

import numpy as np
import pandas as pd

from alpharius.data import DataClient

from ..enums import ActionType, PositionStatus, TradingFrequency
from ..stock_universe import IntradayVolatilityStockUniverse
from ..structs import Context, Position, ProcessorAction
from .processor import Processor


class FirstGreenDayProcessor(Processor):
    def __init__(
        self,
        lookback_start_date: pd.Timestamp,
        lookback_end_date: pd.Timestamp,
        data_client: DataClient,
        output_dir: str,
    ) -> None:
        super().__init__(output_dir)
        self._positions = dict()
        self._stock_universe = IntradayVolatilityStockUniverse(
            lookback_start_date, lookback_end_date, data_client, num_stocks=10, num_top_volume=50
        )
        self._skip_cache = set()

    @override
    def get_trading_frequency(self) -> TradingFrequency:
        return TradingFrequency.FIVE_MIN

    @override
    def get_stock_universe(self, view_time: pd.Timestamp) -> list[str]:
        return list(self._stock_universe.get_stock_universe(view_time) + list(self._positions.keys()))

    @override
    def setup(self, hold_positions: list[Position], current_time: pd.Timestamp | None) -> None:
        to_remove = [
            symbol for symbol, position in self._positions.items() if position['status'] != PositionStatus.ACTIVE
        ]
        for symbol in to_remove:
            self._positions.pop(symbol)
        self._skip_cache.clear()

    @override
    def process_data(self, context: Context) -> ProcessorAction | None:
        if self.is_active(context.symbol):
            return self._close_position(context)
        elif context.symbol not in self._positions:
            return self._open_position(context)

    def _open_position(self, context: Context) -> ProcessorAction | None:
        if (context.symbol, context.current_time.date()) in self._skip_cache:
            return
        if context.current_time.time() <= datetime.time(10, 0):
            return
        if context.current_time.time() > datetime.time(14, 0):
            return
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        interday_highs = context.interday_lookback['High'].to_numpy()
        interday_lows = context.interday_lookback['Low'].to_numpy()
        interday_closes = context.interday_lookback['Close'].to_numpy()
        interday_opens = context.interday_lookback['Open'].to_numpy()
        if len(interday_closes) < 15:
            return
        true_ranges = [
            max(
                interday_highs[i] - interday_lows[i],
                abs(interday_highs[i] - interday_closes[i - 1]),
                abs(interday_lows[i] - interday_closes[i - 1]),
            )
            for i in range(-14, -4)
        ]
        atr = np.mean(true_ranges)
        if (
            interday_closes[-1] > interday_closes[-2]
            or interday_closes[-2] > interday_closes[-3]
            or interday_closes[-3] > interday_closes[-4]
        ):
            self._skip_cache.add((context.symbol, context.current_time.date()))
            return
        if interday_closes[-3] - interday_closes[-2] < atr and interday_closes[-4] - interday_closes[-3] < atr:
            self._skip_cache.add((context.symbol, context.current_time.date()))
            return
        if interday_closes[-1] < interday_opens[-1]:
            self._skip_cache.add((context.symbol, context.current_time.date()))
            return
        if interday_closes[-2] > interday_opens[-2]:
            self._skip_cache.add((context.symbol, context.current_time.date()))
            return
        daily_changes = [interday_closes[i] - interday_closes[i - 1] for i in range(-14, -4)]
        daily_changes.sort(key=lambda x: abs(x), reverse=True)
        if daily_changes[0] < 0 and daily_changes[1] < 0 and -daily_changes[0] > atr and -daily_changes[1] > atr:
            self._skip_cache.add((context.symbol, context.current_time.date()))
            return
        intraday_closes = context.intraday_lookback['Close'].to_numpy()[market_open_index:]
        intraday_opens = context.intraday_lookback['Open'].to_numpy()[market_open_index:]
        if len(intraday_closes) < 3:
            return
        abs_changes = np.abs(np.diff(intraday_closes[-10:]))
        if not (intraday_opens[0] - intraday_closes[-1] > 0.5 * atr and abs_changes[-1] < np.mean(abs_changes)):
            return
        recent_range = np.max(intraday_closes[-10:]) - np.min(intraday_closes[-10:])
        intraday_min = np.min(intraday_closes)
        all_range = np.max(intraday_closes) - intraday_min
        if recent_range > 0.25 * all_range:
            return
        if context.current_price - intraday_min > 0.4 * all_range:
            return
        direction = 'long'
        if intraday_opens[0] > context.prev_day_close:
            if context.current_time.time() >= datetime.time(12, 0):
                self._skip_cache.add((context.symbol, context.current_time.date()))
                return
            if intraday_closes[0] > context.prev_day_close and np.min(intraday_closes) < context.prev_day_close:
                return
            direction = 'short'
        self._logger.debug(
            f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
            f'Opening position. Current price {context.current_price}. Direction: {direction}. ATR: {atr}.'
        )
        self._positions[context.symbol] = {
            'side': direction,
            'entry_time': context.current_time,
            'status': PositionStatus.PENDING,
        }
        self._skip_cache.add((context.symbol, context.current_time.date()))
        if direction == 'long':
            return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)
        else:
            return ProcessorAction(context.symbol, ActionType.SELL_TO_OPEN, 1)

    def _close_position(self, context: Context) -> ProcessorAction | None:
        position = self._positions[context.symbol]
        if context.current_time >= position['entry_time'] + datetime.timedelta(
            minutes=30
        ) or context.current_time.time() >= datetime.time(16, 0):
            self._logger.debug(
                f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                f'Closing position. Current price {context.current_price}.'
            )
            self._positions.pop(context.symbol)
            if position['side'] == 'long':
                return ProcessorAction(context.symbol, ActionType.SELL_TO_CLOSE, 1)
            else:
                return ProcessorAction(context.symbol, ActionType.BUY_TO_CLOSE, 1)
