import datetime
from typing import override

import numpy as np
import pandas as pd

from alpharius.data import DataClient

from ..enums import ActionType, PositionStatus, TradingFrequency
from ..stock_universe import IntradayVolatilityStockUniverse
from ..structs import Context, Position, ProcessorAction
from .processor import Processor


class FirstRedDayProcessor(Processor):
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
        return list(set(self._stock_universe.get_stock_universe(view_time) + list(self._positions.keys())))

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
        if context.current_time.time() < datetime.time(9, minute=45):
            return
        interday_opens = context.interday_lookback['Open'].to_numpy()
        interday_closes = context.interday_lookback['Close'].to_numpy()
        if (
            interday_closes[-1] > interday_closes[-2]
            or interday_closes[-1] > interday_opens[-1]
            or interday_closes[-2] < interday_opens[-2]
            or interday_closes[-2] < interday_closes[-3]
        ):
            self._skip_cache.add((context.symbol, context.current_time.date()))
            return
        if interday_closes[-2] / interday_closes[-5] - 1 < context.l2h_avg * 2:
            self._skip_cache.add((context.symbol, context.current_time.date()))
            return
        for i in range(-4, -1):
            if interday_closes[i] < interday_closes[i - 1]:
                self._skip_cache.add((context.symbol, context.current_time.date()))
                return
        if interday_closes[-1] < 0.65 * np.max(interday_closes[-60:-1]):
            self._skip_cache.add((context.symbol, context.current_time.date()))
            return
        interday_highs = context.interday_lookback['High'].to_numpy()
        interday_lows = context.interday_lookback['Low'].to_numpy()
        true_ranges = [
            max(
                interday_highs[i] - interday_lows[i],
                abs(interday_highs[i] - interday_closes[i - 1]),
                abs(interday_lows[i] - interday_closes[i - 1]),
            )
            for i in range(-4, -1)
        ]
        if true_ranges[-1] < np.average(true_ranges[:-1]) * 1.5:
            self._skip_cache.add((context.symbol, context.current_time.date()))
            return

        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        intraday_opens = context.intraday_lookback['Open'].to_numpy()[market_open_index:]
        intraday_closes = context.intraday_lookback['Close'].to_numpy()[market_open_index:]
        if len(intraday_closes) < 3:
            return
        if intraday_opens[0] > context.prev_day_close:
            return
        open_range_last_ind = min(len(intraday_closes) - 1, 5)
        if intraday_closes[open_range_last_ind] > intraday_opens[0]:
            return
        levels = (min(np.min(intraday_closes[:3]), np.min(intraday_opens[:3])), context.prev_day_close)
        if any(intraday_closes[-1] < level < intraday_closes[-2] for level in levels):
            self._positions[context.symbol] = {
                'side': 'short',
                'entry_time': context.current_time,
                'status': PositionStatus.PENDING,
            }
            self._skip_cache.add((context.symbol, context.current_time.date()))
            return ProcessorAction(context.symbol, ActionType.SELL_TO_OPEN, 1)

    def _close_position(self, context: Context) -> ProcessorAction | None:
        position = self._positions[context.symbol]
        if context.current_time >= position['entry_time'] + datetime.timedelta(
            minutes=35
        ) or context.current_time.time() >= datetime.time(16, 0):
            self._logger.debug(
                f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                f'Closing position. Current price {context.current_price}.'
            )
            self._positions.pop(context.symbol)
            return ProcessorAction(context.symbol, ActionType.BUY_TO_CLOSE, 1)
