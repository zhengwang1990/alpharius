import datetime
from typing import override

import numpy as np
import pandas as pd

from alpharius.data import DataClient
from alpharius.trade.common import DAYS_IN_A_QUARTER

from ..enums import ActionType, PositionStatus, TradingFrequency
from ..stock_universe import IntradayVolatilityStockUniverse
from ..structs import Context, Position, ProcessorAction
from .processor import Processor

NUM_UNIVERSE_SYMBOLS = 20
EXIT_TIME = datetime.time(16, 0)


class ExpProcessor(Processor):
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

    @override
    def process_data(self, context: Context) -> ProcessorAction | None:
        if self.is_active(context.symbol):
            return self._close_position(context)
        elif context.symbol not in self._positions:
            return self._open_position(context)

    def _open_position(self, context: Context) -> ProcessorAction | None:
        if context.current_time.time() < datetime.time(10, 30):
            return
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        interday_closes = context.interday_lookback['Close'].values
        quaterly_max = max(interday_closes[-DAYS_IN_A_QUARTER:])

        intraday_opens = context.intraday_lookback['Open'].to_numpy()[market_open_index:]
        intraday_closes = context.intraday_lookback['Close'].to_numpy()[market_open_index:]

        if interday_closes[-1] > quaterly_max * 0.7:
            return

        if intraday_opens[0] > context.prev_day_close * 0.97:
            return
        if intraday_opens[0] < context.prev_day_close * 0.9:
            return

        open_max = max(*intraday_closes[:6], *intraday_opens[:6])
        if open_max > context.prev_day_close * 0.98:
            return

        if not intraday_closes[-1] > open_max > intraday_opens[-1]:
            return

        bar_sizes = [abs(intraday_closes[i] - intraday_opens[i]) for i in range(-6, 0)]
        if bar_sizes[-1] > 2 * np.median(bar_sizes[:-1]):
            return
        if bar_sizes[-1] > 2 * bar_sizes[-2]:
            return

        self._logger.debug(
            f'[{context.current_time.strftime("%F %H:%M")}] Low open high close strategy. '
            f'Current price: {context.current_price}.'
        )
        self._positions[context.symbol] = {
            'entry_time': context.current_time,
            'status': PositionStatus.PENDING,
            'side': 'long',
        }
        return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)

    def _close_position(self, context: Context) -> ProcessorAction | None:
        position = self._positions[context.symbol]
        if context.current_time >= position['entry_time'] + datetime.timedelta(
            minutes=20
        ) or context.current_time.time() >= datetime.time(16, 0):
            position['status'] = PositionStatus.CLOSED
            return ProcessorAction(context.symbol, ActionType.SELL_TO_CLOSE, 1)
