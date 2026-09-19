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

NUM_UNIVERSE_SYMBOLS = 15
NUM_TOP_VOLUME_SYMBOLS = 50
EXIT_TIME = datetime.time(16, 0)
TQQQ_SYMBOL = 'TQQQ'


class GapDownBreakoutProcessor(Processor):
    """A stock gaps down at the open, then breaks above the high of its first half hour.

    Enter long on the breakout and hold for 20 minutes, extended to 40 minutes if the price has not moved, and
    stopped out on a 1% loss.
    """

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
            lookback_start_date,
            lookback_end_date,
            data_client,
            num_stocks=NUM_UNIVERSE_SYMBOLS,
            num_top_volume=NUM_TOP_VOLUME_SYMBOLS,
        )

    @override
    def get_trading_frequency(self) -> TradingFrequency:
        return TradingFrequency.FIVE_MIN

    @override
    def get_stock_universe(self, view_time: pd.Timestamp) -> list[str]:
        return list(
            set(self._stock_universe.get_stock_universe(view_time) + [TQQQ_SYMBOL] + list(self._positions.keys()))
        )

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
        if context.current_time.time() < datetime.time(11, 0):
            return
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        interday_closes = context.interday_lookback['Close'].values
        l2h_avg = context.l2h_avg

        if interday_closes[-1] / interday_closes[-2] - 1 > l2h_avg:
            return

        quarterly_max = max(interday_closes[-DAYS_IN_A_QUARTER:])

        intraday_opens = context.intraday_lookback['Open'].to_numpy()[market_open_index:]
        intraday_closes = context.intraday_lookback['Close'].to_numpy()[market_open_index:]

        if interday_closes[-1] > quarterly_max * 0.7:
            return

        # Use a fixed ratio for TQQQ
        ratio = 0.97 if context.symbol == TQQQ_SYMBOL else (1 - 0.75 * l2h_avg)
        if intraday_opens[0] > context.prev_day_close * ratio:
            return
        if intraday_opens[0] < context.prev_day_close * 0.9:
            return

        open_max = max(*intraday_closes[:6], *intraday_opens[:6])
        if open_max > context.prev_day_close * (1 - 0.25 * l2h_avg):
            return

        if not intraday_closes[-1] > open_max > intraday_opens[-1]:
            return

        bar_sizes = [abs(intraday_closes[i] - intraday_opens[i]) for i in range(-6, 0)]
        if bar_sizes[-1] > 2 * np.median(bar_sizes[:-1]):
            return
        if bar_sizes[-1] > 2 * bar_sizes[-2]:
            return

        recent_min = min(*intraday_closes[-6:], *intraday_opens[-6:])
        intraday_highs = context.intraday_lookback['High'].to_numpy()[market_open_index:]
        intraday_lows = context.intraday_lookback['Low'].to_numpy()[market_open_index:]
        day_range = max(intraday_highs) - min(intraday_lows)
        if context.current_price - recent_min > 0.4 * day_range:
            return

        self._logger.debug(
            f'[{context.current_time.strftime("%F %H:%M")}] Gap down breakout strategy. '
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
        exit_after_min = 20
        stop_loss = False
        intraday_closes = context.intraday_lookback['Close'].values
        entry_index = len(intraday_closes) - 1 - (context.current_time - position['entry_time']).seconds // 300
        if 0 <= entry_index < len(intraday_closes):
            entry_price = intraday_closes[entry_index]
            change = context.current_price / entry_price - 1
            if abs(change) < 0.002:
                exit_after_min = 40
            stop_loss = change < -0.01
        if (
            context.current_time >= position['entry_time'] + datetime.timedelta(minutes=exit_after_min)
            or context.current_time.time() >= EXIT_TIME
            or stop_loss
        ):
            position['status'] = PositionStatus.CLOSED
            return ProcessorAction(context.symbol, ActionType.SELL_TO_CLOSE, 1)
