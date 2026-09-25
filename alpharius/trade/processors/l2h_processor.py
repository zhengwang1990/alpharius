import datetime
from typing import override
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from alpharius.data import DataClient

from ..common import (
    DAYS_IN_A_MONTH,
    DAYS_IN_A_YEAR,
)
from ..enums import ActionType, Mode, PositionStatus, TradingFrequency
from ..stock_universe import IntradayVolatilityStockUniverse
from ..structs import Context, Position, ProcessorAction
from .processor import Processor

NUM_UNIVERSE_SYMBOLS = 25
EXIT_TIME = datetime.time(16, 0)


class L2hProcessor(Processor):
    def __init__(
        self,
        lookback_start_date: pd.Timestamp,
        lookback_end_date: pd.Timestamp,
        data_client: DataClient,
        output_dir: str,
        logging_timezone: ZoneInfo | None = None,
    ) -> None:
        super().__init__(output_dir, logging_timezone)
        self._positions = dict()
        self._stock_universe = IntradayVolatilityStockUniverse(
            lookback_start_date, lookback_end_date, data_client, num_stocks=NUM_UNIVERSE_SYMBOLS
        )

    @override
    def get_trading_frequency(self) -> TradingFrequency:
        return TradingFrequency.FIVE_MIN

    @override
    def setup(self, hold_positions: list[Position], current_time: pd.Timestamp | None) -> None:
        to_remove = [symbol for symbol, position in self._positions.items() if position['status'] != 'active']
        for symbol in to_remove:
            self._positions.pop(symbol)

    @override
    def get_stock_universe(self, view_time: pd.Timestamp) -> list[str]:
        return list(set(self._stock_universe.get_stock_universe(view_time) + list(self._positions.keys())))

    @override
    def process_data(self, context: Context) -> ProcessorAction | None:
        if self.is_active(context.symbol):
            return self._close_position(context)
        elif context.symbol not in self._positions:
            return self._open_position(context)

    def _open_position(self, context: Context) -> ProcessorAction | None:
        t = context.current_time.time()
        if t >= EXIT_TIME:
            return
        interday_closes = context.interday_lookback['Close'].to_numpy()
        if len(interday_closes) < DAYS_IN_A_YEAR:
            return
        if context.current_price > interday_closes[-DAYS_IN_A_MONTH] * 2:
            return
        if context.current_price / context.prev_day_close - 1 > 0.2:
            return
        last_month_closes = interday_closes[-DAYS_IN_A_MONTH:]
        if (
            abs(context.current_price / min(last_month_closes) - 1) > 0.5
            or abs(context.current_price / max(last_month_closes) - 1) > 0.5
        ):
            return
        if interday_closes[-1] / interday_closes[-2] - 1 > 0.05:
            return
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        intraday_closes = context.intraday_lookback['Close'].values[market_open_index:]
        intraday_opens = context.intraday_lookback['Open'].values[market_open_index:]
        if len(intraday_closes) < 10:
            return
        if intraday_closes[-2] <= intraday_opens[-2] or intraday_closes[-1] <= intraday_opens[-1]:
            return

        for i in range(-3, -8, -1):
            if intraday_closes[i] <= intraday_closes[i - 1] and intraday_closes[i] <= intraday_opens[i]:
                break
        else:
            return

        bar_sizes = [
            intraday_closes[i] - intraday_opens[i]
            for i in range(len(intraday_closes))
            if intraday_closes[i] > intraday_opens[i]
        ]
        if bar_sizes[-2] < 2 * np.median(bar_sizes):
            return
        if bar_sizes[-1] > bar_sizes[-2]:
            return

        if t >= datetime.time(14, 0) and len(intraday_closes) > 36:
            afternoon_change = context.current_price - min(intraday_closes[36:])
            morning_change = max(intraday_closes[12:36]) - min(intraday_closes[12:36])
            if afternoon_change > 4 * morning_change:
                return

        intraday_highs = context.intraday_lookback['High'].values[market_open_index:]
        if intraday_highs[-1] - intraday_closes[-1] > 0.8 * (intraday_closes[-1] - intraday_opens[-1]):
            return

        current_gain = context.current_price / intraday_closes[-10] - 1
        ratio = 0.75
        if context.current_price > context.prev_day_close > intraday_closes[-10]:
            ratio = 0.8
        threshold = context.l2h_avg * ratio

        is_trade = threshold < current_gain < 1.33 * threshold
        if is_trade or (context.mode == Mode.TRADE and current_gain > threshold * 0.8):
            self._logger.debug(
                f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                f'Current gain: {current_gain * 100:.2f}%. '
                f'Threshold: {threshold * 100:.2f}%. '
                f'Current price {context.current_price}.'
            )
        if is_trade:
            self._positions[context.symbol] = {'entry_time': context.current_time, 'status': PositionStatus.PENDING}
            return ProcessorAction(context.symbol, ActionType.SELL_TO_OPEN, 1)

    def _close_position(self, context: Context) -> ProcessorAction | None:
        position = self._positions[context.symbol]
        intraday_closes = list(context.intraday_lookback['Close'])
        take_profit = (
            context.current_time == position['entry_time'] + datetime.timedelta(minutes=10)
            and len(intraday_closes) >= 3
            and intraday_closes[-1] < intraday_closes[-3]
        )
        # If profit isn't taken after 10 min and the last bar still increases
        stop_loss = (
            context.current_time == position['entry_time'] + datetime.timedelta(minutes=15)
            and len(intraday_closes) >= 2
            and intraday_closes[-1] > intraday_closes[-2]
        )
        is_close = (
            take_profit
            or stop_loss
            or context.current_time >= position['entry_time'] + datetime.timedelta(minutes=20)
            or context.current_time.time() >= EXIT_TIME
        )
        if is_close:
            self._logger.debug(
                f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                f'Closing position. Current price {context.current_price}.'
            )
            position['status'] = PositionStatus.CLOSED
            return ProcessorAction(context.symbol, ActionType.BUY_TO_CLOSE, 1)
