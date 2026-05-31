import datetime
from typing import List, Optional
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from alpharius.data import DataClient

from ..common import DAYS_IN_A_MONTH
from ..enums import ActionType, PositionStatus, TradingFrequency
from ..stock_universe import IntradayVolatilityStockUniverse
from ..structs import Context, Position, ProcessorAction
from .processor import Processor


class OpenRangeProcessor(Processor):
    """The first hour of trading often dertemines the sentiment for the rest of the day."""

    def __init__(
        self,
        lookback_start_date: pd.Timestamp,
        lookback_end_date: pd.Timestamp,
        data_client: DataClient,
        output_dir: str,
        logging_timezone: Optional[ZoneInfo] = None,
    ) -> None:
        super().__init__(output_dir, logging_timezone)
        self._positions = dict()
        self._stock_universe = IntradayVolatilityStockUniverse(
            lookback_start_date, lookback_end_date, data_client, num_stocks=15, num_top_volume=50
        )

    def get_trading_frequency(self) -> TradingFrequency:
        return TradingFrequency.FIVE_MIN

    def setup(self, hold_positions: List[Position], current_time: Optional[pd.Timestamp]) -> None:
        to_remove = [
            symbol for symbol, position in self._positions.items() if position['status'] != PositionStatus.ACTIVE
        ]
        for symbol in to_remove:
            self._positions.pop(symbol)

    def get_stock_universe(self, view_time: pd.Timestamp) -> List[str]:
        return list(set(self._stock_universe.get_stock_universe(view_time) + list(self._positions.keys())))

    def process_data(self, context: Context) -> Optional[ProcessorAction]:
        if self.is_active(context.symbol):
            return self._close_position(context)
        elif context.symbol not in self._positions:
            action = self._open_long_position(context)
            if action:
                return action

    def _open_long_position(self, context: Context) -> Optional[ProcessorAction]:
        n_long = 6
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        if context.current_time.time() <= datetime.time(10, 45):
            return
        if context.current_time.time() >= datetime.time(14, 30):
            return
        intraday_closes = context.intraday_lookback['Close'].to_numpy()[market_open_index:]
        if len(intraday_closes) < n_long + 1:
            return
        min_range, max_range = self._get_range(context)
        open_range = max_range - min_range
        if open_range < 0.03 * context.prev_day_close:
            return
        if open_range > 2 * context.l2h_avg * context.prev_day_close:
            # Too much volatility
            return
        if max_range > context.prev_day_close > min_range:
            return
        level = min_range
        if not intraday_closes[-2] < level < intraday_closes[-1]:
            return
        interday_closes = context.interday_lookback['Close'].to_numpy()
        if context.prev_day_close > min(interday_closes[-5:]) * 1.3:
            return
        if (
            context.current_price / context.prev_day_close - 1 < context.h2l_avg
            and context.current_price / min(interday_closes[-DAYS_IN_A_MONTH:]) - 1 > 0.4
        ):
            # Pull back from recent high
            return
        if context.current_price < max(interday_closes[-DAYS_IN_A_MONTH:]) * 0.6:
            # Too much drop from recent high
            return
        n_dec = sum(intraday_closes[i] < intraday_closes[i - 1] for i in range(-n_long, 0))
        if context.current_price / intraday_closes[-n_long] - 1 < context.l2h_avg * 0.3:
            if n_dec > 0:
                return
        else:
            if n_dec > 2:
                return
        intraday_opens = context.intraday_lookback['Open'].iloc[market_open_index:].to_numpy()
        for i in range(len(intraday_closes) - n_long):
            if intraday_closes[i] > level and intraday_closes[i] > intraday_opens[i]:
                break
        else:
            return
        bar_sizes = [
            intraday_closes[i] - intraday_closes[i - 1]
            for i in range(-n_long, 0)
            if intraday_closes[i] - intraday_closes[i - 1] > 0
        ]
        if bar_sizes[-1] > 2.5 * np.median(bar_sizes):
            return
        if context.current_time.time() < datetime.time(10, minute=30) and bar_sizes[-1] == max(bar_sizes):
            return
        self._logger.debug(
            f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
            f'Level: {level}. '
            f'Current price: {context.current_price}. Side: long.'
        )
        self._positions[context.symbol] = {
            'entry_time': context.current_time,
            'status': PositionStatus.PENDING,
            'side': 'long',
        }
        return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)

    def _close_position(self, context: Context) -> Optional[ProcessorAction]:
        position = self._positions[context.symbol]
        side = position['side']
        is_close = False
        if side == 'long':
            is_close = context.current_time >= position['entry_time'] + datetime.timedelta(
                minutes=60
            ) or context.current_time.time() >= datetime.time(16, 0)
            if not is_close:
                intraday_closes = context.intraday_lookback['Close'].to_numpy()
                if (
                    intraday_closes[-1]
                    < intraday_closes[-2]
                    < intraday_closes[-3]
                    < intraday_closes[-4]
                    < intraday_closes[-5]
                ):
                    level, _ = self._get_range(context)
                    if context.current_price < level * 0.99:
                        is_close = True
        if is_close:
            self._logger.debug(
                f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                f'Closing position. Current price {context.current_price}.'
            )
            position['status'] = PositionStatus.CLOSED
            action_type = ActionType.BUY_TO_CLOSE if side == 'short' else ActionType.SELL_TO_CLOSE
            return ProcessorAction(context.symbol, action_type, 1)

    def _get_range(self, context: Context) -> tuple[float, float]:
        market_open_index = context.market_open_index
        intraday_closes = context.intraday_lookback['Close'].to_numpy()[market_open_index:]
        intraday_opens = context.intraday_lookback['Open'].to_numpy()[market_open_index:]
        max_range = max(max(intraday_closes[:12]), max(intraday_opens[:12]))
        min_range = min(min(intraday_closes[:12]), min(intraday_opens[:12]))
        return min_range, max_range
