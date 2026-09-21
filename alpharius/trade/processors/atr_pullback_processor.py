import datetime
from typing import override
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

from alpharius.data import DataClient
from alpharius.trade.common import DAYS_IN_A_WEEK

from ..enums import ActionType, PositionStatus, TradingFrequency
from ..stock_universe import IntradayVolatilityStockUniverse
from ..structs import Context, Position, ProcessorAction
from .processor import Processor

EXIT_TIME = datetime.time(16, 0)
RANGE_BARS = 12
ATR_DAYS = 14
ATR_PULLBACK_MULTIPLE = 0.8
BAR_SIZE_LOOKBACK = 12
BAR_SIZE_MULTIPLE = 2.5


class AtrPullbackProcessor(Processor):
    """Buys the pullback after a break out of the first hour range, and sells the bounce after a break down."""

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
            lookback_start_date,
            lookback_end_date,
            data_client,
            num_stocks=15,
            num_top_volume=50,
        )

    @override
    def get_trading_frequency(self) -> TradingFrequency:
        return TradingFrequency.FIVE_MIN

    @override
    def setup(self, hold_positions: list[Position], current_time: pd.Timestamp | None) -> None:
        to_remove = [
            symbol for symbol, position in self._positions.items() if position['status'] != PositionStatus.ACTIVE
        ]
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
        current_time = context.current_time.time()
        if (
            current_time < datetime.time(11, 0)
            or current_time >= datetime.time(14, 30)
            or datetime.time(13, 0) <= current_time <= datetime.time(14, 0)
        ):
            return
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        atr = context.atr(ATR_DAYS)

        # Too much volatility
        interday_closes = context.interday_lookback['Close'].values
        if max(context.current_price, *interday_closes[-DAYS_IN_A_WEEK * 2 :]) > 3 * min(
            context.current_price, *interday_closes[-DAYS_IN_A_WEEK * 2 :]
        ):
            return

        bars = context.intraday_lookback.iloc[market_open_index:]
        if len(bars) <= RANGE_BARS:
            return
        opens = bars['Open'].to_numpy()
        closes = bars['Close'].to_numpy()
        range_high = max(max(closes[:RANGE_BARS]), max(opens[:RANGE_BARS]))
        range_low = min(min(closes[:RANGE_BARS]), min(opens[:RANGE_BARS]))
        intraday_high = bars['High'].max()
        intraday_low = bars['Low'].min()
        price = context.current_price
        if price > range_high and price < intraday_high - ATR_PULLBACK_MULTIPLE * atr and closes[-1] > opens[-1]:
            side = 'long'
            level = range_high
        elif (
            price < range_low
            and price > intraday_low + ATR_PULLBACK_MULTIPLE * atr
            and (closes[-1] < opens[-1] or (closes[-2] < opens[-2] and closes[-3] < opens[-3]))
        ):
            side = 'short'
            level = range_low
        else:
            return

        interday_opens = context.interday_lookback['Open'].values
        if (
            max(abs(interday_closes[-1] - interday_closes[-2]), abs(interday_closes[-1] - interday_opens[-1]))
            > 2.6 * atr
        ):
            if side == 'long' and (opens[0] < context.prev_day_close and opens[0] > closes[RANGE_BARS]):
                self._logger.debug(
                    f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] Downtrend. Skip long.'
                )
                return
            elif side == 'short' and (opens[0] > context.prev_day_close and opens[0] < closes[RANGE_BARS]):
                self._logger.debug(
                    f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] Uptrend. Skip short.'
                )
                return

        bar_sizes = np.abs(closes - opens)[-BAR_SIZE_LOOKBACK:]
        median_size = np.median(bar_sizes)
        if bar_sizes[-1] > BAR_SIZE_MULTIPLE * median_size:
            return

        sign = -1 if side == 'long' else 1
        diffs = [(closes[i] - opens[i]) * sign for i in range(-2, -6, -1)]
        if all(d > 0 for d in diffs) and any(d > median_size for d in diffs):
            return

        self._logger.debug(
            f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
            f'Level: {level}. ATR: {atr:.4f}. Intraday range: [{intraday_low}, {intraday_high}]. '
            f'Current price: {price}. Side: {side}.'
        )
        self._positions[context.symbol] = {
            'entry_time': context.current_time,
            'status': PositionStatus.PENDING,
            'side': side,
            'level': level,
        }
        action_type = ActionType.BUY_TO_OPEN if side == 'long' else ActionType.SELL_TO_OPEN
        return ProcessorAction(context.symbol, action_type, 1)

    def _close_position(self, context: Context) -> ProcessorAction | None:
        position = self._positions[context.symbol]
        side = position['side']
        level = position['level']
        is_close = (
            context.current_time >= position['entry_time'] + datetime.timedelta(minutes=60)
            or context.current_time.time() >= EXIT_TIME
        )
        take_profit_time = context.current_time >= position['entry_time'] + datetime.timedelta(minutes=40)
        if not is_close:
            bars = context.intraday_lookback
            market_open_index = context.market_open_index
            if market_open_index is not None:
                bars = bars.iloc[market_open_index:]
            closes = bars['Close'].to_numpy()
            if len(closes) >= 5:
                entry_index = len(closes) - 1 - (context.current_time - position['entry_time']).seconds // 300
                atr = context.atr(ATR_DAYS)
                entry_price = None
                if 0 <= entry_index < len(closes):
                    entry_price = closes[entry_index]
                if side == 'long':
                    is_close = closes[-1] < closes[-2] < closes[-3] < closes[-4] < closes[-5] and (
                        context.current_price < level * 0.99
                    )
                    if entry_price is not None:
                        is_close = is_close or (
                            context.current_price > entry_price * 1.001
                            and context.current_price > bars['High'].max() - 0.2 * atr
                        )
                        if not is_close and take_profit_time:
                            change_pct = context.current_price / entry_price - 1
                            if 0.01 > change_pct > 0.002 and max(closes[-6:]) / min(closes[-6:]) < 1.01:
                                is_close = True
                else:
                    is_close = closes[-1] > closes[-2] > closes[-3] > closes[-4] > closes[-5] and (
                        context.current_price > level * 1.01
                    )
                    if entry_price is not None:
                        is_close = is_close or (
                            context.current_price < entry_price * 0.999
                            and context.current_price < bars['Low'].min() + 0.1 * atr
                        )

        if is_close:
            self._logger.debug(
                f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                f'Closing position. Current price {context.current_price}.'
            )
            position['status'] = PositionStatus.CLOSED
            action_type = ActionType.BUY_TO_CLOSE if side == 'short' else ActionType.SELL_TO_CLOSE
            return ProcessorAction(context.symbol, action_type, 1)
