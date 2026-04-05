import datetime
from typing import List, Optional

import pandas as pd

from alpharius.data import DataClient

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

    def get_trading_frequency(self) -> TradingFrequency:
        return TradingFrequency.FIVE_MIN

    def get_stock_universe(self, view_time: pd.Timestamp) -> List[str]:
        return list(set(self._stock_universe.get_stock_universe(view_time) + list(self._positions.keys())))

    def setup(self, hold_positions: List[Position], current_time: Optional[pd.Timestamp]) -> None:
        to_remove = [
            symbol for symbol, position in self._positions.items() if position['status'] != PositionStatus.ACTIVE
        ]
        for symbol in to_remove:
            self._positions.pop(symbol)

    def process_data(self, context: Context) -> Optional[ProcessorAction]:
        if self.is_active(context.symbol):
            return self._close_position(context)
        elif context.symbol not in self._positions:
            return self._open_position(context)

    def _open_position(self, context: Context) -> Optional[ProcessorAction]:
        if context.current_time.time() > datetime.time(10, 30):
            return
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        interday_opens = context.interday_lookback['Open'].to_numpy()
        interday_closes = context.interday_lookback['Close'].to_numpy()
        last_two_day_inc = all(interday_opens[i] < interday_closes[i - 1] < interday_closes[i] for i in range(-2, 0))
        last_six_day_inc = sum(interday_opens[i] < interday_closes[i] for i in range(-6, 0))
        last_six_day_inc_strict = sum(
            interday_opens[i] < interday_closes[i - 1] < interday_closes[i] for i in range(-6, 0)
        )
        if not (last_two_day_inc or (last_six_day_inc >= 5 and last_six_day_inc_strict >= 3)):
            return
        intraday_closes = context.intraday_lookback['Close'].tolist()[market_open_index:]
        intraday_opens = context.intraday_lookback['Open'].tolist()[market_open_index:]
        if context.current_price > intraday_opens[0]:
            return
        if context.current_price < intraday_opens[-1]:
            return
        intraday_lows = context.intraday_lookback['Low'].tolist()[market_open_index:-1] or [1]
        if (
            context.current_price / min(intraday_closes) - 1 > 0.005
            or context.current_price / min(intraday_lows) - 1 > 0.01
        ):
            self._logger.debug(
                f'[{context.current_time.strftime("%F %H:%M")}] Low open high close strategy. '
                f'Current price: {context.current_price}.'
            )
            self._positions[context.symbol] = {
                'entry_time': context.current_time,
                'strategy': 'low_open_high_close',
                'side': 'long',
            }
            return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)

    def _close_position(self, context: Context) -> Optional[ProcessorAction]:
        def exit_position():
            self._logger.debug(
                f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                f'Closing position. Current price {context.current_price}.'
            )
            self._positions.pop(context.symbol)
            return action

        position = self._positions[context.symbol]
        side = position['side']
        action_type = ActionType.SELL_TO_CLOSE if side == 'long' else ActionType.BUY_TO_CLOSE
        action = ProcessorAction(context.symbol, action_type, 1)
        market_open_index = context.market_open_index
        intraday_closes = context.intraday_lookback['Close'].to_numpy()[market_open_index:]
        entry_index = len(intraday_closes) - (context.current_time - position['entry_time']).seconds // 300 - 1
        take_profit = False
        if entry_index >= 0:
            entry_price = intraday_closes[entry_index]
            if context.current_price / entry_price - 1 > 0.01:
                take_profit = True
        if (
            context.current_time >= position['entry_time'] + datetime.timedelta(minutes=30)
            or context.current_time.time() >= datetime.time(16, 0)
            or take_profit
        ):
            return exit_position()
