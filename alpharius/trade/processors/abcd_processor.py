import datetime
from typing import List, Optional

import numpy as np
from ..common import (
    ActionType, Context, DataSource, Processor, ProcessorFactory, TradingFrequency,
    Position, ProcessorAction, Mode, DATETIME_TYPE)
from ..data_loader import get_shortable_symbols
from ..stock_universe import IntradayVolatilityStockUniverse

NUM_UNIVERSE_SYMBOLS = 20


class AbcdProcessor(Processor):

    def __init__(self,
                 lookback_start_date: DATETIME_TYPE,
                 lookback_end_date: DATETIME_TYPE,
                 data_source: DataSource,
                 output_dir: str) -> None:
        super().__init__(output_dir)
        self._positions = dict()
        self._stock_universe = IntradayVolatilityStockUniverse(lookback_start_date,
                                                               lookback_end_date,
                                                               data_source,
                                                               num_stocks=NUM_UNIVERSE_SYMBOLS)
        self._shortable_symbols = set(get_shortable_symbols())

    def get_trading_frequency(self) -> TradingFrequency:
        return TradingFrequency.FIVE_MIN

    def setup(self, hold_positions: List[Position], current_time: Optional[DATETIME_TYPE]) -> None:
        to_remove = [symbol for symbol, position in self._positions.items()
                     if position['status'] != 'active']
        for symbol in to_remove:
            self._positions.pop(symbol)

    def get_stock_universe(self, view_time: DATETIME_TYPE) -> List[str]:
        return list(set(self._stock_universe.get_stock_universe(view_time) +
                        list(self._positions.keys()) + ['TQQQ']))

    def process_data(self, context: Context) -> Optional[ProcessorAction]:
        if self.is_active(context.symbol):
            return self._close_position(context)
        elif context.symbol not in self._positions:
            return self._open_position(context)

    def _open_position(self, context: Context) -> Optional[ProcessorAction]:
        if abs(context.current_price / context.prev_day_close - 1) > 0.5:
            return
        t = context.current_time.time()
        if (datetime.time(10, 30) <= t <= datetime.time(15, 30) and
                context.current_price > context.prev_day_close):
            if context.symbol == 'TQQQ':
                return self._open_long_position(context, 0.7, 0.4)
            else:
                return self._open_long_position(context, 1, 0.6)
        if (datetime.time(12, 0) <= t <= datetime.time(15, 30) and
                context.current_price < context.prev_day_close and
                context.symbol in self._shortable_symbols and
                context.symbol != 'TQQQ'):
            return self._open_short_position(context)

    def _open_long_position(self, context: Context, a: float, b: float) -> Optional[ProcessorAction]:
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        open_price = context.intraday_lookback['Open'][market_open_index]
        intraday_closes = context.intraday_lookback['Close'][market_open_index:]
        if len(intraday_closes) < 10:
            return
        if intraday_closes[-1] >= intraday_closes[-2]:
            return
        if abs(intraday_closes[-3] - intraday_closes[-2]) < abs(intraday_closes[-2] - intraday_closes[-1]):
            return
        max_i = np.argmax(intraday_closes)
        intraday_high = intraday_closes[max_i]
        if not context.prev_day_close < open_price < context.current_price < intraday_high:
            return
        l2h = context.l2h_avg
        if intraday_high / open_price - 1 < a * l2h:
            return
        if intraday_high / context.current_price - 1 < b * l2h:
            return
        if len(intraday_closes) > max_i + 15 or len(intraday_closes) < max_i + 10:
            return
        self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                           f'L2h: {l2h}. Intraday high: {intraday_high}. Current price: {context.current_price}.')
        self._positions[context.symbol] = {'entry_time': context.current_time,
                                           'status': 'pending',
                                           'side': 'long'}
        return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)

    def _open_short_position(self, context: Context) -> Optional[ProcessorAction]:
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        open_price = context.intraday_lookback['Open'][market_open_index]
        intraday_closes = context.intraday_lookback['Close'][market_open_index:]
        if len(intraday_closes) < 10:
            return
        intraday_low = np.min(intraday_closes)
        if not context.prev_day_close > open_price > context.current_price > intraday_low:
            return
        h2l = context.h2l_avg
        if intraday_low / open_price - 1 > 0.7 * h2l:
            return
        if intraday_low / context.current_price - 1 > 0.5 * h2l:
            return
        min_i = np.argmin(intraday_closes)
        if len(intraday_closes) > min_i + 10 or len(intraday_closes) < min_i + 5:
            return
        if context.mode == Mode.TRADE:
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               f'Checking bar shape conditions. Current price: {context.current_price}.')
        for i in range(-5, 0):
            if intraday_closes[i] < intraday_closes[i - 1]:
                break
        else:
            return
        if abs(intraday_closes[-3] - intraday_closes[-2]) < abs(intraday_closes[-2] - intraday_closes[-1]):
            return
        if intraday_closes[-1] <= intraday_closes[-2]:
            return
        self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                           f'H2l: {h2l}. Intraday low: {intraday_low}. Current price: {context.current_price}.')
        self._positions[context.symbol] = {'entry_time': context.current_time,
                                           'status': 'active',
                                           'side': 'short'}
        return ProcessorAction(context.symbol, ActionType.SELL_TO_OPEN, 1)

    def _close_position(self, context: Context) -> Optional[ProcessorAction]:
        position = self._positions[context.symbol]
        side = position['side']
        intraday_closes = context.intraday_lookback['Close']
        take_profit = (context.current_time == position['entry_time'] + datetime.timedelta(minutes=30)
                       and len(intraday_closes) >= 7)
        if side == 'long':
            take_profit = take_profit and context.current_price > intraday_closes[-7]
        else:
            take_profit = take_profit and context.current_price < intraday_closes[-7]
        is_close = (take_profit or
                    context.current_time >= position['entry_time'] + datetime.timedelta(minutes=40)
                    or context.current_time.time() >= datetime.time(16, 0))
        self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                           f'Closing position: {is_close}. Current price {context.current_price}.')
        if is_close:
            position['status'] = 'inactive'
            action_type = ActionType.SELL_TO_CLOSE if side == 'long' else ActionType.BUY_TO_CLOSE
            return ProcessorAction(context.symbol, action_type, 1)


class AbcdProcessorFactory(ProcessorFactory):

    def __init__(self):
        super().__init__()

    def create(self,
               lookback_start_date: DATETIME_TYPE,
               lookback_end_date: DATETIME_TYPE,
               data_source: DataSource,
               output_dir: str,
               *args, **kwargs) -> AbcdProcessor:
        return AbcdProcessor(lookback_start_date, lookback_end_date, data_source, output_dir)
