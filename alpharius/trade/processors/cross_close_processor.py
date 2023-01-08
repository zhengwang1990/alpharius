import datetime
from typing import List, Optional

import numpy as np
from ..common import (
    ActionType, Context, DataSource, Processor, ProcessorFactory, TradingFrequency,
    Position, ProcessorAction, Mode, DAYS_IN_A_MONTH, DATETIME_TYPE)
from ..stock_universe import IntradayVolatilityStockUniverse

NUM_UNIVERSE_SYMBOLS = 20
EXIT_TIME = datetime.time(10, 0)


class CrossCloseProcessor(Processor):
    """Strategy acting on 5-min bar crossing previous day close."""

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

    def get_trading_frequency(self) -> TradingFrequency:
        return TradingFrequency.FIVE_MIN

    def setup(self, hold_positions: List[Position], current_time: Optional[DATETIME_TYPE]) -> None:
        to_remove = [symbol for symbol, position in self._positions.items()
                     if position['status'] != 'active']
        for symbol in to_remove:
            self._positions.pop(symbol)

    def get_stock_universe(self, view_time: DATETIME_TYPE) -> List[str]:
        return list(set(self._stock_universe.get_stock_universe(view_time) +
                        list(self._positions.keys())))

    def process_data(self, context: Context) -> Optional[ProcessorAction]:
        if context.symbol in self._positions:
            return self._close_position(context)
        else:
            return self._open_position(context)

    @staticmethod
    def _get_threshold(context: Context) -> float:
        interday_highs = context.interday_lookback['High'][-DAYS_IN_A_MONTH:]
        interday_lows = context.interday_lookback['Low'][-DAYS_IN_A_MONTH:]
        h2l_losses = [l / h - 1 for h, l in zip(interday_highs, interday_lows)]
        h2l_avg = np.average(h2l_losses)
        return h2l_avg * 0.5

    def _open_position(self, context: Context) -> Optional[ProcessorAction]:
        t = context.current_time.time()
        if t >= EXIT_TIME:
            return
        market_open_index = context.market_open_index
        intraday_opens = context.intraday_lookback['Open'][market_open_index:]
        intraday_closes = context.intraday_lookback['Close'][market_open_index:]
        if len(intraday_closes) < 2:
            return
        if abs(context.current_price / context.prev_day_close - 1) > 0.5:
            return
        current_loss = context.current_price / intraday_closes[-2] - 1
        threshold = self._get_threshold(context)
        is_cross = intraday_opens[-1] > context.prev_day_close > intraday_closes[-1]
        is_trade = current_loss < threshold and is_cross
        if is_trade or (context.mode == Mode.TRADE and current_loss < threshold * 0.8 and is_cross):
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               f'Current loss: {current_loss * 100:.2f}%. '
                               f'Threshold: {threshold * 100:.2f}%. '
                               f'Current price {context.current_price}.')
        if is_trade:
            self._positions[context.symbol] = {'entry_time': context.current_time,
                                               'status': 'active'}
            return ProcessorAction(context.symbol, ActionType.SELL_TO_OPEN, 1)

    def _close_position(self, context: Context) -> Optional[ProcessorAction]:
        position = self._positions[context.symbol]
        if position['status'] != 'active':
            return
        intraday_closes = context.intraday_lookback['Close']
        take_profit = (context.current_time == position['entry_time'] + datetime.timedelta(minutes=10)
                       and context.current_price < intraday_closes[-3])
        is_close = (take_profit or
                    context.current_time >= position['entry_time'] + datetime.timedelta(minutes=15) or
                    context.current_time.time() >= EXIT_TIME)
        if is_close:
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               f'Closing position. Current price {context.current_price}.')
            position['status'] = 'inactive'
            return ProcessorAction(context.symbol, ActionType.BUY_TO_CLOSE, 1)


class CrossCloseProcessorFactory(ProcessorFactory):

    def __init__(self):
        super().__init__()

    def create(self,
               lookback_start_date: DATETIME_TYPE,
               lookback_end_date: DATETIME_TYPE,
               data_source: DataSource,
               output_dir: str,
               *args, **kwargs) -> CrossCloseProcessor:
        return CrossCloseProcessor(lookback_start_date, lookback_end_date, data_source, output_dir)