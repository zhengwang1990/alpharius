import datetime
from typing import List, Optional

import numpy as np
import pandas as pd

from alpharius.data import DataClient
from ..common import (
    ActionType, Context, Processor, ProcessorFactory, TradingFrequency,
    ProcessorAction, DAYS_IN_A_WEEK)


class OvernightTqqqProcessor(Processor):

    def __init__(self,
                 lookback_start_date: pd.Timestamp,
                 lookback_end_date: pd.Timestamp,
                 data_client: DataClient,
                 output_dir: str) -> None:
        super().__init__(output_dir)

    def get_trading_frequency(self) -> TradingFrequency:
        return TradingFrequency.CLOSE_TO_OPEN

    def get_stock_universe(self, view_time: pd.Timestamp) -> List[str]:
        return ['TQQQ', 'SQQQ']

    def process_data(self, context: Context) -> Optional[ProcessorAction]:
        if context.current_time.time() < datetime.time(10, 0):
            return ProcessorAction(context.symbol, ActionType.SELL_TO_CLOSE, 1)
        market_open_index = context.market_open_index
        if market_open_index is None:
            return
        intraday_high = max(context.intraday_lookback['High'].values[market_open_index:])
        intraday_low = min(context.intraday_lookback['Low'].values[market_open_index:])
        intraday_change = intraday_high / intraday_low - 1
        interday_closes = context.interday_lookback['Close'].values
        two_week_closes = interday_closes[-2 * DAYS_IN_A_WEEK:]
        two_week_changes = [two_week_closes[i] / two_week_closes[i - 1] for i in range(1, len(two_week_closes))]
        two_week_std = np.std(two_week_changes)

        four_week_closes = interday_closes[-4 * DAYS_IN_A_WEEK:]
        four_week_changes = [four_week_closes[i] / four_week_closes[i - 1] for i in range(1, len(four_week_closes))]
        four_week_std = np.std(four_week_changes)

        if two_week_std < 0.05 and intraday_change < 0.09 and context.symbol == 'TQQQ':
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               + f'{two_week_std=}, {intraday_change=}')
            if not interday_closes[-1] > interday_closes[-2] > interday_closes[-3]:
                return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)
        if two_week_std > 0.1 > four_week_std and context.symbol == 'SQQQ':
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               + f'{two_week_std=}, {four_week_std=}')
            return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)


class OvernightTqqqProcessorFactory(ProcessorFactory):
    processor_class = OvernightTqqqProcessor
