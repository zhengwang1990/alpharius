import datetime
import pytz
from typing import List, Optional

import numpy as np
import pandas as pd

from .processor import Processor
from ..common import ActionType, Context, TradingFrequency, ProcessorAction, DAYS_IN_A_WEEK, DAYS_IN_A_MONTH


class OvernightTqqqProcessor(Processor):

    def __init__(self,
                 output_dir: str,
                 logging_timezone: Optional[pytz.timezone] = None) -> None:
        super().__init__(output_dir, logging_timezone)

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
        two_week_changes = [two_week_closes[i] / two_week_closes[i - 1] - 1 for i in range(1, len(two_week_closes))]
        two_week_std = np.std(two_week_changes)

        one_week_closes = interday_closes[-DAYS_IN_A_WEEK:]
        one_week_changes = [one_week_closes[i] / one_week_closes[i - 1] - 1 for i in range(1, len(one_week_closes))]
        one_week_std = np.std(one_week_changes)

        four_week_closes = interday_closes[-4 * DAYS_IN_A_WEEK:]
        four_week_changes = [four_week_closes[i] / four_week_closes[i - 1] - 1 for i in range(1, len(four_week_closes))]
        four_week_std = np.std(four_week_changes)

        if (two_week_std < 0.05 and intraday_change < 0.09
                and context.current_price / max(two_week_closes) > 0.8 and context.symbol == 'TQQQ'):
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               + f'{two_week_std=:.4f}, {intraday_change=:.4f}')
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               + f'interday_closes {interday_closes[-3:]}')
            two_week_max = max(two_week_closes)
            # If large drop and it's Friday, don't buy
            if context.current_time.isoweekday() == 5:
                if (context.current_price / max(two_week_closes) < 0.85 or
                        context.current_price / interday_closes[-1] < 0.95):
                    self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}]'
                                       + f' Large recent drop; Skip.')
                    return
                price_high = max(intraday_high, context.prev_day_close)
                if price_high / intraday_low - 1 > 0.06 and context.current_price < 0.94 * two_week_max:
                    self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}]'
                                       + f' Large volatility; Skip.')
                    return
            else:
                six_week_closes = interday_closes[-6 * DAYS_IN_A_WEEK:]
                # If grows too much, starts to drop and today's volatility is low
                if (context.current_price < 0.9 * two_week_max
                        and two_week_max > 1.3 * min(six_week_closes)
                        and context.prev_day_close * 1.06 > context.current_price > context.prev_day_close * 0.97):
                    self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}]'
                                       + f' Recent pullback; Skip.')
                    return
            if not interday_closes[-1] > interday_closes[-2] > interday_closes[-3]:
                return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)
            else:
                if (context.current_price > context.prev_day_close * 1.05 or
                        (min(four_week_closes) / max(four_week_closes) - 1 > -0.15
                         and intraday_low > context.prev_day_close * 0.99)):
                    return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)
        if (2 * one_week_std > two_week_std > four_week_std > 0.05
                and context.current_price / interday_closes[-1] - 1 < -0.07
                and context.symbol == 'SQQQ'):
            if context.current_price / min(interday_closes[-3 * DAYS_IN_A_MONTH:]) > 1.6:
                return
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               + f'{two_week_std=:.4f}, {four_week_std=:.4f}')
            return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)
