import datetime
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import tabulate

from alpharius.data import DataClient
from .processor import Processor
from ..common import (
    ActionType, Context, TradingFrequency, Position, ProcessorAction,
    DAYS_IN_A_YEAR, DAYS_IN_A_QUARTER, DAYS_IN_A_MONTH, DAYS_IN_A_WEEK, get_header)
from ..stock_universe import TopVolumeUniverse

NUM_UNIVERSE_SYMBOLS = 200
NUM_DIRECTIONAL_SYMBOLS = 5


class OvernightProcessor(Processor):

    def __init__(self,
                 lookback_start_date: pd.Timestamp,
                 lookback_end_date: pd.Timestamp,
                 data_client: DataClient,
                 output_dir: str) -> None:
        super().__init__(output_dir)
        self._stock_universe = TopVolumeUniverse(lookback_start_date,
                                                 lookback_end_date,
                                                 data_client,
                                                 num_stocks=NUM_UNIVERSE_SYMBOLS)
        self._universe_symbols = []
        self._hold_positions = []
        self._output_dir = output_dir

    def get_trading_frequency(self) -> TradingFrequency:
        return TradingFrequency.CLOSE_TO_OPEN

    def setup(self, hold_positions: List[Position], current_time: Optional[pd.Timestamp]) -> None:
        self._hold_positions = hold_positions

    def get_stock_universe(self, view_time: pd.Timestamp) -> List[str]:
        hold_symbols = [position.symbol for position in self._hold_positions]
        self._universe_symbols = self._stock_universe.get_stock_universe(view_time)
        return list(set(hold_symbols + self._universe_symbols))

    def process_all_data(self, contexts: List[Context]) -> List[ProcessorAction]:
        current_prices = {context.symbol: context.current_price for context in contexts}
        if not contexts:
            return []
        current_time = contexts[0].current_time
        if current_time.time() < datetime.time(10, 0):
            actions = []
            for position in self._hold_positions:
                if position.symbol not in current_prices:
                    self._logger.warning('Position [%s] not found in contexts', position.symbol)
                    continue
                action_type = ActionType.SELL_TO_CLOSE if position.qty >= 0 else ActionType.BUY_TO_CLOSE
                actions.append(ProcessorAction(position.symbol, action_type, 1))
            return actions

        contexts_selected = [context for context in contexts
                             if context.symbol in self._universe_symbols]
        performances = []
        for context in contexts_selected:
            performances.append((context.symbol, self._get_performance(context)))
        performances.sort(key=lambda s: s[1], reverse=True)
        long_symbols = [s[0] for s in performances[:NUM_DIRECTIONAL_SYMBOLS] if s[1] > 0]

        self._logging(performances, current_prices, current_time)

        actions = []
        for symbol in long_symbols:
            actions.append(ProcessorAction(symbol, ActionType.BUY_TO_OPEN, 1))
        return actions

    def _logging(self,
                 performances: List[Tuple[str, float]],
                 current_prices: Dict[str, float],
                 current_time: pd.Timestamp) -> None:
        performance_info = []
        for symbol, metric in performances[:NUM_DIRECTIONAL_SYMBOLS + 15]:
            price = current_prices[symbol]
            performance_info.append([symbol, price, metric])
        header = get_header(f'Metric Info {current_time.date()}')
        self._logger.debug('\n' + header + '\n' + tabulate.tabulate(
            performance_info, headers=['Symbol', 'Price', 'Performance'], tablefmt='grid'))

    @staticmethod
    def _get_performance(context: Context) -> float:
        interday_lookback = context.interday_lookback
        if len(interday_lookback) < DAYS_IN_A_YEAR:
            return 0
        closes = interday_lookback['Close'].tolist()[-DAYS_IN_A_YEAR:]
        if context.current_price / closes[-DAYS_IN_A_WEEK] - 1 < -0.5:
            return 0
        values = np.append(closes, context.current_price)
        profits = [np.log(values[k + 1] / values[k])
                   for k in range(len(values) - 1)]
        r = np.average(profits)
        std = np.std(profits)
        if (profits[-1] - r) / std < -1:
            return 0
        today_open = context.today_open
        opens = np.append(interday_lookback['Open'].iloc[-DAYS_IN_A_YEAR + 1:], today_open)
        overnight_returns = []
        for close_price, open_price in zip(closes, opens):
            overnight_returns.append(np.log(open_price / close_price))
        quarterly = np.sum(overnight_returns[-DAYS_IN_A_QUARTER:])
        weekly = np.sum(overnight_returns[-DAYS_IN_A_WEEK:])
        if ((quarterly < 0 or closes[-1] < closes[-DAYS_IN_A_MONTH]) and
                (weekly < 0 or closes[-1] < closes[-DAYS_IN_A_WEEK])):
            return 0
        yearly = np.sum(sorted(overnight_returns)[25:-25])
        performance = yearly + 0.3 * quarterly + 0.3 * weekly
        return performance
