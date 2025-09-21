import datetime
import pytz
from typing import List, Optional

import numpy as np
import pandas as pd

from ..common import (
    ProcessorAction, ActionType, Context, Processor, ProcessorFactory, TradingFrequency,
    DAYS_IN_A_MONTH)


class TqqqProcessor(Processor):

    def __init__(self,
                 output_dir: str,
                 logging_timezone: Optional[pytz.timezone] = None) -> None:
        super().__init__(output_dir, logging_timezone)
        self._positions = dict()

    def get_trading_frequency(self) -> TradingFrequency:
        return TradingFrequency.FIVE_MIN

    def get_stock_universe(self, view_time: pd.Timestamp) -> List[str]:
        return ['TQQQ']

    def process_data(self, context: Context) -> Optional[ProcessorAction]:
        if self.is_active(context.symbol):
            return self._close_position(context)
        else:
            return self._open_position(context)

    def _open_position(self, context: Context) -> Optional[ProcessorAction]:
        if context.market_open_index is None:
            return
        action = self._last_hour_momentum(context)
        if action:
            return action
        action = self._mean_reversion(context)
        if action:
            return action
        action = self._first_hour_momentum(context)
        if action:
            return action
        action = self._four_day_drop(context)
        if action:
            return action
        action = self._open_high_momentum(context)
        if action:
            return action

    def _mean_reversion(self, context: Context) -> Optional[ProcessorAction]:
        t = context.current_time.time()
        if t <= datetime.time(11, 15) or t >= datetime.time(15, 0):
            return
        interday_closes = context.interday_lookback['Close'].tolist()
        market_open_index = context.market_open_index
        intraday_closes = context.intraday_lookback['Close'].tolist()[market_open_index:]
        # short
        l2h = context.l2h_avg
        short_t = 26
        if (interday_closes[-1] < np.max(interday_closes[-DAYS_IN_A_MONTH:]) * 0.9
                and len(intraday_closes) >= short_t):
            change = intraday_closes[-1] / intraday_closes[-short_t] - 1
            single_bar_max = max([intraday_closes[i] / intraday_closes[i - 1] - 1 for i in range(-short_t + 1, 0)])
            if change > 0.8 * l2h:
                self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                                   f'Mean reversion strategy. Side: short. Change: {change * 100:.2f}%. '
                                   f'Threshold: {l2h * 100:.2f}%. Single bar max {single_bar_max * 100:.2f}%')
            if change > l2h and single_bar_max < 0.5 * change:
                self._positions[context.symbol] = {'side': 'short',
                                                   'strategy': 'mean_reversion',
                                                   'entry_time': context.current_time}
                return ProcessorAction(context.symbol, ActionType.SELL_TO_OPEN, 1)
        # long
        h2l = context.h2l_avg
        long_t = 19
        if len(intraday_closes) >= long_t:
            change = intraday_closes[-1] / intraday_closes[-long_t] - 1
            if change < 0.8 * h2l:
                self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                                   f'Mean reversion strategy. Current price: {context.current_price}. '
                                   f'Side: long. Change: {change * 100:.2f}%. Threshold: {h2l * 100:.2f}%.')
            if change < h2l:
                self._positions[context.symbol] = {'side': 'long',
                                                   'strategy': 'mean_reversion',
                                                   'entry_time': context.current_time}
                return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)

    def _first_hour_momentum(self, context: Context) -> Optional[ProcessorAction]:
        t = context.current_time.time()
        if t != datetime.time(10, 0):
            return
        market_open_index = context.market_open_index
        intraday_highs = context.intraday_lookback['High'].tolist()[market_open_index:]
        intraday_opens = context.intraday_lookback['Open'].tolist()[market_open_index:]
        intraday_closes = context.intraday_lookback['Close'].tolist()[market_open_index:]
        if len(intraday_highs) != 6:
            return
        cnt = 0
        for i in range(len(intraday_closes)):
            cnt += int(intraday_closes[i] >= intraday_opens[i])
        if cnt < 5:
            return
        for i in range(len(intraday_highs) - 1):
            if intraday_highs[i] > intraday_highs[i + 1] and intraday_closes[i] > intraday_closes[i + 1]:
                return
        bar_sizes = [abs(intraday_closes[i] - intraday_opens[i]) for i in range(len(intraday_closes))]
        bar_sizes.sort(reverse=True)
        if bar_sizes[0] > 2 * bar_sizes[1]:
            return
        self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                           f'First hour momentum strategy. Current price: {context.current_price}.')
        self._positions[context.symbol] = {'side': 'long',
                                           'strategy': 'first_hour_momentum',
                                           'entry_time': context.current_time}
        return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)

    def _last_hour_momentum(self, context: Context) -> Optional[ProcessorAction]:
        def _open_position(side: str) -> ProcessorAction:
            self._positions[context.symbol] = {'side': side,
                                               'strategy': 'last_hour_momentum',
                                               'entry_time': context.current_time}
            action_type = ActionType.SELL_TO_OPEN if side == 'short' else ActionType.BUY_TO_OPEN
            return ProcessorAction(context.symbol, action_type, 1)

        t = context.current_time.time()
        if t <= datetime.time(15, 0) or t >= datetime.time(15, 30):
            return
        interday_closes = list(context.interday_lookback['Close'])
        if interday_closes[-1] > np.max(interday_closes[-DAYS_IN_A_MONTH:]) * 0.9:
            return
        market_open_index = context.market_open_index
        intraday_opens = context.intraday_lookback['Open'].tolist()[market_open_index:]
        change_from_open = context.current_price / intraday_opens[0] - 1
        change_from_close = context.current_price / context.prev_day_close - 1
        h2l = context.h2l_avg
        if change_from_open < 0.7 * h2l or change_from_close < 2 * h2l:
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] Last hour momentum strategy. '
                               f'Current price: {context.current_price}.')
            return _open_position('short')
        l2h = context.l2h_avg
        intraday_closes = context.intraday_lookback['Close'][market_open_index:]
        change_from_min = context.current_price / np.min(intraday_closes) - 1
        if change_from_min > 1.2 * l2h and intraday_closes[-1] < intraday_closes[-2]:
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] Last hour momentum strategy. '
                               f'Current price: {context.current_price}. '
                               f'Change from min [{change_from_min * 100:.2f}%] '
                               f'exceeds threshold [{1.2 * l2h * 100:.2f}%]')
            return _open_position('long')
        if intraday_opens[0] < context.prev_day_close:
            for i in range(-25, -len(intraday_closes), -24):
                if intraday_closes[i + 24] / intraday_closes[i] - 1 < l2h * 0.15:
                    break
            else:
                self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] '
                                   f'Last hour momentum strategy. Current price: {context.current_price}. '
                                   'Continuous up trend.')
                return _open_position('long')
        if change_from_open > 0.7 * l2h:
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] Last hour momentum strategy. '
                               f'Current price: {context.current_price}. '
                               f'Change from open [{change_from_open * 100:.2f}%] '
                               f'exceeds threshold [{0.7 * l2h * 100:.2f}%]')
            return _open_position('long')
        if change_from_close > 1.5 * l2h:
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] Last hour momentum strategy. '
                               f'Current price: {context.current_price}. '
                               f'Change from close [{change_from_close * 100:.2f}%] '
                               f'exceeds threshold [{1.5 * l2h * 100:.2f}%]')
            return _open_position('long')

    def _four_day_drop(self, context: Context) -> Optional[ProcessorAction]:
        t = context.current_time.time()
        if not datetime.time(10, 0) <= t < datetime.time(12, 0):
            return
        interday_closes = context.interday_lookback['Close']
        if context.current_price > context.prev_day_close:
            return
        for i in range(-1, -4, -1):
            day_change = interday_closes.iloc[i] / interday_closes.iloc[i - 1] - 1
            if day_change > 0.3 * context.h2l_avg:
                return
        market_open_index = context.market_open_index
        intraday_closes = context.intraday_lookback['Close'][market_open_index:]
        if len(intraday_closes) < 5:
            return
        change_today = context.current_price / context.prev_day_close - 1
        if change_today > 0.3 * context.h2l_avg:
            return
        if market_open_index is None:
            return
        is_trade = False
        wait_min = 0
        for i in range(-1, -5, -1):
            if intraday_closes[i] > intraday_closes[i - 1]:
                break
        else:
            is_trade = (change_today < 0.4 * context.h2l_avg and
                        context.current_price == np.min(intraday_closes) and
                        2 * intraday_closes[-2] < intraday_closes[-3] + intraday_closes[-1])
            wait_min = 30
        for i in range(-1, -4, -1):
            if intraday_closes[i] < intraday_closes[i - 1]:
                break
        else:
            is_trade = True
            wait_min = 15
        if is_trade:
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] Four day drop strategy. '
                               f'Current price: {context.current_price}. '
                               f'Change today [{change_today * 100:.2f}%]. '
                               f'H2l [{context.h2l_avg * 100:.2f}%].')
            self._positions[context.symbol] = {'entry_time': context.current_time,
                                               'strategy': 'four_day_drop',
                                               'wait_min': wait_min,
                                               'side': 'long'}
            return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 1)

    def _open_high_momentum(self, context: Context) -> Optional[ProcessorAction]:
        t = context.current_time.time()
        if not datetime.time(11, 0) <= t < datetime.time(16, 0):
            return
        interday_closes = context.interday_lookback['Close']
        if interday_closes.iloc[-1] / interday_closes.iloc[-2] - 1 > context.l2h_avg:
            return
        market_open_index = context.market_open_index
        intraday_opens = context.intraday_lookback['Open'].tolist()[market_open_index:]
        open_price = intraday_opens[0]
        open_gain = open_price / context.prev_day_close - 1
        if open_gain < context.l2h_avg:
            return
        intraday_closes = context.intraday_lookback['Close'].tolist()[market_open_index:]
        if len(intraday_closes) < 19:
            return
        for i in [-1, -7, -13]:
            if intraday_closes[i] < intraday_closes[i - 6]:
                return
        self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] Open high momentum strategy. '
                           f'Current price: {context.current_price}. '
                           f'Open gain: {open_gain * 100:.2f}%. '
                           f'H2l [{context.h2l_avg * 100:.2f}%].')
        self._positions[context.symbol] = {'entry_time': context.current_time,
                                           'strategy': 'open_high_momentum',
                                           'side': 'long'}
        return ProcessorAction(context.symbol, ActionType.BUY_TO_OPEN, 0.5)

    def _close_position(self, context: Context) -> Optional[ProcessorAction]:
        def exit_position():
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               f'Closing position. Current price {context.current_price}.')
            self._positions.pop(context.symbol)
            return action

        position = self._positions[context.symbol]
        side = position['side']
        action_type = ActionType.SELL_TO_CLOSE if side == 'long' else ActionType.BUY_TO_CLOSE
        action = ProcessorAction(context.symbol, action_type, 1)
        market_open_index = context.market_open_index
        intraday_closes = context.intraday_lookback['Close'][market_open_index:]
        strategy = position['strategy']
        if strategy == 'last_hour_momentum':
            # Stop loss
            if context.current_time.time() == datetime.time(15, 45):
                entry_price = context.intraday_lookback['Close'].iloc[-4]
                if side == 'short' and context.current_price > entry_price * 1.01:
                    return exit_position()
                if side == 'long' and context.current_price < entry_price * 0.99:
                    return exit_position()
            if context.current_time.time() == datetime.time(16, 0):
                return exit_position()
        if strategy == 'mean_reversion':
            if context.current_time >= position['entry_time'] + datetime.timedelta(minutes=60):
                return exit_position()
        if strategy == 'first_hour_momentum':
            if context.current_time >= position['entry_time'] + datetime.timedelta(minutes=30):
                return exit_position()
        if strategy == 'four_day_drop':
            wait_min = position['wait_min']
            if context.current_time >= position['entry_time'] + datetime.timedelta(minutes=wait_min):
                return exit_position()
        if strategy == 'open_high_momentum':
            stop_loss = context.current_price == np.min(intraday_closes)
            if stop_loss or context.current_time.time() == datetime.time(16, 0):
                return exit_position()


class TqqqProcessorFactory(ProcessorFactory):
    processor_class = TqqqProcessor
