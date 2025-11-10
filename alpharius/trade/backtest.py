import builtins
import collections
import datetime
import difflib
import functools
import keyword
import math
import os
import re
import signal
import time
import threading
import warnings
from typing import Any, Dict, List, Optional, Set, Tuple, Type, Union

import alpaca.trading as trading
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tabulate
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning

try:
    import git
except ImportError:
    git = None

from alpharius.data import (
    DataClient, load_intraday_dataset, load_interday_dataset,
)
from alpharius.utils import (
    TIME_ZONE,
    Transaction,
    compute_risks,
    compute_drawdown,
    compute_bernoulli_ci95,
    get_all_symbols,
    get_trading_client,
)
from .processors.processor import Processor, instantiate_processor
from .common import (
    Action, ActionType, Context, Position, TradingFrequency, Mode,
    BASE_DIR, MARKET_OPEN, MARKET_CLOSE, OUTPUT_DIR, INTERDAY_LOOKBACK_LOAD,
    BID_ASK_SPREAD, SHORT_RESERVE_RATIO, logging_config, timestamp_to_index,
    get_unique_actions, get_header)

_MAX_WORKERS = 20

warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)


class Backtest:

    def __init__(self,
                 start_date: Union[pd.Timestamp, str],
                 end_date: Union[pd.Timestamp, str],
                 processors: List[Union[Type[Processor], Processor]],
                 data_client: DataClient,
                 ack_all: Optional[bool] = False) -> None:
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date)
        self._start_date = start_date
        self._end_date = end_date
        self._processor_classes = processors
        self._processors: List[Processor] = []
        self._positions = []
        self._daily_equity = [1]
        self._num_win, self._num_lose = 0, 0
        self._cash = 1
        self._cash_portion = 1
        self._processor_stats = dict()
        self._interday_dataset = None
        self._ack_all = ack_all
        self._data_client = data_client

        backtesting_output_dir = os.path.join(OUTPUT_DIR, 'backtest')
        self._output_num = 1
        while True:
            output_dir = os.path.join(backtesting_output_dir,
                                      datetime.datetime.now().strftime('%m-%d'),
                                      f'{self._output_num:02d}')
            if not os.path.exists(output_dir):
                self._output_dir = output_dir
                os.makedirs(output_dir, exist_ok=True)
                break
            self._output_num += 1

        self._details_log = logging_config(os.path.join(
            self._output_dir, 'details.txt'), detail=False, name='details')
        self._summary_log = logging_config(os.path.join(
            self._output_dir, 'summary.txt'), detail=False, name='summary')

        trading_client = get_trading_client()
        calendar = trading_client.get_calendar(
            filters=trading.GetCalendarRequest(
                start=self._start_date.date(),
                end=(self._end_date - datetime.timedelta(days=1)).date(),
            ))
        self._market_dates = [market_day.date for market_day in calendar
                              if market_day.date < self._end_date.date()]
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, self._safe_exit)

        self._run_start_time = None
        self._interday_load_time = 0
        self._intraday_load_time = 0
        self._stock_universe_load_time = 0
        self._context_prep_time = 0
        self._transactions = []
        self._processor_time = collections.defaultdict(int)

    def _safe_exit(self, signum, frame) -> None:
        self._close()
        exit(1)

    def _close(self):
        self._print_profile()
        self._print_summary()
        self._plot_summary()
        for processor in self._processors:
            processor.teardown()

    def _init_processors(self, history_start) -> None:
        self._processors = []
        for processor_class in self._processor_classes:
            processor = instantiate_processor(
                processor_class,
                lookback_start_date=history_start,
                lookback_end_date=self._end_date,
                data_client=self._data_client,
                output_dir=self._output_dir)
            self._processors.append(processor)

    def _record_diff(self):
        repo = git.Repo(BASE_DIR)
        html = ''
        max_num_line = 0
        regex_template = r'((&nbsp;|[\w\.\(\)\[\]_]|(<[^>]+>))*?)((?<![A-Za-z\-_\.])({})(?![A-Za-z\-_=]))'
        keyword_pattern = re.compile(regex_template.format('|'.join(keyword.kwlist)), re.VERBOSE)
        builtin_names = [b for b in dir(builtins) if b.islower()] + ['self']
        builtin_pattern = re.compile(regex_template.format('|'.join(builtin_names), re.VERBOSE))
        for item in repo.head.commit.diff(None):
            old_content, new_content = [], []
            if item.change_type != 'A':
                old_content = item.a_blob.data_stream.read().decode('utf-8').split('\n')
            if item.change_type != 'D':
                try:
                    with open(os.path.join(BASE_DIR, item.b_path), 'r') as f:
                        new_content = f.read().split('\n')
                except UnicodeDecodeError:
                    continue
            max_num_line = max(max_num_line, len(old_content), len(new_content))
            html_diff = difflib.HtmlDiff(wrapcolumn=120)
            html += f'<div><h1>{item.b_path}</h1>'
            diff_table = html_diff.make_table(old_content, new_content, context=True)
            if item.b_path.endswith('.py'):
                soup = BeautifulSoup(diff_table, 'html.parser')
                for td in soup.find_all('td'):
                    content = td.decode_contents()
                    if '#' in content:
                        continue
                    if any(k in content for k in keyword.kwlist):
                        content = keyword_pattern.sub(
                            r'\1<span class="python_keyword">\4</span>',
                            content,
                        )
                    if any(k in content for k in builtin_names):
                        content = builtin_pattern.sub(
                            r'\1<span class="python_builtin">\5</span>',
                            content,
                        )
                    td.clear()
                    td.append(BeautifulSoup(content, 'html.parser'))
                diff_table = str(soup)
            html += diff_table
            html += '</div>'
        if html:
            current_dir = os.path.dirname(os.path.realpath(__file__))
            template_file = os.path.join(current_dir, 'html', 'diff.html')
            header_width = (int(np.log10(max_num_line)) + 1) * 7 + 6
            with open(template_file, 'r') as f:
                template = f.read()
            with open(os.path.join(self._output_dir, 'diff.html'), 'w', encoding='utf-8') as f:
                f.write(template.format(
                    header_width=header_width, html=html, output_num=self._output_num,
                    logo_path=os.path.join(current_dir, 'html', 'diff.png')))

    def run(self) -> List[Transaction]:
        self._run_start_time = time.time()
        if git is not None:
            try:
                self._record_diff()
            except (ValueError, git.GitError) as e:
                # Git doesn't work in some circumstances
                self._summary_log.warning(f'Diff can not be generated: {e}')
        history_start = self._start_date - datetime.timedelta(days=INTERDAY_LOOKBACK_LOAD)
        self._interday_dataset = load_interday_dataset(
            get_all_symbols(), history_start, self._end_date, self._data_client)
        self._interday_load_time += time.time() - self._run_start_time
        self._init_processors(history_start)
        transactions = []
        for day in self._market_dates:
            executed_closes = self._process(day)
            transactions.extend(executed_closes)
        self._close()
        return transactions

    def _load_stock_universe(
            self,
            day: datetime.date,
    ) -> Tuple[Dict[str, List[str]], Dict[TradingFrequency, Set[str]]]:
        load_stock_universe_start = time.time()
        processor_stock_universes = dict()
        stock_universe = collections.defaultdict(set)
        for processor in self._processors:
            processor_name = processor.name
            processor_stock_universe = processor.get_stock_universe(pd.Timestamp(day))
            processor_stock_universes[processor_name] = processor_stock_universe
            stock_universe[processor.get_trading_frequency()].update(processor_stock_universe)
        self._stock_universe_load_time += time.time() - load_stock_universe_start
        return processor_stock_universes, stock_universe

    def _process_data(self,
                      contexts: Dict[str, Context],
                      stock_universes: Dict[str, List[str]],
                      processors: List[Processor]) -> List[Action]:
        actions = []
        for processor in processors:
            data_process_start = time.time()
            processor_name = processor.name
            processor_stock_universe = stock_universes[processor_name]
            processor_contexts = []
            for symbol in processor_stock_universe:
                context = contexts.get(symbol)
                if context:
                    processor_contexts.append(context)
            processor_actions = processor.process_all_data(processor_contexts)
            actions.extend([Action(pa.symbol, pa.type, pa.percent,
                                   contexts[pa.symbol].current_price,
                                   processor)
                            for pa in processor_actions])
            self._processor_time[processor_name] += time.time() - data_process_start
        return actions

    @functools.lru_cache()
    def _prepare_interday_lookback(self, day: pd.Timestamp, symbol: str) -> Optional[pd.DataFrame]:
        if symbol not in self._interday_dataset:
            return
        interday_data = self._interday_dataset[symbol]
        interday_ind = timestamp_to_index(interday_data.index, pd.Timestamp(day).tz_localize(TIME_ZONE))
        if interday_ind is None:
            return
        interday_lookback = interday_data.iloc[:interday_ind]
        return interday_lookback

    @staticmethod
    def _prepare_intraday_lookback(current_interval_start: pd.Timestamp, symbol: str,
                                   intraday_datas: Dict[str, pd.DataFrame]) -> Optional[pd.DataFrame]:
        intraday_data = intraday_datas[symbol]
        intraday_ind = timestamp_to_index(intraday_data.index, current_interval_start)
        if intraday_ind is None:
            return
        intraday_lookback = intraday_data.iloc[:intraday_ind + 1]
        return intraday_lookback

    def _load_intraday_data(self,
                            day: pd.Timestamp,
                            stock_universe: Dict[TradingFrequency, Set[str]]) -> Dict[str, pd.DataFrame]:
        load_intraday_start = time.time()
        unique_symbols = set()
        for _, symbols in stock_universe.items():
            unique_symbols.update(symbols)
        intraday_dataset = load_intraday_dataset(unique_symbols, day, self._data_client)
        self._intraday_load_time += time.time() - load_intraday_start
        return intraday_dataset

    def _process(self, day: datetime.date) -> List[Transaction]:
        for processor in self._processors:
            processor.setup(self._positions, day)

        processor_stock_universes, stock_universe = self._load_stock_universe(day)

        intraday_datas = self._load_intraday_data(pd.Timestamp(day), stock_universe)

        market_open = pd.to_datetime(pd.Timestamp.combine(day, MARKET_OPEN)).tz_localize(TIME_ZONE)
        market_close = pd.to_datetime(pd.Timestamp.combine(day, MARKET_CLOSE)).tz_localize(TIME_ZONE)
        current_interval_start = market_open

        executed_actions = []
        while current_interval_start < market_close:
            current_time = current_interval_start + datetime.timedelta(minutes=5)

            frequency_to_process = [TradingFrequency.FIVE_MIN]
            if current_interval_start == market_open:
                frequency_to_process = [TradingFrequency.FIVE_MIN,
                                        TradingFrequency.CLOSE_TO_OPEN]
            elif current_time == market_close:
                frequency_to_process = [TradingFrequency.FIVE_MIN,
                                        TradingFrequency.CLOSE_TO_OPEN,
                                        TradingFrequency.CLOSE_TO_CLOSE]

            prep_context_start = time.time()
            contexts = dict()
            unique_symbols = set()
            for frequency, symbols in stock_universe.items():
                if frequency in frequency_to_process:
                    unique_symbols.update(symbols)
            for symbol in unique_symbols:
                intraday_lookback = self._prepare_intraday_lookback(
                    current_interval_start, symbol, intraday_datas)
                if intraday_lookback is None or len(intraday_lookback) == 0:
                    continue
                interday_lookback = self._prepare_interday_lookback(day, symbol)
                if interday_lookback is None or len(interday_lookback) == 0:
                    continue
                current_price = intraday_lookback['Close'].iloc[-1]
                context = Context(symbol=symbol,
                                  current_time=current_time,
                                  current_price=current_price,
                                  interday_lookback=interday_lookback,
                                  intraday_lookback=intraday_lookback,
                                  mode=Mode.BACKTEST)
                contexts[symbol] = context
            self._context_prep_time += time.time() - prep_context_start

            processors = []
            for processor in self._processors:
                if processor.get_trading_frequency() in frequency_to_process:
                    processors.append(processor)
            actions = self._process_data(contexts, processor_stock_universes, processors)
            current_executed_actions = self._process_actions(current_time, actions)
            executed_actions.extend(current_executed_actions)

            current_interval_start += datetime.timedelta(minutes=5)

        for processor in self._processors:
            processor.teardown()

        self._log_day(day, executed_actions)
        return executed_actions

    def _process_actions(self, current_time: pd.Timestamp, actions: List[Action]) -> List[List[Any]]:
        unique_actions = get_unique_actions(actions)

        close_actions = [action for action in unique_actions
                         if action.type in [ActionType.BUY_TO_CLOSE, ActionType.SELL_TO_CLOSE]]
        executed_closes = self._close_positions(current_time, close_actions)

        open_actions = [action for action in unique_actions
                        if action.type in [ActionType.BUY_TO_OPEN, ActionType.SELL_TO_OPEN]]
        self._open_positions(current_time, open_actions)

        return executed_closes

    def _pop_current_position(self, symbol: str) -> Optional[Position]:
        for ind, position in enumerate(self._positions):
            if position.symbol == symbol:
                current_position = self._positions.pop(ind)
                return current_position
        return None

    def _get_current_position(self, symbol: str) -> Optional[Position]:
        for position in self._positions:
            if position.symbol == symbol:
                return position
        return None

    def _close_positions(self, current_time: pd.Timestamp, actions: List[Action]) -> List[Transaction]:
        executed_actions = []
        one_time_processor_profit = collections.defaultdict(float)
        for action in actions:
            assert action.type in [ActionType.BUY_TO_CLOSE, ActionType.SELL_TO_CLOSE]
            symbol = action.symbol
            current_position = self._get_current_position(symbol)
            if current_position is None:
                continue
            if action.type == ActionType.BUY_TO_CLOSE and current_position.qty > 0:
                continue
            if action.type == ActionType.SELL_TO_CLOSE and current_position.qty < 0:
                continue
            self._pop_current_position(symbol)
            qty = current_position.qty * action.percent
            portion = current_position.entry_portion * action.percent
            self._cash_portion += portion
            new_qty = current_position.qty - qty
            new_portion = current_position.entry_portion - portion
            if abs(new_qty) > 1E-7:
                self._positions.append(Position(symbol, new_qty,
                                                current_position.entry_price,
                                                current_position.entry_time,
                                                new_portion))
            spread_adjust = (1 - BID_ASK_SPREAD
                             if action.type == ActionType.SELL_TO_CLOSE else 1 + BID_ASK_SPREAD)
            adjusted_action_price = action.price * spread_adjust
            self._cash += adjusted_action_price * qty
            profit = adjusted_action_price / current_position.entry_price - 1
            if action.type == ActionType.BUY_TO_CLOSE:
                profit *= -1
            processor_stats = self._processor_stats.setdefault(action.processor.name,
                                                               {'profit': 0.0, 'num_win': 0, 'num_lose': 0})
            if profit > 0:
                self._num_win += 1
                processor_stats['num_win'] += 1
            else:
                self._num_lose += 1
                processor_stats['num_lose'] += 1
            one_time_processor_profit[action.processor.name] += portion * profit
            executed_actions.append(
                Transaction(symbol, action.type == ActionType.SELL_TO_CLOSE, action.processor.name,
                            current_position.entry_price, action.price, current_position.entry_time,
                            current_time, qty, profit * qty * current_position.entry_price,
                            profit, None, None))
        for processor_name, profit in one_time_processor_profit.items():
            processor_stats = self._processor_stats.setdefault(processor_name,
                                                               {'profit': 0.0, 'num_win': 0, 'num_lose': 0})
            processor_stats['profit'] = (processor_stats['profit'] + 1) * (1 + profit) - 1
        self._transactions.extend(executed_actions)
        return executed_actions

    def _open_positions(self, current_time: pd.Timestamp, actions: List[Action]) -> None:
        tradable_cash = self._cash
        for position in self._positions:
            if position.qty < 0:
                tradable_cash += position.entry_price * position.qty * (1 + SHORT_RESERVE_RATIO)
        action_cnt = collections.defaultdict(int)
        cash_portion = self._cash_portion
        for action in actions:
            action_cnt[action.symbol] += 1
        for action in actions:
            assert action.type in [ActionType.BUY_TO_OPEN, ActionType.SELL_TO_OPEN]
            symbol = action.symbol
            # Avoid controversial actions for the same symbol
            if action_cnt[symbol] > 1:
                continue
            portion = min(1 / len(actions), action.percent)
            # Use abs to avoid sign error caused by floating point error
            cash_to_trade = abs(tradable_cash * portion)
            if abs(cash_to_trade) > 1E-7 or self._ack_all:
                action.processor.ack(symbol)
            else:
                continue
            entry_portion = cash_portion * portion
            self._cash_portion -= entry_portion
            qty = cash_to_trade / action.price
            if action.type == ActionType.SELL_TO_OPEN:
                qty = -qty
            old_position = self._get_current_position(symbol)
            if old_position is None:
                entry_price = action.price
                new_qty = qty
            else:
                self._pop_current_position(symbol)
                if old_position.qty == 0:
                    entry_price = action.price
                else:
                    entry_price = (old_position.entry_price * old_position.qty +
                                   action.price * qty) / (old_position.qty + qty)
                    entry_portion = old_position.entry_portion + entry_portion
                new_qty = qty + old_position.qty
            new_position = Position(symbol, new_qty, entry_price, current_time, entry_portion)
            self._positions.append(new_position)
            self._cash -= action.price * qty

    def _log_day(self,
                 day: datetime.date,
                 executed_closes: List[Transaction]) -> None:
        outputs = [get_header(day)]
        if executed_closes:
            table_list = [[t.symbol, t.processor, t.entry_time.time(), t.exit_time.time(),
                           'long' if t.is_long else 'short', f'{t.entry_price:.4g}',
                           f'{t.exit_price:.4g}', f'{t.gl_pct * 100:+.2f}%'] for t in executed_closes]
            trade_info = tabulate.tabulate(table_list,
                                           headers=['Symbol', 'Processor', 'Entry Time', 'Exit Time', 'Side',
                                                    'Entry Price', 'Exit Price', 'Gain/Loss'],
                                           tablefmt='grid',
                                           disable_numparse=True)
            outputs.append('[ Trades ]')
            outputs.append(trade_info)

        if self._positions:
            position_info = []
            for position in self._positions:
                interday_data = self._interday_dataset[position.symbol]
                interday_ind = timestamp_to_index(interday_data.index, pd.Timestamp(day).tz_localize(TIME_ZONE))
                close_price, daily_change = None, None
                if interday_ind is not None:
                    close_price = interday_data['Close'].iloc[interday_ind]
                    if interday_ind > 0:
                        daily_change = (close_price / interday_data['Close'].iloc[interday_ind - 1] - 1) * 100
                change = (close_price / position.entry_price - 1) * 100 if close_price is not None else None
                value = close_price * position.qty if close_price is not None else None
                position_info.append([position.symbol, f'{position.qty:.2g}', f'{position.entry_price:.4g}',
                                      f'{close_price:.4g}', f'{value:.2g}',
                                      f'{daily_change:+.2f}%' if daily_change is not None else None,
                                      f'{change:+.2f}%' if change is not None else None])
            outputs.append('[ Positions ]')
            outputs.append(tabulate.tabulate(position_info,
                                             headers=['Symbol', 'Qty', 'Entry Price', 'Current Price',
                                                      'Current Value', 'Daily Change', 'Change'],
                                             tablefmt='grid',
                                             disable_numparse=True))

        equity = self._cash
        for position in self._positions:
            interday_data = self._interday_dataset[position.symbol]
            close_price = interday_data.loc[day]['Close'] if day in interday_data.index else position.entry_price
            equity += position.qty * close_price
        profit_pct = equity / self._daily_equity[-1] - 1 if self._daily_equity[-1] else 0
        self._daily_equity.append(equity)
        total_profit_pct = equity / self._daily_equity[0] - 1
        stats = [['Total Gain/Loss',
                  f'{total_profit_pct * 100:+.2f}%' if total_profit_pct < 10 else f'{total_profit_pct:+.4g}',
                  'Daily Gain/Loss', f'{profit_pct * 100:+.2f}%']]

        outputs.append('[ Stats ]')
        outputs.append(tabulate.tabulate(stats, tablefmt='grid', disable_numparse=True))

        if not executed_closes and not self._positions:
            return
        self._details_log.info('\n'.join(outputs))

    def _print_summary(self) -> None:
        def _profit_to_str(profit_num: float) -> str:
            return f'{profit_num * 100:+.2f}%' if profit_num < 10 else f'{profit_num:+.4g}'

        outputs = [get_header('Summary')]
        n_trades = self._num_win + self._num_lose
        win_rate = self._num_win / n_trades if n_trades > 0 else 0
        market_dates = self._market_dates[:len(self._daily_equity) - 1]
        if not market_dates:
            return
        summary = [['Time Range', f'{market_dates[0]} ~ {market_dates[-1]}'],
                   ['Win Rate', f'{win_rate * 100:.2f}%'],
                   ['Num of Trades', f'{n_trades} ({n_trades / len(market_dates):.2f} per day)'],
                   ['Output Dir', os.path.relpath(self._output_dir, BASE_DIR)]]
        outputs.append('[ Basic Info ]')
        outputs.append(tabulate.tabulate(summary, tablefmt='grid'))

        processor_stats = [['Processor', 'Gain/Loss', 'Win Rate', 'Num of Trades']]
        for processor_name in sorted(self._processor_stats.keys()):
            current_stats = self._processor_stats[processor_name]
            processor_n_trade = current_stats['num_win'] + current_stats['num_lose']
            processor_win_rate = current_stats['num_win'] / processor_n_trade
            processor_win_rate_ci = compute_bernoulli_ci95(processor_win_rate, processor_n_trade)
            processor_stats.append([
                processor_name,
                _profit_to_str(current_stats['profit']),
                f'{processor_win_rate * 100:.2f}% \xB1 {processor_win_rate_ci * 100:.2f}%',
                f'{processor_n_trade} ({processor_n_trade / len(market_dates):.2f} per day)'])
        outputs.append('[ Processor Performance ]')
        outputs.append(tabulate.tabulate(processor_stats, tablefmt='grid'))

        self._transactions.sort(key=lambda s: s.gl_pct)
        for title, transactions, judge in zip(['[ Best Trades ]', '[ Worst Trades ]'],
                                              [self._transactions[::-1][:5], self._transactions[:5]],
                                              [lambda p: p > 0, lambda p: p < 0]):
            tx_list = [[t.symbol, t.processor, t.entry_time.strftime('%F'), t.entry_time.time(),
                        t.exit_time.time(), 'long' if t.is_long else 'short', f'{t.gl_pct * 100:+.2f}%']
                       for t in transactions if judge(t.gl_pct)]
            if not tx_list:
                continue
            tx_table = tabulate.tabulate(tx_list,
                                         headers=['Symbol', 'Processor', 'Entry Date', 'Entry Time',
                                                  'Exit Time', 'Side', 'Gain/Loss'],
                                         tablefmt='grid',
                                         disable_numparse=True)
            outputs.append(title)
            outputs.append(tx_table)

        print_symbols = ['QQQ', 'SPY', 'TQQQ']
        market_symbol = 'SPY'
        stats = [['', 'My Portfolio'] + print_symbols]
        current_year = self._start_date.year
        current_start = 0
        for i, date in enumerate(market_dates):
            if i != len(market_dates) - 1 and market_dates[i + 1].year != current_year + 1:
                continue
            year_market_last_day_index = timestamp_to_index(self._interday_dataset[market_symbol].index,
                                                            pd.Timestamp(date).tz_localize(TIME_ZONE))
            year_market_values = self._interday_dataset[market_symbol]['Close'].tolist()[
                                 year_market_last_day_index - (i - current_start) - 1:year_market_last_day_index + 1]
            year_profit_number = self._daily_equity[i + 1] / self._daily_equity[current_start] - 1
            year_profit = [f'{current_year} Gain/Loss', _profit_to_str(year_profit_number)]
            _, _, year_sharpe_ratio = compute_risks(
                self._daily_equity[current_start: i + 2], year_market_values)
            year_sharpe = [f'{current_year} Sharpe Ratio',
                           f'{year_sharpe_ratio:.2f}' if not math.isnan(year_sharpe_ratio) else 'N/A']
            for symbol in print_symbols:
                if symbol not in self._interday_dataset:
                    continue
                last_day_index = timestamp_to_index(self._interday_dataset[symbol].index,
                                                    pd.Timestamp(date).tz_localize(TIME_ZONE))
                symbol_values = self._interday_dataset[symbol]['Close'].tolist()[
                                last_day_index - (i - current_start) - 1:last_day_index + 1]
                symbol_profit_pct = (symbol_values[-1] / symbol_values[0] - 1) * 100
                _, _, symbol_sharpe = compute_risks(
                    symbol_values, year_market_values)
                year_profit.append(f'{symbol_profit_pct:+.2f}%')
                year_sharpe.append(f'{symbol_sharpe:.2f}' if not math.isnan(symbol_sharpe) else 'N/A')
            stats.append(year_profit)
            stats.append(year_sharpe)
            current_start = i
            current_year += 1
        total_profit_number = self._daily_equity[-1] / self._daily_equity[0] - 1
        total_profit = ['Total Gain/Loss', _profit_to_str(total_profit_number)]
        market_first_day_index = timestamp_to_index(self._interday_dataset[market_symbol].index,
                                                    pd.Timestamp(market_dates[0]).tz_localize(TIME_ZONE))
        market_last_day_index = timestamp_to_index(self._interday_dataset[market_symbol].index,
                                                   pd.Timestamp(market_dates[-1]).tz_localize(TIME_ZONE))
        market_values = self._interday_dataset[market_symbol]['Close'].tolist()[
                        market_first_day_index - 1:market_last_day_index + 1]
        my_alpha, my_beta, my_sharpe_ratio = compute_risks(self._daily_equity, market_values)
        my_drawdown, my_hi, my_li = compute_drawdown(self._daily_equity)
        my_drawdown_start = market_dates[max(my_hi - 1, 0)]
        my_drawdown_end = market_dates[max(my_li - 1, 0)]
        alpha_row = ['Alpha', f'{my_alpha * 100:.2f}%' if not math.isnan(my_alpha) else 'N/A']
        beta_row = ['Beta', f'{my_beta:.2f}' if not math.isnan(my_beta) else 'N/A']
        sharpe_ratio_row = ['Sharpe Ratio',
                            f'{my_sharpe_ratio:.2f}' if not math.isnan(my_sharpe_ratio) else 'N/A']
        drawdown_row = ['Drawdown', f'{my_drawdown * 100:+.2f}%']
        drawdown_start_row = ['Drawdown Start', my_drawdown_start.strftime('%F')]
        drawdown_end_row = ['Drawdown End', my_drawdown_end.strftime('%F')]
        for symbol in print_symbols:
            first_day_index = timestamp_to_index(self._interday_dataset[symbol].index,
                                                 pd.Timestamp(market_dates[0]).tz_localize(TIME_ZONE))
            last_day_index = timestamp_to_index(self._interday_dataset[symbol].index,
                                                pd.Timestamp(market_dates[-1]).tz_localize(TIME_ZONE))
            symbol_values = self._interday_dataset[symbol]['Close'].tolist()[first_day_index - 1:last_day_index + 1]
            symbol_total_profit_pct = (symbol_values[-1] / symbol_values[0] - 1) * 100
            total_profit.append(f'{symbol_total_profit_pct:+.2f}%')
            symbol_alpha, symbol_beta, symbol_sharpe_ratio = compute_risks(
                symbol_values, market_values)
            alpha_row.append(
                f'{symbol_alpha * 100:.2f}%' if not math.isnan(symbol_alpha) else 'N/A')
            beta_row.append(
                f'{symbol_beta:.2f}' if not math.isnan(symbol_beta) else 'N/A')
            sharpe_ratio_row.append(f'{symbol_sharpe_ratio:.2f}'
                                    if not math.isnan(symbol_sharpe_ratio) else 'N/A')
            symbol_drawdown, symbol_hi, symbol_li = compute_drawdown(symbol_values)
            symbol_drawdown_start = market_dates[max(symbol_hi - 1, 0)]
            symbol_drawdown_end = market_dates[max(symbol_li - 1, 0)]
            drawdown_row.append(f'{symbol_drawdown * 100:+.2f}%')
            drawdown_start_row.append(symbol_drawdown_start.strftime('%F'))
            drawdown_end_row.append(symbol_drawdown_end.strftime('%F'))
        stats.append(total_profit)
        stats.append(alpha_row)
        stats.append(beta_row)
        stats.append(sharpe_ratio_row)
        stats.append(drawdown_row)
        stats.append(drawdown_start_row)
        stats.append(drawdown_end_row)
        outputs.append('[ Statistics ]')
        outputs.append(tabulate.tabulate(stats, tablefmt='grid', disable_numparse=True))
        self._summary_log.info('\n'.join(outputs))

    def _plot_summary(self) -> None:
        pd.plotting.register_matplotlib_converters()
        plot_symbols = ['QQQ', 'SPY', 'TQQQ']
        color_map = {'QQQ': '#78d237', 'SPY': '#FF6358', 'TQQQ': '#aa46be'}
        formatter = mdates.DateFormatter('%m-%d')
        current_year = self._start_date.year
        current_start = 0
        dates, values = [], [1]
        market_dates = self._market_dates[:len(self._daily_equity) - 1]
        for i, date in enumerate(market_dates):
            dates.append(date)
            values.append(self._daily_equity[i + 1] / self._daily_equity[current_start])
            if i != len(market_dates) - 1 and market_dates[i + 1].year != current_year + 1:
                continue
            dates = [dates[0] - datetime.timedelta(days=1)] + dates
            profit_pct = (self._daily_equity[i + 1] / self._daily_equity[current_start] - 1) * 100
            plt.figure(figsize=(10, 4))
            plt.plot(dates, values,
                     label=f'My Portfolio ({profit_pct:+.2f}%)',
                     color='#28b4c8')
            yscale = 'linear'
            for symbol in plot_symbols:
                if symbol not in self._interday_dataset:
                    continue
                last_day_index = timestamp_to_index(self._interday_dataset[symbol].index,
                                                    pd.Timestamp(date).tz_localize(TIME_ZONE))
                symbol_values = list(self._interday_dataset[symbol]['Close'][
                                     last_day_index + 1 - len(dates):last_day_index + 1])
                for j in range(len(symbol_values) - 1, -1, -1):
                    symbol_values[j] /= symbol_values[0]
                if symbol == 'TQQQ':
                    if abs(symbol_values[-1] - 1) > 2 * abs(values[-1] - 1):
                        continue
                    elif abs(values[-1] - 1) > 3 * abs(symbol_values[-1] - 1):
                        yscale = 'log'
                plt.plot(dates, symbol_values,
                         label=f'{symbol} ({(symbol_values[-1] - 1) * 100:+.2f}%)',
                         color=color_map[symbol])
            text_kwargs = {'family': 'monospace'}
            plt.xlabel('Date', **text_kwargs)
            plt.ylabel('Normalized Value', **text_kwargs)
            plt.title(f'{current_year} History', **text_kwargs, y=1.15)
            plt.grid(linestyle='--', alpha=0.5)
            plt.legend(ncol=len(plot_symbols) + 1, bbox_to_anchor=(0, 1),
                       loc='lower left', prop=text_kwargs)
            ax = plt.gca()
            ax.spines['right'].set_color('none')
            ax.spines['top'].set_color('none')
            ax.xaxis.set_major_formatter(formatter)
            plt.yscale(yscale)
            plt.tight_layout()
            plt.savefig(os.path.join(self._output_dir, f'{current_year}.png'))
            plt.close()

            dates, values = [], [1]
            current_start = i
            current_year += 1

    def _print_profile(self):
        if self._run_start_time is None:
            return
        txt_output = os.path.join(self._output_dir, 'profile.txt')
        total_time = max(time.time() - self._run_start_time, 1E-7)
        data_process_time = max(float(np.sum(list(self._processor_time.values()))), 1E-7)
        outputs = [get_header('Profile')]
        stage_profile = [
            ['Stage', 'Time Cost (s)', 'Percentage'],
            ['Total', f'{total_time:.0f}', '100%'],
            ['Interday Data Load', f'{self._interday_load_time:.0f}',
             f'{self._interday_load_time / total_time * 100:.0f}%'],
            ['Intraday Data Load', f'{self._intraday_load_time:.0f}',
             f'{self._intraday_load_time / total_time * 100:.0f}%'],
            ['Stock Universe Load', f'{self._stock_universe_load_time:.0f}',
             f'{self._stock_universe_load_time / total_time * 100:.0f}%'],
            ['Context Prepare', f'{self._context_prep_time:.0f}',
             f'{self._context_prep_time / total_time * 100:.0f}%'],
            ['Data Process', f'{data_process_time:.0f}',
             f'{data_process_time / total_time * 100:.0f}%'],
        ]
        outputs.append(tabulate.tabulate(stage_profile, tablefmt='grid'))
        processor_profile = [
            ['Processor', 'Time Cost (s)', 'Percentage'],
            ['Total', f'{data_process_time:.0f}', '100%'], ]
        for processor_name, processor_time in self._processor_time.items():
            processor_profile.append([processor_name, f'{processor_time:.0f}',
                                      f'{processor_time / data_process_time * 100:.0f}%'])
        outputs.append(tabulate.tabulate(processor_profile, tablefmt='grid'))
        with open(txt_output, 'w') as f:
            f.write('\n'.join(outputs))
