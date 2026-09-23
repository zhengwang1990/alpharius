import collections
import datetime
import difflib
import functools
import math
import os
import pathlib
import signal
import threading
import time

import numpy as np
import pandas as pd
import tabulate
from alpaca import trading

try:
    import git
except ImportError:
    git = None

from alpharius.data import (
    DataClient,
    load_interday_dataset,
    load_intraday_dataset,
)
from alpharius.utils import (
    TIME_ZONE,
    ChartSeries,
    DiffFile,
    ReportChart,
    ReportHighlight,
    ReportTable,
    Transaction,
    compute_bernoulli_ci95,
    compute_drawdown,
    compute_risks,
    count_changed_lines,
    get_all_symbols,
    get_trading_client,
    highlight_diff_table,
    profit_to_str,
    render_diff_page,
    render_report_page,
    round_values,
)

from .common import (
    BASE_DIR,
    BID_ASK_SPREAD,
    INTERDAY_LOOKBACK_LOAD,
    MARKET_CLOSE,
    MARKET_OPEN,
    OUTPUT_DIR,
    SHORT_RESERVE_RATIO,
    get_header,
    get_unique_actions,
    logging_config,
    timestamp_to_index,
)
from .enums import ActionType, Mode, TradingFrequency
from .processors.processor import Processor, instantiate_processor
from .structs import Action, Context, Position

_MAX_WORKERS = 20


class Backtest:
    def __init__(
        self,
        start_date: pd.Timestamp | str,
        end_date: pd.Timestamp | str,
        processors: list[type[Processor] | Processor],
        data_client: DataClient,
        ack_all: bool | None = False,
    ) -> None:
        if isinstance(start_date, str):
            start_date = pd.to_datetime(start_date)
        if isinstance(end_date, str):
            end_date = pd.to_datetime(end_date)
        self._start_date = start_date
        self._end_date = end_date
        self._processor_classes = processors
        self._processors: list[Processor] = []
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
            output_dir = os.path.join(
                backtesting_output_dir, datetime.datetime.now().strftime('%m-%d'), f'{self._output_num:02d}'
            )
            if not os.path.exists(output_dir):
                self._output_dir = output_dir
                os.makedirs(output_dir, exist_ok=True)
                break
            self._output_num += 1

        self._details_log = logging_config(os.path.join(self._output_dir, 'details.txt'), detail=False, name='details')
        self._summary_log = logging_config(detail=False, name='summary')

        trading_client = get_trading_client()
        calendar = trading_client.get_calendar(
            filters=trading.GetCalendarRequest(
                start=self._start_date.date(),
                end=(self._end_date - datetime.timedelta(days=1)).date(),
            )
        )
        self._market_dates = [market_day.date for market_day in calendar if market_day.date < self._end_date.date()]
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, self._safe_exit)

        self._run_start_time = None
        self._interday_load_time = 0
        self._intraday_load_time = 0
        self._stock_universe_load_time = 0
        self._context_prep_time = 0
        self._transactions = []
        self._has_diff = False
        self._processor_time = collections.defaultdict(int)

    def _safe_exit(self, signum, frame) -> None:
        self._close()
        exit(1)

    def _close(self):
        self._write_report()
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
                output_dir=self._output_dir,
            )
            self._processors.append(processor)

    @staticmethod
    def _read_lines(path: str, max_chars: int | None = None) -> list[str] | None:
        """Reads a file of the repo as lines, or None if it is longer than max_chars."""
        with open(os.path.join(BASE_DIR, path), 'r', encoding='utf-8') as f:
            content = f.read() if max_chars is None else f.read(max_chars + 1)
        if max_chars is not None and len(content) > max_chars:
            return None
        return content.split('\n')

    @staticmethod
    def _diff_file(
        path: str, old_content: list[str], new_content: list[str], status: str, old_path: str | None = None
    ) -> DiffFile:
        diff_table = difflib.HtmlDiff(wrapcolumn=120).make_table(old_content, new_content, context=True)
        if path.endswith('.py'):
            diff_table = highlight_diff_table(diff_table)
        added, removed = count_changed_lines(old_content, new_content)
        return DiffFile(path=path, table=diff_table, status=status, added=added, removed=removed, old_path=old_path)

    def _record_diff(self):
        repo = git.Repo(BASE_DIR)
        files = []
        for item in repo.head.commit.diff(None):
            old_content, new_content = [], []
            try:
                if item.change_type != 'A':
                    old_content = item.a_blob.data_stream.read().decode('utf-8').split('\n')
                if item.change_type != 'D':
                    new_content = self._read_lines(item.b_path)
            except UnicodeDecodeError:
                # Binary file
                continue
            status = 'R' if item.renamed_file else item.change_type
            files.append(
                self._diff_file(
                    item.b_path, old_content, new_content, status, item.a_path if item.renamed_file else None
                )
            )
        # The diff above only knows files that git tracks (or that were added to the index)
        for path in repo.untracked_files:
            try:
                new_content = self._read_lines(path, max_chars=1_000_000)
            except (UnicodeDecodeError, OSError):
                continue
            if new_content is not None:
                files.append(self._diff_file(path, [], new_content, 'A'))
        files.sort(key=lambda f: f.path)
        if files:
            html_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'html')
            with open(os.path.join(html_dir, 'diff.html'), 'r', encoding='utf-8') as f:
                template = f.read()
            commit = repo.head.commit
            page = render_diff_page(
                template,
                files,
                output_num=self._output_num,
                logo_uri=pathlib.Path(html_dir, 'diff.png').as_uri(),
                base_commit=str(commit.hexsha)[:7],
                base_message=str(commit.summary),
            )
            with open(os.path.join(self._output_dir, 'diff.html'), 'w', encoding='utf-8') as f:
                f.write(page)
            self._has_diff = True

    def run(self) -> list[Transaction]:
        self._run_start_time = time.time()
        if git is not None:
            try:
                self._record_diff()
            except (ValueError, git.GitError) as e:
                # Git doesn't work in some circumstances
                self._summary_log.warning(f'Diff can not be generated: {e}')
        history_start = self._start_date - datetime.timedelta(days=INTERDAY_LOOKBACK_LOAD)
        self._interday_dataset = load_interday_dataset(
            get_all_symbols(), history_start, self._end_date, self._data_client
        )
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
    ) -> tuple[dict[str, list[str]], dict[TradingFrequency, set[str]]]:
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

    def _process_data(
        self, contexts: dict[str, Context], stock_universes: dict[str, list[str]], processors: list[Processor]
    ) -> list[Action]:
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
            actions.extend(
                [
                    Action(pa.symbol, pa.type, pa.percent, contexts[pa.symbol].current_price, processor)
                    for pa in processor_actions
                ]
            )
            self._processor_time[processor_name] += time.time() - data_process_start
        return actions

    @functools.lru_cache(maxsize=1000)
    def _prepare_interday_lookback(self, day: pd.Timestamp, symbol: str) -> pd.DataFrame | None:
        if symbol not in self._interday_dataset:
            return
        interday_data = self._interday_dataset[symbol]
        interday_ind = timestamp_to_index(interday_data.index, pd.Timestamp(day).tz_localize(TIME_ZONE))
        if interday_ind is None:
            return
        interday_lookback = interday_data.iloc[:interday_ind]
        return interday_lookback

    @staticmethod
    def _prepare_intraday_lookback(
        current_interval_start: pd.Timestamp, symbol: str, intraday_datas: dict[str, pd.DataFrame]
    ) -> pd.DataFrame | None:
        intraday_data = intraday_datas[symbol]
        intraday_ind = timestamp_to_index(intraday_data.index, current_interval_start)
        if intraday_ind is None:
            return
        intraday_lookback = intraday_data.iloc[: intraday_ind + 1]
        return intraday_lookback

    def _load_intraday_data(
        self, day: pd.Timestamp, stock_universe: dict[TradingFrequency, set[str]]
    ) -> dict[str, pd.DataFrame]:
        load_intraday_start = time.time()
        unique_symbols = set()
        for _, symbols in stock_universe.items():
            unique_symbols.update(symbols)
        intraday_dataset = load_intraday_dataset(unique_symbols, day, self._data_client)
        self._intraday_load_time += time.time() - load_intraday_start
        return intraday_dataset

    def _process(self, day: datetime.date) -> list[Transaction]:
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
                frequency_to_process = [TradingFrequency.FIVE_MIN, TradingFrequency.CLOSE_TO_OPEN]
            elif current_time == market_close:
                frequency_to_process = [
                    TradingFrequency.FIVE_MIN,
                    TradingFrequency.CLOSE_TO_OPEN,
                    TradingFrequency.CLOSE_TO_CLOSE,
                ]

            prep_context_start = time.time()
            contexts = dict()
            unique_symbols = set()
            for frequency, symbols in stock_universe.items():
                if frequency in frequency_to_process:
                    unique_symbols.update(symbols)
            for symbol in unique_symbols:
                intraday_lookback = self._prepare_intraday_lookback(current_interval_start, symbol, intraday_datas)
                if intraday_lookback is None or len(intraday_lookback) == 0:
                    continue
                interday_lookback = self._prepare_interday_lookback(day, symbol)
                if interday_lookback is None or len(interday_lookback) == 0:
                    continue
                current_price = intraday_lookback['Close'].iloc[-1]
                context = Context(
                    symbol=symbol,
                    current_time=current_time,
                    current_price=current_price,
                    interday_lookback=interday_lookback,
                    intraday_lookback=intraday_lookback,
                    mode=Mode.BACKTEST,
                )
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

    def _process_actions(self, current_time: pd.Timestamp, actions: list[Action]) -> list[Transaction]:
        unique_actions = get_unique_actions(actions)

        close_actions = [
            action for action in unique_actions if action.type in [ActionType.BUY_TO_CLOSE, ActionType.SELL_TO_CLOSE]
        ]
        executed_closes = self._close_positions(current_time, close_actions)

        open_actions = [
            action for action in unique_actions if action.type in [ActionType.BUY_TO_OPEN, ActionType.SELL_TO_OPEN]
        ]
        self._open_positions(current_time, open_actions)

        return executed_closes

    def _pop_current_position(self, symbol: str) -> Position | None:
        for ind, position in enumerate(self._positions):
            if position.symbol == symbol:
                current_position = self._positions.pop(ind)
                return current_position
        return None

    def _get_current_position(self, symbol: str) -> Position | None:
        for position in self._positions:
            if position.symbol == symbol:
                return position
        return None

    def _close_positions(self, current_time: pd.Timestamp, actions: list[Action]) -> list[Transaction]:
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
            if abs(new_qty) > 1e-7:
                self._positions.append(
                    Position(symbol, new_qty, current_position.entry_price, current_position.entry_time, new_portion)
                )
            spread_adjust = 1 - BID_ASK_SPREAD if action.type == ActionType.SELL_TO_CLOSE else 1 + BID_ASK_SPREAD
            adjusted_action_price = action.price * spread_adjust
            self._cash += adjusted_action_price * qty
            profit = adjusted_action_price / current_position.entry_price - 1
            if action.type == ActionType.BUY_TO_CLOSE:
                profit *= -1
            processor_stats = self._processor_stats.setdefault(
                action.processor.name, {'profit': 0.0, 'num_win': 0, 'num_lose': 0}
            )
            if profit > 0:
                self._num_win += 1
                processor_stats['num_win'] += 1
            else:
                self._num_lose += 1
                processor_stats['num_lose'] += 1
            one_time_processor_profit[action.processor.name] += portion * profit
            executed_actions.append(
                Transaction(
                    symbol,
                    action.type == ActionType.SELL_TO_CLOSE,
                    action.processor.name,
                    current_position.entry_price,
                    action.price,
                    current_position.entry_time,
                    current_time,
                    qty,
                    profit * qty * current_position.entry_price,
                    profit,
                    None,
                    None,
                )
            )
        for processor_name, profit in one_time_processor_profit.items():
            processor_stats = self._processor_stats.setdefault(
                processor_name, {'profit': 0.0, 'num_win': 0, 'num_lose': 0}
            )
            processor_stats['profit'] = (processor_stats['profit'] + 1) * (1 + profit) - 1
        self._transactions.extend(executed_actions)
        return executed_actions

    def _open_positions(self, current_time: pd.Timestamp, actions: list[Action]) -> None:
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
            existing_position = self._get_current_position(symbol)
            if existing_position is not None and existing_position.qty != 0:
                is_long_action = action.type == ActionType.BUY_TO_OPEN
                if (existing_position.qty > 0) != is_long_action:
                    continue
            portion = min(1 / len(actions), action.percent)
            # Use abs to avoid sign error caused by floating point error
            cash_to_trade = abs(tradable_cash * portion)
            if abs(cash_to_trade) > 1e-7 or self._ack_all:
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
                    entry_price = (old_position.entry_price * old_position.qty + action.price * qty) / (
                        old_position.qty + qty
                    )
                    entry_portion = old_position.entry_portion + entry_portion
                new_qty = qty + old_position.qty
            new_position = Position(symbol, new_qty, entry_price, current_time, entry_portion)
            self._positions.append(new_position)
            self._cash -= action.price * qty

    def _log_day(self, day: datetime.date, executed_closes: list[Transaction]) -> None:
        outputs = [get_header(day)]
        if executed_closes:
            table_list = [
                [
                    t.symbol,
                    t.processor,
                    t.entry_time.time(),
                    t.exit_time.time(),
                    'long' if t.is_long else 'short',
                    f'{t.entry_price:.4g}',
                    f'{t.exit_price:.4g}',
                    f'{t.gl_pct * 100:+.2f}%',
                ]
                for t in executed_closes
            ]
            trade_info = tabulate.tabulate(
                table_list,
                headers=[
                    'Symbol',
                    'Processor',
                    'Entry Time',
                    'Exit Time',
                    'Side',
                    'Entry Price',
                    'Exit Price',
                    'Gain/Loss',
                ],
                tablefmt='grid',
                disable_numparse=True,
            )
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
                position_info.append(
                    [
                        position.symbol,
                        f'{position.qty:.2g}',
                        f'{position.entry_price:.4g}',
                        f'{close_price:.4g}',
                        f'{value:.2g}',
                        f'{daily_change:+.2f}%' if daily_change is not None else None,
                        f'{change:+.2f}%' if change is not None else None,
                    ]
                )
            outputs.append('[ Positions ]')
            outputs.append(
                tabulate.tabulate(
                    position_info,
                    headers=[
                        'Symbol',
                        'Qty',
                        'Entry Price',
                        'Current Price',
                        'Current Value',
                        'Daily Change',
                        'Change',
                    ],
                    tablefmt='grid',
                    disable_numparse=True,
                )
            )

        equity = self._cash
        for position in self._positions:
            interday_data = self._interday_dataset[position.symbol]
            close_price = interday_data.loc[day]['Close'] if day in interday_data.index else position.entry_price
            equity += position.qty * close_price
        profit_pct = equity / self._daily_equity[-1] - 1 if self._daily_equity[-1] else 0
        self._daily_equity.append(equity)
        total_profit_pct = equity / self._daily_equity[0] - 1
        stats = [
            [
                'Total Gain/Loss',
                f'{total_profit_pct * 100:+.2f}%' if total_profit_pct < 10 else f'{total_profit_pct:+.4g}',
                'Daily Gain/Loss',
                f'{profit_pct * 100:+.2f}%',
            ]
        ]

        outputs.append('[ Stats ]')
        outputs.append(tabulate.tabulate(stats, tablefmt='grid', disable_numparse=True))

        if not executed_closes and not self._positions:
            return
        self._details_log.info('\n'.join(outputs))

    def _write_report(self) -> None:
        """Writes the summary, profile and charts of the run into a single tabbed HTML page.

        The summary is also printed to the console.
        """
        if self._run_start_time is None:
            return
        highlights, summary = self._get_summary()
        if summary:
            self._summary_log.info(self._tables_to_text('Summary', summary))
        profile = self._get_profile()
        charts = self._get_charts()
        market_dates = self._market_dates[: len(self._daily_equity) - 1]
        subtitle = f'{market_dates[0]} ~ {market_dates[-1]}' if market_dates else ''
        html_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'html')
        page = render_report_page(
            output_num=self._output_num,
            logo_uri=pathlib.Path(html_dir, 'report.png').as_uri(),
            subtitle=subtitle,
            highlights=highlights,
            summary=summary,
            profile=profile,
            charts=charts,
            diff_src='diff.html?embed' if self._has_diff else None,
        )
        with open(os.path.join(self._output_dir, 'report.html'), 'w', encoding='utf-8') as f:
            f.write(page)

    @staticmethod
    def _tables_to_text(title: str, tables: list[ReportTable]) -> str:
        """Lays out report tables as text for the console."""
        outputs = [get_header(title)]
        # The report shows the statistics before the trades, the text after
        for table in sorted(tables, key=lambda t: t.title == 'Statistics'):
            outputs.append(f'[ {table.title} ]')
            outputs.append(
                tabulate.tabulate(table.rows, headers=table.headers or (), tablefmt='grid', disable_numparse=True)
            )
        return '\n'.join(outputs)

    def _get_summary(self) -> tuple[list[ReportHighlight], list[ReportTable]]:
        """Gets the headline numbers and the tables of the summary tab."""

        n_trades = self._num_win + self._num_lose
        win_rate = self._num_win / n_trades if n_trades > 0 else 0
        market_dates = self._market_dates[: len(self._daily_equity) - 1]
        if not market_dates:
            return [], []
        summary = [
            ['Time Range', f'{market_dates[0]} ~ {market_dates[-1]}'],
            ['Win Rate', f'{win_rate * 100:.2f}%'],
            ['Num of Trades', f'{n_trades} ({n_trades / len(market_dates):.2f} per day)'],
            ['Output Dir', os.path.relpath(self._output_dir, BASE_DIR)],
        ]
        basic_info = ReportTable('Basic Info', summary)

        processor_stats = [['Processor', 'Gain/Loss', 'Win Rate', 'Num of Trades']]
        for processor_name in sorted(self._processor_stats.keys()):
            current_stats = self._processor_stats[processor_name]
            processor_n_trade = current_stats['num_win'] + current_stats['num_lose']
            processor_win_rate = current_stats['num_win'] / processor_n_trade
            processor_win_rate_ci = compute_bernoulli_ci95(processor_win_rate, processor_n_trade)
            processor_stats.append(
                [
                    processor_name,
                    profit_to_str(current_stats['profit']),
                    f'{processor_win_rate * 100:.2f}% \xb1 {processor_win_rate_ci * 100:.2f}%',
                    f'{processor_n_trade} ({processor_n_trade / len(market_dates):.2f} per day)',
                ]
            )
        processor_performance = ReportTable(
            'Processor Performance', processor_stats[1:], headers=processor_stats[0], numeric_from=1
        )

        self._transactions.sort(key=lambda s: s.gl_pct)
        trade_tables = []
        for title, transactions, judge in zip(
            ['Best Trades', 'Worst Trades'],
            [self._transactions[::-1][:5], self._transactions[:5]],
            [lambda p: p > 0, lambda p: p < 0],
        ):
            tx_list = [
                [
                    t.symbol,
                    t.processor,
                    t.entry_time.strftime('%F'),
                    t.entry_time.strftime('%H:%M:%S'),
                    t.exit_time.strftime('%H:%M:%S'),
                    'long' if t.is_long else 'short',
                    f'{t.gl_pct * 100:+.2f}%',
                ]
                for t in transactions
                if judge(t.gl_pct)
            ]
            if not tx_list:
                continue
            trade_tables.append(
                ReportTable(
                    title,
                    tx_list,
                    headers=['Symbol', 'Processor', 'Entry Date', 'Entry Time', 'Exit Time', 'Side', 'Gain/Loss'],
                    numeric_from=6,
                    copyable=True,
                    group='Best & Worst Trades',
                )
            )

        if len(self._processors) == 1:
            statistics, total_profit, sharpe_ratio, drawdown = self._get_yearly_statistics(market_dates)
        else:
            statistics, total_profit, sharpe_ratio, drawdown = self._get_market_comparison_statistics(market_dates)
        highlights = [
            ReportHighlight('Total Gain/Loss', total_profit),
            ReportHighlight('Sharpe Ratio', sharpe_ratio),
            ReportHighlight('Drawdown', drawdown),
            ReportHighlight('Win Rate', f'{win_rate * 100:.2f}%'),
            ReportHighlight('Trades', str(n_trades), f'{n_trades / len(market_dates):.2f} per day'),
        ]
        return highlights, [basic_info, processor_performance, statistics] + trade_tables

    def _get_market_comparison_statistics(self, market_dates: list[datetime.date]) -> tuple[ReportTable, str, str, str]:
        """Gets the statistics of the portfolio against the market symbols, by year and in total.

        Returns the table, and the total gain/loss, sharpe ratio and drawdown of the portfolio.
        """
        print_symbols = ['QQQ', 'SPY', 'TQQQ']
        market_symbol = 'SPY'
        stats = [['', 'My Portfolio'] + print_symbols]
        current_year = self._start_date.year
        current_start = 0
        for i, date in enumerate(market_dates):
            if i != len(market_dates) - 1 and market_dates[i + 1].year != current_year + 1:
                continue
            year_market_last_day_index = timestamp_to_index(
                self._interday_dataset[market_symbol].index, pd.Timestamp(date).tz_localize(TIME_ZONE)
            )
            year_market_values = self._interday_dataset[market_symbol]['Close'].tolist()[
                year_market_last_day_index - (i - current_start) - 1 : year_market_last_day_index + 1
            ]
            year_profit_number = self._daily_equity[i + 1] / self._daily_equity[current_start] - 1
            year_profit = [f'{current_year} Gain/Loss', profit_to_str(year_profit_number)]
            _, _, year_sharpe_ratio = compute_risks(self._daily_equity[current_start : i + 2], year_market_values)
            year_sharpe = [
                f'{current_year} Sharpe Ratio',
                f'{year_sharpe_ratio:.2f}' if not math.isnan(year_sharpe_ratio) else 'N/A',
            ]
            for symbol in print_symbols:
                if symbol not in self._interday_dataset:
                    continue
                last_day_index = timestamp_to_index(
                    self._interday_dataset[symbol].index, pd.Timestamp(date).tz_localize(TIME_ZONE)
                )
                symbol_values = self._interday_dataset[symbol]['Close'].tolist()[
                    last_day_index - (i - current_start) - 1 : last_day_index + 1
                ]
                symbol_profit_pct = (symbol_values[-1] / symbol_values[0] - 1) * 100
                _, _, symbol_sharpe = compute_risks(symbol_values, year_market_values)
                year_profit.append(f'{symbol_profit_pct:+.2f}%')
                year_sharpe.append(f'{symbol_sharpe:.2f}' if not math.isnan(symbol_sharpe) else 'N/A')
            stats.append(year_profit)
            stats.append(year_sharpe)
            current_start = i
            current_year += 1
        total_profit_number = self._daily_equity[-1] / self._daily_equity[0] - 1
        total_profit = ['Total Gain/Loss', profit_to_str(total_profit_number)]
        market_first_day_index = timestamp_to_index(
            self._interday_dataset[market_symbol].index, pd.Timestamp(market_dates[0]).tz_localize(TIME_ZONE)
        )
        market_last_day_index = timestamp_to_index(
            self._interday_dataset[market_symbol].index, pd.Timestamp(market_dates[-1]).tz_localize(TIME_ZONE)
        )
        market_values = self._interday_dataset[market_symbol]['Close'].tolist()[
            market_first_day_index - 1 : market_last_day_index + 1
        ]
        my_alpha, my_beta, my_sharpe_ratio = compute_risks(self._daily_equity, market_values)
        my_drawdown, my_hi, my_li = compute_drawdown(self._daily_equity)
        my_drawdown_start = market_dates[max(my_hi - 1, 0)]
        my_drawdown_end = market_dates[max(my_li - 1, 0)]
        alpha_row = ['Alpha', f'{my_alpha * 100:.2f}%' if not math.isnan(my_alpha) else 'N/A']
        beta_row = ['Beta', f'{my_beta:.2f}' if not math.isnan(my_beta) else 'N/A']
        sharpe_ratio_row = ['Sharpe Ratio', f'{my_sharpe_ratio:.2f}' if not math.isnan(my_sharpe_ratio) else 'N/A']
        drawdown_row = ['Drawdown', f'{my_drawdown * 100:+.2f}%']
        drawdown_start_row = ['Drawdown Start', my_drawdown_start.strftime('%F')]
        drawdown_end_row = ['Drawdown End', my_drawdown_end.strftime('%F')]
        for symbol in print_symbols:
            first_day_index = timestamp_to_index(
                self._interday_dataset[symbol].index, pd.Timestamp(market_dates[0]).tz_localize(TIME_ZONE)
            )
            last_day_index = timestamp_to_index(
                self._interday_dataset[symbol].index, pd.Timestamp(market_dates[-1]).tz_localize(TIME_ZONE)
            )
            symbol_values = self._interday_dataset[symbol]['Close'].tolist()[first_day_index - 1 : last_day_index + 1]
            symbol_total_profit_pct = (symbol_values[-1] / symbol_values[0] - 1) * 100
            total_profit.append(f'{symbol_total_profit_pct:+.2f}%')
            symbol_alpha, symbol_beta, symbol_sharpe_ratio = compute_risks(symbol_values, market_values)
            alpha_row.append(f'{symbol_alpha * 100:.2f}%' if not math.isnan(symbol_alpha) else 'N/A')
            beta_row.append(f'{symbol_beta:.2f}' if not math.isnan(symbol_beta) else 'N/A')
            sharpe_ratio_row.append(f'{symbol_sharpe_ratio:.2f}' if not math.isnan(symbol_sharpe_ratio) else 'N/A')
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
        statistics = ReportTable('Statistics', stats[1:], headers=stats[0], numeric_from=1, copyable=True)
        return statistics, total_profit[1], sharpe_ratio_row[1], drawdown_row[1]

    def _get_yearly_statistics(self, market_dates: list[datetime.date]) -> tuple[ReportTable, str, str, str]:
        """Gets the statistics of the portfolio alone, with a column for each year and one for the total.

        Returns the table, and the total gain/loss, sharpe ratio and drawdown of the portfolio.
        """
        market_symbol = 'SPY'
        market_closes = self._interday_dataset[market_symbol]['Close'].tolist()
        market_index = self._interday_dataset[market_symbol].index

        def _column(equity: list[float], market_values: list[float], first_equity_index: int) -> list[str]:
            alpha, beta, sharpe_ratio = compute_risks(equity, market_values)
            drawdown, drawdown_hi, drawdown_li = compute_drawdown(equity)
            drawdown_start = market_dates[max(first_equity_index + drawdown_hi - 1, 0)]
            drawdown_end = market_dates[max(first_equity_index + drawdown_li - 1, 0)]
            return [
                profit_to_str(equity[-1] / equity[0] - 1),
                f'{sharpe_ratio:.2f}' if not math.isnan(sharpe_ratio) else 'N/A',
                f'{alpha * 100:.2f}%' if not math.isnan(alpha) else 'N/A',
                f'{beta:.2f}' if not math.isnan(beta) else 'N/A',
                f'{drawdown * 100:+.2f}%',
                drawdown_start.strftime('%F'),
                drawdown_end.strftime('%F'),
            ]

        headers = ['']
        columns = []
        current_year = self._start_date.year
        current_start = 0
        for i, date in enumerate(market_dates):
            if i != len(market_dates) - 1 and market_dates[i + 1].year != current_year + 1:
                continue
            last_day_index = timestamp_to_index(market_index, pd.Timestamp(date).tz_localize(TIME_ZONE))
            market_values = market_closes[last_day_index - (i - current_start) - 1 : last_day_index + 1]
            headers.append(str(current_year))
            columns.append(_column(self._daily_equity[current_start : i + 2], market_values, current_start))
            current_start = i
            current_year += 1
        first_day_index = timestamp_to_index(market_index, pd.Timestamp(market_dates[0]).tz_localize(TIME_ZONE))
        last_day_index = timestamp_to_index(market_index, pd.Timestamp(market_dates[-1]).tz_localize(TIME_ZONE))
        headers.append('Total')
        columns.append(_column(self._daily_equity, market_closes[first_day_index - 1 : last_day_index + 1], 0))
        labels = ['Gain/Loss', 'Sharpe Ratio', 'Alpha', 'Beta', 'Drawdown', 'Drawdown Start', 'Drawdown End']
        rows = [[label] + [column[k] for column in columns] for k, label in enumerate(labels)]
        statistics = ReportTable('Statistics', rows, headers=headers, numeric_from=1, copyable=True)
        return statistics, columns[-1][0], columns[-1][1], columns[-1][4]

    def _get_charts(self) -> list[ReportChart]:
        """Gets the portfolio history of each year, against the market unless there is a single processor."""
        plot_symbols = [] if len(self._processors) == 1 else ['QQQ', 'SPY', 'TQQQ']
        color_map = {'QQQ': '#78d237', 'SPY': '#FF6358', 'TQQQ': '#aa46be'}
        charts = []
        current_year = self._start_date.year
        current_start = 0
        dates, values = [], [1]
        market_dates = self._market_dates[: len(self._daily_equity) - 1]
        for i, date in enumerate(market_dates):
            dates.append(date)
            values.append(self._daily_equity[i + 1] / self._daily_equity[current_start])
            if i != len(market_dates) - 1 and market_dates[i + 1].year != current_year + 1:
                continue
            dates = [dates[0] - datetime.timedelta(days=1)] + dates
            profit_pct = (self._daily_equity[i + 1] / self._daily_equity[current_start] - 1) * 100
            series = [ChartSeries(f'My Portfolio ({profit_pct:+.2f}%)', round_values(values), '#28b4c8')]
            log_scale = False
            for symbol in plot_symbols:
                if symbol not in self._interday_dataset:
                    continue
                last_day_index = timestamp_to_index(
                    self._interday_dataset[symbol].index, pd.Timestamp(date).tz_localize(TIME_ZONE)
                )
                symbol_values = list(
                    self._interday_dataset[symbol]['Close'][last_day_index + 1 - len(dates) : last_day_index + 1]
                )
                for j in range(len(symbol_values) - 1, -1, -1):
                    symbol_values[j] /= symbol_values[0]
                if symbol == 'TQQQ':
                    if abs(symbol_values[-1] - 1) > 2 * abs(values[-1] - 1):
                        continue
                    elif abs(values[-1] - 1) > 3 * abs(symbol_values[-1] - 1):
                        log_scale = True
                series.append(
                    ChartSeries(
                        f'{symbol} ({(symbol_values[-1] - 1) * 100:+.2f}%)',
                        round_values(symbol_values),
                        color_map[symbol],
                    )
                )
            charts.append(ReportChart(f'{current_year} History', [d.strftime('%F') for d in dates], series, log_scale))

            dates, values = [], [1]
            current_start = i
            current_year += 1
        return charts

    def _get_profile(self) -> list[ReportTable]:
        """Gets the tables of the profile tab: where the run spent its time."""
        if self._run_start_time is None:
            return []
        total_time = max(time.time() - self._run_start_time, 1e-7)
        data_process_time = max(float(np.sum(list(self._processor_time.values()))), 1e-7)
        stage_profile = [
            ['Stage', 'Time Cost (s)', 'Percentage'],
            ['Total', f'{total_time:.0f}', '100%'],
            [
                'Interday Data Load',
                f'{self._interday_load_time:.0f}',
                f'{self._interday_load_time / total_time * 100:.0f}%',
            ],
            [
                'Intraday Data Load',
                f'{self._intraday_load_time:.0f}',
                f'{self._intraday_load_time / total_time * 100:.0f}%',
            ],
            [
                'Stock Universe Load',
                f'{self._stock_universe_load_time:.0f}',
                f'{self._stock_universe_load_time / total_time * 100:.0f}%',
            ],
            ['Context Prepare', f'{self._context_prep_time:.0f}', f'{self._context_prep_time / total_time * 100:.0f}%'],
            ['Data Process', f'{data_process_time:.0f}', f'{data_process_time / total_time * 100:.0f}%'],
        ]
        processor_profile = [
            ['Processor', 'Time Cost (s)', 'Percentage'],
            ['Total', f'{data_process_time:.0f}', '100%'],
        ]
        for processor_name, processor_time in self._processor_time.items():
            processor_profile.append(
                [processor_name, f'{processor_time:.0f}', f'{processor_time / data_process_time * 100:.0f}%']
            )
        return [
            ReportTable('Stages', stage_profile[1:], headers=stage_profile[0], numeric_from=1),
            ReportTable('Processors', processor_profile[1:], headers=processor_profile[0], numeric_from=1),
        ]
