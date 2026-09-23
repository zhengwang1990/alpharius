import datetime

import git
import pandas as pd
import pytest

from alpharius import trade
from alpharius.trade import PROCESSORS, backtest

from ..fakes import FakeDataClient, FakeProcessor

FREQUENCIES = [
    trade.TradingFrequency.FIVE_MIN,
    trade.TradingFrequency.CLOSE_TO_CLOSE,
    trade.TradingFrequency.CLOSE_TO_OPEN,
]


@pytest.fixture(autouse=True)
def mock_git(mocker):
    mock_repo = mocker.MagicMock()
    mock_repo.head.commit.diff.return_value = []
    mock_repo.untracked_files = []
    mocker.patch.object(git, 'Repo', return_value=mock_repo)
    return mock_repo


def _diff_item(mocker, change_type, path, old_content=b'', old_path=None):
    item = mocker.MagicMock()
    item.change_type = change_type
    item.a_path = old_path or path
    item.b_path = path
    item.renamed_file = old_path is not None
    item.a_blob.data_stream.read.return_value = old_content
    return item


def _run_backtest(start_date, end_date, processors):
    backtesting = trade.Backtest(
        start_date=start_date, end_date=end_date, processors=processors, data_client=FakeDataClient()
    )
    backtesting.run()
    return backtesting


def _report_args(render_report_page):
    """The arguments the report page was rendered with."""
    assert render_report_page.call_count == 1
    return render_report_page.call_args.kwargs


def _titles(tables):
    return [table.title for table in tables]


def test_diff(mock_git, mocker):
    render_diff_page = mocker.spy(backtest, 'render_diff_page')
    render_report_page = mocker.spy(backtest, 'render_report_page')
    mock_git.head.commit.diff.return_value = [
        _diff_item(mocker, 'M', 'changed.py', b'line1\nline2'),
        _diff_item(mocker, 'A', 'added.txt'),
        _diff_item(mocker, 'D', 'deleted.py', b'gone\n'),
        _diff_item(mocker, 'M', 'renamed_new.py', b'same\n', old_path='renamed_old.py'),
        _diff_item(mocker, 'M', 'image.png', b'\xff\xfe\x00'),
    ]
    mock_git.untracked_files = ['brand_new.py', 'huge.log', 'unreadable.bin']
    read_lines = backtest.Backtest._read_lines

    def fake_read_lines(path, max_chars=None):
        if path == 'huge.log':
            return None
        if path == 'unreadable.bin':
            raise OSError('unreadable')
        return read_lines(path, max_chars)

    mocker.patch.object(backtest.Backtest, '_read_lines', staticmethod(fake_read_lines))

    # Strings are accepted as dates
    _run_backtest('2021-03-17', '2021-03-18', [FakeProcessor(trading_frequency=trade.TradingFrequency.FIVE_MIN)])

    files = render_diff_page.call_args.args[1]
    # Sorted by path. The binary file, the file that is too large and the unreadable one are left out.
    assert [(f.path, f.status, f.old_path) for f in files] == [
        ('added.txt', 'A', None),
        ('brand_new.py', 'A', None),
        ('changed.py', 'M', None),
        ('deleted.py', 'D', None),
        ('renamed_new.py', 'R', 'renamed_old.py'),
    ]
    changed = next(f for f in files if f.path == 'changed.py')
    assert (changed.added, changed.removed) == (1, 2)  # Both old lines are replaced by the "data" of the mocked file
    assert _report_args(render_report_page)['diff_src'] == 'diff.html?embed'

    # The run goes on without a diff when git does not work
    render_diff_page.reset_mock()
    render_report_page.reset_mock()
    mocker.patch.object(git, 'Repo', side_effect=git.GitError('no repository'))
    _run_backtest('2021-03-17', '2021-03-18', [FakeProcessor(trading_frequency=trade.TradingFrequency.FIVE_MIN)])
    assert render_diff_page.call_count == 0
    assert _report_args(render_report_page)['diff_src'] is None


def test_report_single_processor(mocker):
    render_report_page = mocker.spy(backtest, 'render_report_page')
    processor = FakeProcessor(trading_frequency=trade.TradingFrequency.FIVE_MIN)

    # The run spans two years
    backtesting = _run_backtest(pd.to_datetime('2020-12-21'), pd.to_datetime('2021-01-12'), [processor])

    assert processor.get_stock_universe_call_count > 0 and processor.process_data_call_count > 0
    args = _report_args(render_report_page)
    labels = [h.label for h in args['highlights']]
    assert labels == ['Total Gain/Loss', 'Sharpe Ratio', 'Drawdown', 'Win Rate', 'Trades']
    assert _titles(args['summary'])[:3] == ['Basic Info', 'Processor Performance', 'Statistics']
    assert _titles(args['profile']) == ['Stages', 'Processors']
    # Only the portfolio is compared with itself: no market columns, no market lines
    statistics = next(t for t in args['summary'] if t.title == 'Statistics')
    assert statistics.headers == ['', '2020', '2021', 'Total']
    assert [row[0] for row in statistics.rows] == [
        'Gain/Loss',
        'Sharpe Ratio',
        'Alpha',
        'Beta',
        'Drawdown',
        'Drawdown Start',
        'Drawdown End',
    ]
    assert _titles(args['charts']) == ['2020 History', '2021 History']
    for chart in args['charts']:
        assert [s.label.split(' (')[0] for s in chart.series] == ['My Portfolio']
        assert len(chart.labels) == len(chart.series[0].values)
    assert backtesting._get_summary()[1] == args['summary']


def test_open_positions_skips_opposite_direction_conflict(mocker):
    """A processor's open must not be merged into another processor's opposite-direction position."""
    backtesting = trade.Backtest(
        start_date='2021-03-17', end_date='2021-03-18', processors=[], data_client=FakeDataClient()
    )
    processor_a = mocker.MagicMock(name='ProcessorA')
    processor_b = mocker.MagicMock(name='ProcessorB')
    open_time = pd.Timestamp('2021-03-17 09:35', tz='America/New_York')

    # ProcessorA opens a long position: 25 shares @ $40 (spends all $1000 of cash).
    backtesting._cash = 1000.0
    backtesting._open_positions(open_time, [trade.Action('QQQ', trade.ActionType.BUY_TO_OPEN, 1, 40.0, processor_a)])
    long_position = backtesting._get_current_position('QQQ')
    assert (long_position.qty, long_position.entry_price) == (25, 40.0)
    processor_a.ack.assert_called_once_with('QQQ')

    # ...then ProcessorB tries to open a short of 24 shares @ $50 (spends all $1200 of cash) while
    # ProcessorA's long is still held.
    backtesting._cash = 1200.0
    backtesting._open_positions(
        open_time + datetime.timedelta(minutes=25),
        [trade.Action('QQQ', trade.ActionType.SELL_TO_OPEN, 1, 50.0, processor_b)],
    )

    # The conflicting open is skipped
    position = backtesting._get_current_position('QQQ')
    assert position == long_position
    processor_b.ack.assert_not_called()


def test_report_multi_processors(mocker):
    render_report_page = mocker.spy(backtest, 'render_report_page')
    fake_processors = [FakeProcessor(trading_frequency=frequency) for frequency in FREQUENCIES]

    # All the real processors run along with the fake ones, which trade at each of the trading frequencies
    _run_backtest(pd.to_datetime('2021-03-17'), pd.to_datetime('2021-03-22'), PROCESSORS + fake_processors)

    for processor in fake_processors:
        assert processor.get_stock_universe_call_count > 0 and processor.process_data_call_count > 0
    args = _report_args(render_report_page)
    # The portfolio is compared with the market
    statistics = next(t for t in args['summary'] if t.title == 'Statistics')
    assert statistics.headers[:2] == ['', 'My Portfolio']
    assert len(statistics.headers) > 2
    assert _titles(args['charts']) == ['2021 History']
    labels = [s.label.split(' (')[0] for s in args['charts'][0].series]
    assert labels[0] == 'My Portfolio' and {'QQQ', 'SPY'} <= set(labels)
    performance = next(t for t in args['summary'] if t.title == 'Processor Performance')
    assert len(performance.rows) == len({row[0] for row in performance.rows}) > 1
