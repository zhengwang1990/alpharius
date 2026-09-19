import datetime
import os
import re
import textwrap
import time

import flask
import pandas as pd
import pytest

from alpharius.db import Aggregation
from alpharius.utils import get_current_time, get_today
from alpharius.web import create_app, scheduler, web


@pytest.mark.parametrize('route', ['/', '/dashboard_data'])
def test_dashboard(route, client, mock_trading_client, mock_data_client):
    assert client.get(route).status_code == 200
    assert mock_trading_client.get_portfolio_history_call_count > 0
    assert mock_trading_client.get_orders_call_count > 0
    assert mock_trading_client.get_all_positions_call_count > 0
    assert mock_data_client.get_data_call_count > 0


@pytest.mark.parametrize(
    'route',
    [
        '/transactions',
        '/transactions?page=2',
        '/transactions?processor=Processor1',
        '/transactions?date=2022-11-03',
    ],
)
def test_transactions(route, client, mock_engine):
    mock_engine.conn.execute.side_effect = [
        [(pd.to_datetime('2022-11-02').date(), 'Processor1', 100, 0.01, 0, 0, 3, 2, 1, 0, 1000)],
        iter([[2]]),
        [
            (
                'SYMA',
                True,
                'Processor',
                11.1,
                12.3,
                pd.to_datetime('2022-11-03T09:35:00-04:00'),
                pd.to_datetime('2022-11-03T10:35:00-04:00'),
                10,
                100,
                0.01,
                -100,
                -0.01,
            ),
            (
                'SYMB',
                False,
                'Processor',
                11.1,
                12.3,
                pd.to_datetime('2022-11-03T09:35:00-04:00'),
                pd.to_datetime('2022-11-03T10:35:00-04:00'),
                10,
                100,
                0.01,
                None,
                None,
            ),
        ],
    ]

    assert client.get(route).status_code == 200
    assert mock_engine.conn.execute.call_count == 3


def test_transactions_date_filter(client, mock_engine):
    mock_engine.conn.execute.side_effect = [
        [(pd.to_datetime('2022-11-02').date(), 'Processor1', 100, 0.01, 0, 0, 3, 2, 1, 0, 1000)],
        iter([[45]]),
        [],
    ]

    resp = client.get('/transactions?date=2022-11-03&processor=Processor1')

    # The whole day in the market time zone, in both the count and the list query
    start_time = pd.to_datetime('2022-11-03').tz_localize('America/New_York')
    end_time = pd.to_datetime('2022-11-03 23:59:59').tz_localize('America/New_York')
    count_call, list_call = mock_engine.conn.execute.call_args_list[1:]
    assert count_call[0][1] == {'processor': 'Processor1', 'start_time': start_time, 'end_time': end_time}
    assert list_call[0][1]['start_time'] == start_time
    # Pagination keeps both filters
    assert 'href="?page=2&amp;processor=Processor1&amp;date=2022-11-03"' in resp.text


def test_parse_date():
    assert web._parse_date('2022-11-03') == datetime.date(2022, 11, 3)
    assert web._parse_date(None) is None
    assert web._parse_date('not-a-date') is None


def test_analytics(client, mock_trading_client, mock_engine, mock_data_client):
    mock_engine.conn.execute.return_value = [
        (pd.to_datetime('2022-11-02').date(), 'Processor1', 100, 0.01, 0, 0, 3, 2, 1, 0, 1000),
        (pd.to_datetime('2022-11-03').date(), 'Processor1', 100, 0.01, 10, 0.01, 2, 2, 0, 2, 1000),
        (pd.to_datetime('2022-11-03').date(), 'Processor2', 100, 0.01, -10, -0.01, 3, 2, 1, 1, 1000),
    ]

    assert client.get('/analytics').status_code == 200
    assert mock_engine.conn.execute.call_count == 1
    assert mock_trading_client.get_portfolio_history_call_count == 1
    assert mock_data_client.get_data_call_count > 0


def _agg(days_ago, processor, gl):
    day = get_today().date() - datetime.timedelta(days=days_ago)
    return Aggregation(day, processor, gl, 0.01, -1.0, -0.01, 2, 1, 1, 1, 500)


def test_get_stats_by_range():
    aggs = [
        _agg(10, 'P1', 100),
        _agg(90, 'P1', 10),  # the last day of 3M
        _agg(91, 'P1', 1),
        _agg(700, 'P1', 1000),
        _agg(700, 'P2', 5),
        _agg(10, 'UNKNOWN', 7),
    ]

    stats, cash_flows = web._get_stats(aggs)

    def row(name, processor):
        return next(stat for stat in stats[name] if stat['processor'] == processor)

    assert list(stats) == ['3M', '6M', '1Y', 'ALL']
    for name, expected in [('3M', '110.00'), ('6M', '111.00'), ('1Y', '111.00'), ('ALL', '1,111.00')]:
        assert expected in row(name, 'P1')['gl']
    # Rows with no transactions in the range are left out, counts are a column, UNKNOWN has no slippage
    assert [stat['processor'] for stat in stats['3M']] == ['P1', 'UNKNOWN', 'ALL']
    assert [stat['processor'] for stat in stats['ALL']] == ['P1', 'P2', 'UNKNOWN', 'ALL']
    assert row('3M', 'P1')['cnt'] == '4' and row('ALL', 'ALL')['cnt'] == '12'
    assert row('3M', 'UNKNOWN')['slip'] == 'N/A'
    assert cash_flows['3M'] == [{'processor': 'P1', 'cash_flow': 1000}, {'processor': 'UNKNOWN', 'cash_flow': 500}]


def test_analytics_range_tables(client, mock_engine, mock_trading_client, mock_data_client):
    mock_engine.conn.execute.return_value = [_agg(10, 'Processor1', 100), _agg(200, 'Processor1', 10)]

    resp = client.get('/analytics')

    # The profit and slippage tables each have a body per range, and only the default (3M) is shown
    bodies = re.findall(r'<tbody data-range="(\w+)"([^>]*)>', resp.text)
    assert [(name, 'hidden' in attrs) for name, attrs in bodies] == [
        ('3M', False),
        ('6M', True),
        ('1Y', True),
        ('ALL', True),
    ] * 2


def test_logs(client, mock_engine):
    fake_data = textwrap.dedent("""
    [DEBUG] [2022-11-03 10:33:00] [main.py:11] This is debug log.
    [INFO] [2022-11-03 10:34:00] [main.py:12] This is info log.
    [WARNING] [2022-11-03 10:35:00] [main.py:13] This is warning log.
    [ERROR] [2022-11-03 10:36:00] [main.py:14] This is error log.
    More error messages.
    """)
    mock_engine.conn.execute.side_effect = [
        [
            [pd.to_datetime('2022-11-03').date()],
            [pd.to_datetime('2022-11-03').date()],
            [pd.to_datetime('2022-11-03').date()],
            [pd.to_datetime('2022-11-03').date()],
        ],
        [['Trading', fake_data], ['Processor1', fake_data], ['Processor2', fake_data]],
    ]

    assert client.get('/logs').status_code == 200


def test_job_status(client):
    assert client.get('/job_status').status_code == 200


def test_get_annual_return():
    dates = [
        pd.to_datetime(t, utc=True, unit='s').strftime('%F')
        for t in range(
            int(pd.to_datetime('2017-01-01', utc=True).timestamp()),
            int(pd.to_datetime('2022-10-01', utc=True).timestamp()),
            86400,
        )
    ]
    daily_prices = {'dates': dates, 'symbols': ['My Portfolio', 'SPY', 'QQQ'], 'values': [[100] * len(dates)] * 3}

    annual_return = web._get_annual_return(daily_prices)

    assert len(annual_return['returns'][0]) == 6


def test_get_risks():
    dates = [
        pd.to_datetime(t, utc=True, unit='s').strftime('%F')
        for t in range(
            int(pd.to_datetime('2017-01-01', utc=True).timestamp()),
            int(pd.to_datetime('2022-01-01', utc=True).timestamp()) + 1,
            86400,
        )
    ]
    daily_prices = {
        'dates': dates,
        'symbols': ['My Portfolio', 'SPY', 'QQQ'],
        'values': [[100 + i * 0.01 for i in range(len(dates))]] * 3,
    }
    risks = web._get_risks(daily_prices)

    assert len(risks) == 6  # only show last 5 years + ALL


@pytest.mark.parametrize(
    'route',
    [
        '/charts',
        ('/charts?date=2022-11-18&symbol=QQQ&start_date=2022-11-13&end_date=2022-11-20'),
        ('/charts?date=2022-11-18&symbol=QQQ&start_date=2022-11-14&end_date=2022-11-18'),
    ],
)
def test_charts(route, client):
    assert client.get(route).status_code == 200


def test_charts_moves_weekend_range_to_trading_days(client):
    # Sunday to Sunday becomes Monday to Friday
    resp = client.get('/charts?symbol=QQQ&start_date=2022-11-13&end_date=2022-11-20')

    assert 'INIT_START_DATE = "2022-11-14"' in resp.text
    assert 'INIT_END_DATE = "2022-11-18"' in resp.text


@pytest.mark.parametrize(
    'route',
    [
        '/charts_data?date=2022-11-18&symbol=QQQ&timeframe=intraday',
        '/charts_data?start_date=2022-11-13&end_date=2022-11-23&symbol=QQQ&timeframe=daily',
    ],
)
def test_charts_data(route, client, mock_data_client):
    assert client.get(route).status_code == 200
    assert mock_data_client.get_data_call_count > 0


def test_backtest(client, mock_engine, mocker):
    # Today is set to 2023-01-11
    mocker.patch.object(time, 'time', return_value=1673450000)
    mock_engine.conn.execute.side_effect = [
        # Aggregation
        [(pd.to_datetime('2022-11-02').date(), 'Processor1', 100, 0.01, 0, 0, 3, 2, 1, 0, 1000)],
        # Backtest transactions
        [
            (
                'A',
                True,
                'Processor',
                11.1,
                12.3,
                pd.to_datetime('2023-01-10T09:35:00-04:00'),
                pd.to_datetime('2023-01-10T10:35:00-04:00'),
                10,
                None,
                0.01,
                None,
                None,
            ),
            (
                'C',
                True,
                'Processor',
                11.1,
                12.3,
                pd.to_datetime('2023-01-10T10:40:00-04:00'),
                pd.to_datetime('2023-01-10T11:35:00-04:00'),
                10,
                None,
                0.01,
                None,
                None,
            ),
            (
                'D',
                True,
                'Processor',
                11.1,
                12.3,
                pd.to_datetime('2023-01-10T13:40:00-04:00'),
                pd.to_datetime('2023-01-10T14:35:00-04:00'),
                10,
                None,
                0.01,
                None,
                None,
            ),
            (
                'E',
                True,
                'Processor',
                11.1,
                12.3,
                pd.to_datetime('2023-01-10T14:40:00-04:00'),
                pd.to_datetime('2023-01-10T15:35:00-04:00'),
                10,
                None,
                0.01,
                None,
                None,
            ),
            (
                'F',
                True,
                'Processor',
                11.4,
                11.3,
                pd.to_datetime('2023-01-10T15:40:00-04:00'),
                pd.to_datetime('2023-01-10T15:50:00-04:00'),
                10,
                None,
                0.01,
                None,
                None,
            ),
            (
                'G',
                True,
                'Processor',
                12.5,
                12.3,
                pd.to_datetime('2023-01-10T15:50:00-04:00'),
                pd.to_datetime('2023-01-10T15:55:00-04:00'),
                10,
                None,
                0.01,
                None,
                None,
            ),
        ],
        # Transactions
        [
            (
                'A',
                True,
                'Processor',
                11.1,
                12.3,
                pd.to_datetime('2023-01-10T09:35:00-04:00'),
                pd.to_datetime('2023-01-10T10:35:00-04:00'),
                10,
                1,
                0.01,
                -1,
                -0.01,
            ),
            (
                'B',
                True,
                'Processor',
                11.1,
                12.3,
                pd.to_datetime('2023-01-10T09:35:00-04:00'),
                pd.to_datetime('2023-01-10T10:35:00-04:00'),
                10,
                1,
                0.01,
                -1,
                -0.01,
            ),
            (
                'C',
                True,
                'Processor',
                11.1,
                12.3,
                pd.to_datetime('2023-01-10T10:40:00-04:00'),
                pd.to_datetime('2023-01-10T11:35:00-04:00'),
                10,
                None,
                0.01,
                None,
                None,
            ),
            (
                'E',
                True,
                'Processor',
                11.1,
                12.3,
                pd.to_datetime('2023-01-10T14:45:00-04:00'),
                pd.to_datetime('2023-01-10T15:45:00-04:00'),
                10,
                None,
                0.01,
                None,
                None,
            ),
        ],
    ]

    assert client.get('/backtest').status_code == 200


def test_backtest_with_finish_time(client):
    scheduler.backtest_finish_time = get_current_time()
    assert client.get('/backtest').status_code == 200


def test_backtest_last_quarter(client):
    resp = client.get('/backtest?ndays=91')

    assert re.search(r'<option value="91"\s+selected\s*>Last Quarter</option>', resp.text)


def test_handle_exception(client, mocker):
    mocker.patch('alpharius.data.get_default_data_client', side_effect=ValueError('fake test error'))
    resp = client.get('/')
    assert 'fake test error' in resp.text
    assert resp.status_code != 200


def test_static_urls_are_versioned_by_content(secret, tmp_path):
    css = tmp_path / 'a.css'
    css.write_text('body { color: red; }')

    def url():
        # A new app per call stands for a server restart, which is when files can have changed
        app = create_app({'TESTING': True})
        app.static_folder = str(tmp_path)
        with app.test_request_context():
            return flask.url_for('static', filename='a.css')

    first = url()
    assert re.fullmatch(r'/static/a\.css\?v=[0-9a-f]{10}', first)
    # Deploying rewrites every mtime, so identical content must keep its URL
    os.utime(css, (time.time() + 3600, time.time() + 3600))
    assert url() == first
    css.write_text('body { color: tan; }')
    assert url() != first
