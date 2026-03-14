import json
import os
import time

import pandas as pd
import pytest
import requests

import alpharius.data as data


def fake_get(url, params, *args, **kwargs):
    if 'historical-chart' in url:
        content = [
            {
                'date': '2024-03-26 16:00:00',
                'open': 145.92,
                'low': 145.72,
                'high': 146,
                'close': 145.79,
                'volume': 1492644,
            }
        ]
    elif 'historical-price-eod/full' in url:
        symbol = params['symbol']
        content = {
            'symbol': symbol,
            'historical': [
                {
                    'date': '2024-03-26',
                    'open': 173.8,
                    'high': 176.61,
                    'low': 173.18,
                    'close': 176.53,
                    'adjClose': 176.53,
                    'volume': 21712747,
                    'unadjustedVolume': 21712747,
                    'change': 2.73,
                    'changePercent': 1.57077,
                    'vwap': 175.44,
                    'label': 'October 06, 23',
                    'changeOverTime': 0.0157077,
                },
            ],
        }
    elif 'batch-quote-short' in url:
        symbols = params['symbols'].split(',')
        content = [{'symbol': symbol, 'price': 145.85, 'volume': 42822124} for symbol in symbols]
    else:
        raise ValueError('url not recognized')
    response = requests.Response()
    response._content = json.dumps(content).encode()
    response.status_code = 200
    return response


@pytest.fixture(autouse=True)
def mock_requests(mocker):
    mocker.patch.object(requests, 'get', side_effect=fake_get)


@pytest.mark.parametrize('time_interval', [data.TimeInterval.FIVE_MIN, data.TimeInterval.HOUR, data.TimeInterval.DAY])
def test_get_data(time_interval):
    client = data.FmpClient()
    d = client.get_data(
        'AAPL', start_time=pd.Timestamp('2024-03-26'), end_time=pd.Timestamp('2024-03-27'), time_interval=time_interval
    )
    assert len(d) > 0


def test_get_daily():
    client = data.FmpClient()
    d = client.get_daily('AAPL', pd.Timestamp('2024-03-26'), data.TimeInterval.FIVE_MIN)
    assert len(d) > 0


def test_get_last_trades():
    client = data.FmpClient()
    prices = client.get_last_trades(['AAPL', 'MSFT'])
    assert len(prices) == 2


def test_rate_limited(mocker):
    client = data.FmpClient()
    mock_sleep = mocker.patch.object(time, 'sleep')
    mocker.patch.object(time, 'time', side_effect=[100] * 3000 + [1000] * 2000)

    for _ in range(800):
        client.get_daily('AAPL', pd.Timestamp('2024-03-26'), data.TimeInterval.FIVE_MIN)

    assert mock_sleep.call_count > 0
