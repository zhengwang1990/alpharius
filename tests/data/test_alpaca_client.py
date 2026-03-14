import os

import pandas as pd
import pytest
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.models import BarSet, Trade

import alpharius.data as data


@pytest.fixture(autouse=True)
def mock_get_stock_bars(mocker):
    raw_data = {
        't': int(pd.Timestamp('2024-03-26 16:00:00').timestamp()),
        'o': 123.4,
        'h': 125.4,
        'l': 122.2,
        'c': 123.5,
        'v': 12321,
        'n': 123,
        'vw': 123.9,
    }
    mocker.patch.object(StockHistoricalDataClient, 'get_stock_bars', return_value=BarSet({'AAPL': [raw_data]}))


@pytest.fixture(autouse=True)
def mock_get_stock_latest_trade(mocker):
    raw_data = {
        't': int(pd.Timestamp('2024-03-26 16:00:00').timestamp()),
        'p': 124.3,
        's': 12,
        'x': 'Exchange',
        'i': 123,
        'c': None,
        'z': None,
    }
    mocker.patch.object(
        StockHistoricalDataClient, 'get_stock_latest_trade', return_value={'AAPL': Trade('AAPL', raw_data)}
    )


@pytest.mark.parametrize('time_interval', [data.TimeInterval.FIVE_MIN, data.TimeInterval.HOUR, data.TimeInterval.DAY])
def test_get_data(time_interval):
    client = data.AlpacaClient()
    d = client.get_data(
        'AAPL', start_time=pd.Timestamp('2024-03-26'), end_time=pd.Timestamp('2024-03-27'), time_interval=time_interval
    )
    assert len(d) > 0


def test_get_last_trades():
    client = data.AlpacaClient()
    prices = client.get_last_trades(['AAPL'])
    assert len(prices) == 1
