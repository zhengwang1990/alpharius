import pandas as pd
import pytest
from alpharius.trade import DataLoader, DataSource, TimeInterval


@pytest.mark.parametrize('time_interval', [TimeInterval.FIVE_MIN, TimeInterval.DAY])
@pytest.mark.parametrize("data_source", [DataSource.ALPACA, DataSource.POLYGON])
def test_load_data_list(time_interval, data_source):
    data_loader = DataLoader(time_interval, data_source)
    data = data_loader.load_data_list('QQQ',
                                      pd.to_datetime('2022-10-11'),
                                      pd.to_datetime('2022-10-12'))
    assert len(data) > 0


@pytest.mark.parametrize("data_source", [DataSource.ALPACA, DataSource.POLYGON])
def test_load_data_point(data_source):
    data_loader = DataLoader(TimeInterval.FIVE_MIN, data_source)

    data = data_loader.load_data_point('QQQ',
                                       pd.to_datetime('2022-10-12'))

    assert len(data) > 0


@pytest.mark.parametrize("data_source", [DataSource.ALPACA, DataSource.POLYGON])
def test_load_daily_data(data_source):
    data_loader = DataLoader(TimeInterval.FIVE_MIN, data_source)

    data = data_loader.load_daily_data('QQQ',
                                       pd.to_datetime('2022-10-12'))

    assert len(data) > 0


@pytest.mark.parametrize("data_source", [DataSource.ALPACA, DataSource.POLYGON])
def test_get_last_trades(data_source):
    data_loader = DataLoader(TimeInterval.FIVE_MIN, data_source)

    trades = data_loader.get_last_trades(['QQQ', 'SPY'])

    assert 'QQQ' in trades
    assert 'SPY' in trades

