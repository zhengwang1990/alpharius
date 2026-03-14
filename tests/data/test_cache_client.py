import datetime

import pandas as pd

import alpharius.data as data
import alpharius.data.cache_client as cache_client

from ..fakes import FakeDataClient


def test_get_db_file():
    assert 'FIVE_MIN' in cache_client.get_db_file(data.TimeInterval.FIVE_MIN)


def test_time_range_merge():
    time_range = cache_client.TimeRange([])
    time_range.merge(pd.Timestamp('2024-02-01'), pd.Timestamp('2024-03-01'))
    assert time_range.intervals == [(datetime.date(2024, 2, 1), datetime.date(2024, 3, 1))]
    time_range.merge(pd.Timestamp('2024-01-15'), pd.Timestamp('2024-02-10'))
    assert time_range.intervals == [(datetime.date(2024, 1, 15), datetime.date(2024, 3, 1))]
    time_range.merge(pd.Timestamp('2024-02-25'), pd.Timestamp('2024-03-10'))
    assert time_range.intervals == [(datetime.date(2024, 1, 15), datetime.date(2024, 3, 10))]
    time_range.merge(pd.Timestamp('2024-04-01'), pd.Timestamp('2024-04-01'))
    time_range.merge(pd.Timestamp('2024-01-01'), pd.Timestamp('2024-01-05'))
    assert time_range.intervals == [
        (datetime.date(2024, 1, 1), datetime.date(2024, 1, 5)),
        (datetime.date(2024, 1, 15), datetime.date(2024, 3, 10)),
        (datetime.date(2024, 4, 1), datetime.date(2024, 4, 1)),
    ]


def test_time_range_serialize():
    t1 = cache_client.TimeRange([])
    t1.merge(pd.Timestamp('2024-02-01'), pd.Timestamp('2024-03-01'))
    t1.merge(pd.Timestamp('2024-04-01'), pd.Timestamp('2024-05-01'))
    t1.merge(pd.Timestamp('2024-06-01'), pd.Timestamp('2024-07-01'))
    s1 = t1.to_string()
    t2 = cache_client.TimeRange.from_string(s1)
    s2 = t2.to_string()
    assert t1.intervals == t2.intervals
    assert s1 == s2


def test_time_range_include():
    time_range = cache_client.TimeRange([])
    time_range.merge(pd.Timestamp('2024-02-01'), pd.Timestamp('2024-03-01'))
    assert time_range.include(pd.Timestamp('2024-02-12'), pd.Timestamp('2024-03-01'))
    assert not time_range.include(pd.Timestamp('2024-01-25'), pd.Timestamp('2024-02-10'))


def test_cache_client(mocker):
    mocker.patch.object(cache_client, 'get_db_file', return_value=':memory:')
    fake_data_client = FakeDataClient()
    client = cache_client.CacheClient(fake_data_client)
    df1 = client.get_daily('QQQ', pd.Timestamp('2024-04-18'), data.TimeInterval.FIVE_MIN)
    assert fake_data_client.get_data_call_count == 1
    assert client.cache_hit == 0
    df2 = client.get_daily('QQQ', pd.Timestamp('2024-04-18'), data.TimeInterval.FIVE_MIN)
    assert fake_data_client.get_data_call_count == 1
    assert client.cache_hit == 1
    assert df1.to_string() == df2.to_string()
