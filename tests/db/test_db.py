import os

import pandas as pd
import pytest

import alpharius.data as data
import alpharius.utils as utils

from ..fakes import FakeDataClient

TEST_TRANSACTION = utils.Transaction(
    'SYMB',
    True,
    'Processor',
    10,
    12,
    pd.to_datetime('2022-11-03T16:00:00'),
    pd.to_datetime('2022-11-04T09:35:00'),
    100,
    11,
    0.2,
    -11,
    -0.01,
)


def test_insert_transaction(client, mock_engine):
    client.insert_transaction(TEST_TRANSACTION)

    mock_engine.conn.execute.assert_called_once()


def test_upsert_transaction(client, mock_engine):
    client.upsert_transaction(TEST_TRANSACTION)

    mock_engine.conn.execute.assert_called_once()


def test_update_aggregation(client, mock_engine):
    mock_engine.conn.execute.return_value = [
        ('Processor1', 1, 0.01, None, None, 100),
        ('Processor1', 2, 0.02, -1, -0.01, 200),
        ('Processor2', -3, -0.03, 1, 0.01, 300),
        (None, 2, 0.02, -1, -0.01, 500),
    ]

    client.update_aggregation('2022-11-03')

    assert mock_engine.conn.execute.call_count == 4


def test_update_log(mocker, client, mock_engine):
    mocker.patch.object(os.path, 'isdir', return_value=True)
    mocker.patch.object(os, 'listdir', return_value=['trading.txt', 'one_processor.txt', 'other'])
    mocker.patch(
        'builtins.open',
        side_effect=[mocker.mock_open(read_data='data').return_value, mocker.mock_open(read_data='').return_value],
    )

    client.update_log('2022-11-03', 'fake_dir')

    mock_engine.conn.execute.assert_called_once()


def test_backfill(client, mock_engine):
    client.backfill(FakeDataClient(), '2022-11-03')

    assert mock_engine.conn.execute.call_count > 0


@pytest.mark.parametrize(
    'filters',
    [
        {},
        {'processor': 'Processor'},
        {'processor': 'Processor', 'start_time': pd.to_datetime('2022-11-03')},
        {'start_time': pd.to_datetime('2022-11-03'), 'end_time': pd.to_datetime('2022-11-04')},
    ],
)
def test_list_transactions(filters, client, mock_engine):
    mock_engine.conn.execute.return_value = [
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
    ]

    trans = client.list_transactions(10, 0, **filters)

    mock_engine.conn.execute.assert_called_once()
    assert len(trans) == 2


@pytest.mark.parametrize('processor', [None, 'Processor'])
def test_get_transaction_count(processor, client, mock_engine):
    client.get_transaction_count(processor)

    mock_engine.conn.execute.assert_called_once()


def test_list_aggregations(client, mock_engine):
    mock_engine.conn.execute.return_value = [
        (pd.to_datetime('2022-11-03').date(), 'Processor', 100, 0.01, -10, -0.01, 3, 2, 1, 1, 100)
    ]

    aggs = client.list_aggregations()

    mock_engine.conn.execute.assert_called_once()
    assert len(aggs) == 1


def test_list_log_dates(client, mock_engine):
    mock_engine.conn.execute.return_value = [[pd.to_datetime('2022-11-03').date()]]

    dates = client.list_log_dates()

    assert dates == ['2022-11-03']


def test_get_logs(client, mock_engine):
    mock_engine.conn.execute.return_value = [['Logger', 'Content']]

    logs = client.get_logs('2022-11-03')

    assert logs == [('Logger', 'Content')]


def test_insert_backtest(client, mock_engine):
    client.insert_backtest(TEST_TRANSACTION)

    mock_engine.conn.execute.assert_called_once()


@pytest.mark.parametrize('processor', [None, 'Processor'])
def test_get_backtest(processor, client, mock_engine):
    mock_engine.conn.execute.return_value = [
        (
            'SYMA',
            True,
            'Processor',
            11.1,
            12.3,
            pd.to_datetime('2022-11-03T09:35:00-04:00'),
            pd.to_datetime('2022-11-03T10:35:00-04:00'),
            10,
            None,
            0.01,
            None,
            None,
        ),
    ]

    trans = client.get_backtest(pd.to_datetime('2022-11-03'), pd.to_datetime('2022-11-04'), processor)

    mock_engine.conn.execute.assert_called_once()
    assert len(trans) == 1
