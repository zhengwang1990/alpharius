import datetime

import pandas as pd
import pytest
from alpharius.utils import get_transactions, get_latest_day, compute_drawdown, compute_bernoulli_ci95
from tests.fakes import Order


def test_get_transactions(mocker, mock_alpaca):
    orders = []
    for i in range(400):
        orders.append(Order(
            f'id{2 * i}', 'SYM', 'sell', '14', None, '10',
            pd.to_datetime('2021-03-17T10:14:57.0Z') - datetime.timedelta(seconds=2 * i), '12',
            pd.to_datetime('2021-03-17T10:14:57.0Z') - datetime.timedelta(seconds=2 * i), 'filled'))
        orders.append(Order(
            f'id{2 * i + 1}', 'SYM', 'buy', '14', None, '10',
            pd.to_datetime('2021-03-17T10:14:57.0Z') - datetime.timedelta(seconds=2 * i + 1), '11.99',
            pd.to_datetime('2021-03-17T10:14:57.0Z') - datetime.timedelta(seconds=2 * i + 1), 'filled'))
    list_orders = mocker.patch.object(mock_alpaca, 'list_orders', side_effect=[orders[:500], orders[497:]])

    transactions = get_transactions('2021-03-17')

    assert list_orders.call_count == 2
    assert len(transactions) == 400


def test_get_latest_day_returns_previous_day(mocker):
    mocker.patch.object(pd, 'to_datetime',
                        return_value=pd.to_datetime('2022-11-13 06:00:00+0'))

    latest_day = get_latest_day()

    assert latest_day == datetime.date(2022, 11, 12)


def test_compute_drawdown():
    values = [1, 2, 3, 4, 5, 2, 2, 1, 1, 3, 6]

    d, hi, li = compute_drawdown(values)

    assert abs(d + 0.8) < 1E-7
    assert hi == 4
    assert li == 8


def test_compute_bernoulli_ci95():
    assert compute_bernoulli_ci95(0, 1) == 0
    assert compute_bernoulli_ci95(1, 1) == 0
    assert compute_bernoulli_ci95(0.5, 10) == pytest.approx(0.3099, rel=1E-3)
