import datetime

import alpaca.trading as trading
import pandas as pd

from alpharius.data import get_transactions

from ..fakes import FakeDataClient, get_order


def test_get_transactions(mocker, mock_trading_client):
    orders = []
    for i in range(400):
        orders.append(
            get_order(
                'SYM',
                trading.OrderSide.SELL,
                None,
                pd.to_datetime('2021-03-17T10:14:57.0Z') - datetime.timedelta(seconds=2 * i),
                '10',
            )
        )
        orders.append(
            get_order(
                'SYM',
                trading.OrderSide.BUY,
                None,
                pd.to_datetime('2021-03-17T10:14:57.0Z') - datetime.timedelta(seconds=2 * i + 1),
                '10',
            )
        )
    get_orders = mocker.patch.object(mock_trading_client, 'get_orders', side_effect=[orders[:500], orders[497:]])

    transactions = get_transactions('2021-03-17', FakeDataClient())

    assert get_orders.call_count == 2
    assert len(transactions) == 400
