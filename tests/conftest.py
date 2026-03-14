import os

import alpaca.trading as alpaca_trading
import alpaca_trade_api as tradeapi
import pytest

from . import fakes


@pytest.fixture(autouse=True)
def mock_environment(mocker):
    mocker.patch.dict(
        os.environ,
        {
            'FMP_API_KEY': 'fake_api_key',
            'APCA_API_KEY_ID': 'fake_id',
            'APCA_API_SECRET_KEY': 'fake_key',
            'EMAIL_USERNAME': 'fake_user',
            'EMAIL_PASSWORD': 'fake_password',
            'EMAIL_RECEIVER': 'fake_receiver',
            'CASH_RESERVE': '0',
            'SQL_STRING': 'fake_path',
        },
    )


@pytest.fixture(autouse=True)
def mock_alpaca(mocker):
    client = fakes.FakeAlpaca()
    mocker.patch.object(tradeapi, 'REST', return_value=client)
    return client


@pytest.fixture(autouse=True)
def mock_trading_client(mocker):
    client = fakes.FakeTradingClient()
    mocker.patch.object(alpaca_trading, 'TradingClient', return_value=client)
    return client


@pytest.fixture(autouse=True)
def mock_default_data_client(mocker):
    client = fakes.FakeDataClient()
    mocker.patch('alpharius.data.get_default_data_client', return_value=client)
    return client
