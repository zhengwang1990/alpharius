import os

import pandas as pd
import pytest
import sqlalchemy

from alpharius.utils import TIME_ZONE
from alpharius.web import cache, create_app

from .. import fakes


@pytest.fixture
def secret(mocker):
    mocker.patch.dict(os.environ, {'SECRET_KEY': 'test'})


@pytest.fixture
def app(secret):
    app = create_app({'TESTING': True})
    return app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture(autouse=True)
def mock_trading_hours(mocker):
    """Fixes the cache clock at a weekday noon, when nothing is cached, so tests do not depend on when they run."""
    mocker.patch('alpharius.web.cache.get_current_time', return_value=pd.Timestamp('2022-11-03 12:00', tz=TIME_ZONE))
    cache.clear_cache()


@pytest.fixture(autouse=True)
def mock_engine(mocker):
    engine = fakes.FakeDbEngine()
    mocker.patch.object(sqlalchemy, 'create_engine', return_value=engine)
    return engine


@pytest.fixture(autouse=True)
def mock_data_client(mocker, mock_default_data_client):
    mocker.patch('alpharius.data.FmpClient', return_value=mock_default_data_client)
    return mock_default_data_client
