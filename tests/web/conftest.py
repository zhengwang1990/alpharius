import os

import pytest
import sqlalchemy

from alpharius.web import create_app

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
def mock_engine(mocker):
    engine = fakes.FakeDbEngine()
    mocker.patch.object(sqlalchemy, 'create_engine', return_value=engine)
    return engine


@pytest.fixture(autouse=True)
def mock_data_client(mocker, mock_default_data_client):
    # Same fake that data.get_default_data_client() returns, so call counts reflect what the web client used.
    mocker.patch('alpharius.data.FmpClient', return_value=mock_default_data_client)
    return mock_default_data_client
