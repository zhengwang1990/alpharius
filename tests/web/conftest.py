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
def mock_data_client(mocker):
    client = fakes.FakeDataClient()
    mocker.patch('alpharius.data.FmpClient', return_value=client)
    return client
