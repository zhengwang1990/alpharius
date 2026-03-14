import os

import pytest
import sqlalchemy

import alpharius.db as db

from .. import fakes


@pytest.fixture(autouse=True)
def mock_engine(mocker):
    os.environ['SQL_STRING'] = 'fake_path'
    engine = fakes.FakeDbEngine()
    mocker.patch.object(sqlalchemy, 'create_engine', return_value=engine)
    return engine


@pytest.fixture
def client():
    return db.Db()
