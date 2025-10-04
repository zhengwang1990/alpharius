import functools
import git
import pandas as pd
import pytest

from alpharius import trade
from alpharius.trade import PROCESSORS
from ..fakes import FakeProcessor, FakeDataClient


@pytest.fixture(autouse=True)
def mock_git(mocker):
    mock_repo = mocker.MagicMock()
    mock_diff = mocker.MagicMock()
    mock_stream = mocker.MagicMock()
    mock_repo.head.commit.diff.return_value = [mock_diff]
    mock_diff.change_type = 'M'
    mock_diff.a_blob.datastream.read.return_value = mock_stream
    mock_stream.decode.return_value = 'line1\nline2'
    mocker.patch.object(git, 'Repo', return_value=mock_repo)


@pytest.mark.parametrize('trading_frequency',
                         [trade.TradingFrequency.FIVE_MIN,
                          trade.TradingFrequency.CLOSE_TO_CLOSE,
                          trade.TradingFrequency.CLOSE_TO_OPEN])
def test_run_success(trading_frequency):
    fake_processor = FakeProcessor(trading_frequency=trading_frequency)
    backtesting = trade.Backtest(start_date=pd.to_datetime('2021-03-17'),
                                 end_date=pd.to_datetime('2021-03-24'),
                                 processors=[fake_processor],
                                 data_client=FakeDataClient())

    backtesting.run()

    assert fake_processor.get_stock_universe_call_count > 0
    assert fake_processor.process_data_call_count > 0


def test_run_with_processors():
    backtesting = trade.Backtest(start_date=pd.to_datetime('2021-03-17'),
                                 end_date=pd.to_datetime('2021-03-18'),
                                 processors=PROCESSORS,
                                 data_client=FakeDataClient())

    backtesting.run()
