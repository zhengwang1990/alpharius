import pandas as pd
import pytest

import alpharius.trade as trade


@pytest.mark.parametrize('view_time', ['2015-01-01', '2022-01-01'])
def test_get_nasdaq100(view_time):
    nasdaq100_symbols = trade.get_nasdaq100(pd.to_datetime(view_time))
    assert 'AAPL' in nasdaq100_symbols


@pytest.mark.parametrize('view_time', ['1999-01-01', '2022-01-01'])
def test_get_sp500(view_time):
    sp500_symbols = trade.get_sp500(pd.to_datetime(view_time))
    assert 'AAPL' in sp500_symbols
