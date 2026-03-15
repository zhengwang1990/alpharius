import pandas as pd

from alpharius.trade.stock_universe import CachedStockUniverse


class FakeStockUniverse(CachedStockUniverse):
    def __init__(
        self, a: int, b: float, c: list[str], lookback_start_date: pd.Timestamp, lookback_end_date: pd.Timestamp
    ):
        super().__init__(lookback_start_date, lookback_end_date)
        self.a = a
        self.b = b
        self.c = c


def test_cached_stock_universe():
    test_universe1 = FakeStockUniverse(
        a=1,
        b=2.0,
        c=['test'],
        lookback_start_date=pd.Timestamp('2020-01-01'),
        lookback_end_date=pd.Timestamp('2020-12-31'),
    )
    test_universe2 = FakeStockUniverse(1, 2.0, ['test'], pd.Timestamp('2020-01-01'), pd.Timestamp('2020-12-31'))
    test_universe3 = FakeStockUniverse(1, 3.0, ['test'], pd.Timestamp('2020-01-01'), pd.Timestamp('2020-12-31'))
    test_universe4 = FakeStockUniverse(
        {'s'}, {'k': ('v',)}, ['test4'], pd.Timestamp('2020-01-01'), pd.Timestamp('2020-12-31')
    )
    test_universe5 = FakeStockUniverse(1, 2.0, ['test'], pd.Timestamp('2019-01-01'), pd.Timestamp('2020-12-31'))

    assert test_universe1.get_cache_dir() == test_universe2.get_cache_dir()
    assert test_universe1.get_cache_dir() != test_universe3.get_cache_dir()
    assert test_universe1.get_cache_dir() != test_universe4.get_cache_dir()
    assert test_universe1.get_cache_dir() != test_universe5.get_cache_dir()
