import datetime

import pandas as pd
import pytest

from alpharius.trade.common import MARKET_OPEN
from alpharius.trade.structs import Context
from alpharius.utils import TIME_ZONE


def _reference_market_open_index(index: pd.DatetimeIndex) -> int | None:
    """The straightforward definition: first bar whose time of day is not before market open."""
    for i in range(len(index)):
        if index[i].time() >= MARKET_OPEN:
            return i
    return None


def _make_context(times, tz: datetime.tzinfo | str | None = TIME_ZONE) -> Context:
    intraday = pd.DataFrame({'Open': 1.0, 'Close': 1.0, 'High': 1.1, 'Low': 0.9}, index=pd.DatetimeIndex(times, tz=tz))
    interday = pd.DataFrame({'Close': [10.0, 11.0], 'Open': [14.0, 11.0], 'High': [14.0, 11.0], 'Low': [10.0, 11.0]})
    return Context('AAPL', pd.Timestamp('2022-01-03 10:00', tz=TIME_ZONE), 1.0, interday, intraday)


CASES = {
    'empty': ([], TIME_ZONE),
    'premarket only': (['2022-01-03 04:00', '2022-01-03 09:25'], TIME_ZONE),
    'exactly at open': (['2022-01-03 09:25', '2022-01-03 09:30', '2022-01-03 09:35'], TIME_ZONE),
    'seconds after open': (['2022-01-03 09:29:59', '2022-01-03 09:30:30'], TIME_ZONE),
    'just before open': (['2022-01-03 09:29:59.999999', '2022-01-03 09:31'], TIME_ZONE),
    'starts after open': (['2022-01-03 10:00', '2022-01-03 10:05'], TIME_ZONE),
    'starts previous evening': (['2022-01-02 20:00', '2022-01-03 04:00', '2022-01-03 09:30'], TIME_ZONE),
    'multi-day, first day has open': (
        ['2022-01-03 09:25', '2022-01-03 09:30', '2022-01-04 04:00', '2022-01-04 09:30'],
        TIME_ZONE,
    ),
    'multi-day, first day ends premarket': (
        ['2022-01-03 04:00', '2022-01-03 09:00', '2022-01-04 04:00', '2022-01-04 09:30'],
        TIME_ZONE,
    ),
    'unsorted': (['2022-01-03 09:35', '2022-01-03 09:25', '2022-01-03 09:30'], TIME_ZONE),
    'duplicates at open': (['2022-01-03 09:25', '2022-01-03 09:30', '2022-01-03 09:30', '2022-01-03 09:35'], TIME_ZONE),
    'tz naive': (['2022-01-03 09:25', '2022-01-03 09:30'], None),
    'other timezone uses local time': (['2022-01-03 08:25', '2022-01-03 09:30'], 'America/Chicago'),
    'single bar before open': (['2022-01-03 09:29'], TIME_ZONE),
    'single bar at open': (['2022-01-03 09:30'], TIME_ZONE),
}


@pytest.mark.parametrize('times, tz', CASES.values(), ids=CASES.keys())
def test_market_open_index(times, tz):
    context = _make_context(times, tz)
    expected = _reference_market_open_index(context.intraday_lookback.index)
    assert context.market_open_index == expected
    assert type(context.market_open_index) is type(expected)


@pytest.mark.parametrize('start', ['2022-01-03', '2022-03-14', '2022-11-07', '2022-03-13', '2022-11-06'])
@pytest.mark.parametrize('length', [1, 60, 66, 67, 96, 97, 150, 300])
def test_market_open_index_full_day_prefixes(start, length):
    # Includes daylight saving switches, where midnight plus 9.5 hours is not 9:30 local time.
    times = pd.date_range(f'{start} 00:00', periods=300, freq='5min', tz=TIME_ZONE)[:length]
    context = _make_context(times)
    assert context.market_open_index == _reference_market_open_index(times)


def test_market_open_index_across_daylight_saving_switch_with_premarket():
    for day in ['2022-03-14', '2022-11-07']:
        times = pd.date_range(f'{day} 04:00', periods=150, freq='5min', tz=TIME_ZONE)
        assert _make_context(times).market_open_index == 66
        assert times[66].time() == datetime.time(9, 30)


def test_market_open_index_is_none_until_open_bar_arrives():
    times = pd.date_range('2022-01-03 04:00', periods=100, freq='5min', tz=TIME_ZONE)
    assert _make_context(times[:66]).market_open_index is None
    assert _make_context(times[:67]).market_open_index == 66


def test_prev_day_close():
    context = _make_context(['2022-01-03 09:30'])
    assert context.prev_day_close == 11.0


def test_atr():
    context = _make_context(['2022-01-03 09:30'])
    assert context.atr(2) == pytest.approx(2.5)
