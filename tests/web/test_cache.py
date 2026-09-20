import datetime

import pandas as pd
import pytest

from alpharius.utils import TIME_ZONE
from alpharius.web import cache


def _at(text: str) -> pd.Timestamp:
    return pd.Timestamp(text, tz=TIME_ZONE)


@pytest.mark.parametrize(
    'now, stable_from, expected',
    [
        # 2022-11-04 is a Friday
        ('2022-11-04 17:00', cache.AFTER_TRADING, '2022-11-04'),
        ('2022-11-04 17:00', cache.AFTER_HOURS, None),
        # Through the weekend until Monday 03:00 it is still Friday's data
        ('2022-11-05 12:00', cache.AFTER_HOURS, '2022-11-04'),
        ('2022-11-07 02:59', cache.AFTER_HOURS, '2022-11-04'),
        ('2022-11-07 03:00', cache.AFTER_TRADING, None),
        # Overnight on a weekday it is the previous evening's data
        ('2022-11-08 02:59', cache.AFTER_TRADING, '2022-11-07'),
    ],
)
def test_get_stable_day(now, stable_from, expected):
    expected = datetime.date.fromisoformat(expected) if expected else None
    assert cache.get_stable_day(_at(now), stable_from) == expected


def test_stable_cache(mocker):
    class Source:
        calls = 0

        @cache.stable_cache(cache.AFTER_TRADING)
        def get(self, key):
            Source.calls += 1
            return [key]

    now = mocker.patch('alpharius.web.cache.get_current_time')
    # Trading hours: always fetched
    now.return_value = _at('2022-11-04 12:00')
    Source().get('a')
    Source().get('a')
    assert Source.calls == 2
    # Weekend: fetched once per argument, whichever instance asks, and callers cannot change the cached value
    now.return_value = _at('2022-11-05 12:00')
    Source().get('a').append('changed')
    assert Source().get('a') == ['a']
    assert Source.calls == 3
    Source().get('b')
    assert Source.calls == 4
    # The next weekend is fetched again
    now.return_value = _at('2022-11-12 12:00')
    Source().get('a')
    assert Source.calls == 5
