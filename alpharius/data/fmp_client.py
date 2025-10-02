import collections
import contextlib
import os
import threading
import time
from datetime import timedelta
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import requests
import retrying

from alpharius.utils import TIME_ZONE
from .base import DATA_COLUMNS, DataClient, TimeInterval

_FMP_API_KEY_ENV = 'FMP_API_KEY'
_BASE_URL = 'https://financialmodelingprep.com/'


class FmpClient(DataClient):

    def __init__(self, api_key: Optional[str] = None) -> None:
        """Instantiates an FMP Data Client.

        Parameters:
            api_key: FMP API key.
        """
        self._api_key = api_key or os.environ[_FMP_API_KEY_ENV]
        self._call_history = collections.deque()
        self._max_calls = 700
        self._period = 60
        self._lock = threading.RLock()
        self._now = pd.Timestamp.now().tz_localize(TIME_ZONE)

    @contextlib.contextmanager
    def rate_limit(self):
        def remove_queue():
            while self._call_history and self._call_history[0] < time.time() - self._period:
                self._call_history.popleft()

        with self._lock:
            remove_queue()
            while len(self._call_history) >= self._max_calls:
                wait_time = max(self._call_history[0] + 60 - time.time(), 0)
                time.sleep(wait_time)
                remove_queue()
        yield
        with self._lock:
            self._call_history.append(time.time())

    @retrying.retry(stop_max_attempt_number=3,
                    wait_exponential_multiplier=500,
                    retry_on_exception=lambda e: isinstance(e, requests.HTTPError))
    def get_data(self,
                 symbol: str,
                 start_time: pd.Timestamp,
                 end_time: pd.Timestamp,
                 time_interval: TimeInterval) -> pd.DataFrame:
        """Loads data with specified start and end time.

        start_time and end_time are inclusive.
        """
        if not start_time.tzinfo:
            start_time = start_time.tz_localize(TIME_ZONE)
        if not end_time.tzinfo:
            end_time = end_time.tz_localize(TIME_ZONE)
        url = _BASE_URL
        if time_interval == TimeInterval.FIVE_MIN:
            if self._now - start_time > timedelta(days=60):
                url += f'api/v3/historical-chart/5min/{symbol}'
            else:
                url += 'stable/historical-chart/5min'
        elif time_interval == TimeInterval.HOUR:
            url += 'stable/historical-chart/1hour'
        elif time_interval == TimeInterval.DAY:
            url += 'stable/historical-price-eod/full'
        else:
            raise ValueError(f'time_interval {time_interval} not supported')
        start = start_time.strftime('%F')
        end = end_time.strftime('%F')
        params = {'symbol': symbol, 'from': start, 'to': end, 'apikey': self._api_key, 'extended': 'true'}
        with self.rate_limit():
            response = requests.get(url, params=params)
            response.raise_for_status()
        response_json = response.json()
        if isinstance(response_json, dict):
            raw_bars = response_json.get('historical', [])
        else:
            raw_bars = response_json
        bars = []
        for bar in raw_bars:
            t = pd.Timestamp(bar['date']).tz_localize(TIME_ZONE)
            if time_interval == time_interval.DAY or start_time <= t <= end_time:
                bars.append(bar)
        bars.sort(key=lambda b: b['date'])
        index = pd.DatetimeIndex([pd.Timestamp(b['date']).tz_localize(TIME_ZONE) for b in bars])
        data = [[np.float32(b['open']), np.float32(b['high']), np.float32(b['low']), np.float32(b['close']),
                 np.uint64(b['volume'] or 0)] for b in bars]
        return pd.DataFrame(data, index=index, columns=DATA_COLUMNS)

    @retrying.retry(stop_max_attempt_number=3,
                    wait_exponential_multiplier=500,
                    retry_on_exception=lambda e: isinstance(e, requests.HTTPError))
    def get_last_trades(self, symbols: List[str]) -> Dict[str, float]:
        """Gets the last trade prices of a list of symbols."""
        url = _BASE_URL + 'stable/batch-quote-short'
        params = {'symbols': ','.join(symbols), 'apikey': self._api_key}
        with self.rate_limit():
            response = requests.get(url, params=params)
            response.raise_for_status()
        res = {}
        for item in response.json():
            res[item['symbol']] = item['price']
        return res
