import datetime
import os
import sqlite3
from typing import Dict, List, Tuple

import pandas as pd

from .base import CACHE_DIR, DATA_COLUMNS, DataClient, TimeInterval


class CacheClient(DataClient):
    """A cache layer on top of DataClient with real data access.

    It utilizes a local SQL Lite database as cache storage layer.
    """

    def __init__(self, data_client: DataClient):
        self._data_client = data_client
        self._db = init_db()
        self.cache_hit = 0

    def get_data(
        self, symbol: str, start_time: pd.Timestamp, end_time: pd.Timestamp, time_interval: TimeInterval
    ) -> pd.DataFrame:
        """Loads data with specified start and end time.

        start_time and end_time are inclusive.
        """
        db = self._db[time_interval]
        time_range = db.execute('SELECT time_range from time_range WHERE symbol = ?', (symbol,)).fetchone()
        time_range = TimeRange.from_string(time_range[0] if time_range else '')
        if time_range.include(start_time, end_time):
            columns = ','.join([c.lower() for c in DATA_COLUMNS])
            bars = db.execute(
                f'SELECT time, {columns} FROM chart WHERE date >= ? AND date <= ?',
                [str(start_time.date()), str(end_time.date())],
            ).fetchall()
            index = pd.DatetimeIndex([pd.Timestamp(bar[0]) for bar in bars])
            data = [bar[1:] for bar in bars]
            self.cache_hit += 1
            return pd.DataFrame(data, index=index, columns=DATA_COLUMNS)
        else:
            df = self._data_client.get_data(symbol, start_time, end_time, time_interval)
            values = []
            for ind, row in df.iterrows():
                values.append([symbol, str(ind.date()), str(ind)] + [row[col] for col in DATA_COLUMNS])
            columns = ','.join(DATA_COLUMNS)
            marks = ','.join(['?' for _ in DATA_COLUMNS])
            db.executemany(
                (
                    f'INSERT INTO chart (symbol, date, time, {columns})'
                    f'VALUES (?, ?, ?, {marks}) ON CONFLICT (symbol, time) DO NOTHING'
                ),
                values,
            )
            time_range.merge(start_time, end_time)
            db.execute(
                (
                    'INSERT INTO time_range (symbol, time_range) VALUES (?, ?)'
                    'ON CONFLICT (symbol) DO UPDATE SET time_range = ?'
                ),
                [symbol, time_range.to_string(), time_range.to_string()],
            )
            db.commit()
            return df

    def get_last_trades(self, symbols: List[str]) -> Dict[str, float]:
        return self._data_client.get_last_trades(symbols)


class TimeRange:
    def __init__(self, intervals: List[Tuple[datetime.date, datetime.date]]):
        self.intervals = intervals

    @classmethod
    def from_string(cls, s: str):
        s = s.strip()
        if not s:
            return cls([])
        interval_strs = s.split(';')
        intervals = []
        for interval_str in interval_strs:
            s, e = interval_str.split(',')
            interval = (pd.Timestamp(s).date(), pd.Timestamp(e).date())
            intervals.append(interval)
        return cls(intervals)

    def to_string(self):
        interval_strs = []
        for interval in self.intervals:
            interval_str = f'{interval[0]},{interval[1]}'
            interval_strs.append(interval_str)
        return ';'.join(interval_strs)

    def include(self, start_time: pd.Timestamp, end_time: pd.Timestamp):
        start_date = start_time.date()
        end_date = end_time.date()
        for interval_start, interval_end in self.intervals:
            if start_date >= interval_start and end_date <= interval_end:
                return True
        return False

    def merge(self, start_time: pd.Timestamp, end_time: pd.Timestamp):
        self.intervals.append((start_time.date(), end_time.date()))
        self.intervals.sort()
        intervals = []
        for interval in self.intervals:
            if intervals and intervals[-1][1] >= interval[0]:
                intervals[-1] = (intervals[-1][0], interval[1])
            else:
                intervals.append(interval)
        self.intervals = intervals


def get_db_file(time_interval: TimeInterval):
    db_file = os.path.join(CACHE_DIR, str(time_interval), 'data.db')
    os.makedirs(os.path.dirname(db_file), exist_ok=True)
    return db_file


def init_db() -> Dict[TimeInterval, sqlite3.Connection]:
    sql_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'cache_db.sql')
    with open(sql_file, 'r') as f:
        init_script = f.read()
    db = {}
    for time_interval in TimeInterval:
        db_file = get_db_file(time_interval)
        conn = sqlite3.connect(db_file)
        conn.executescript(init_script)
        db[time_interval] = conn
    return db
