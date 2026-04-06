import argparse
import datetime
import os
import sys
from typing import List, NamedTuple, Optional, Tuple

import pandas as pd
import retrying
import sqlalchemy
from tqdm import tqdm

import alpharius.data as data
from alpharius.utils import TIME_ZONE, Transaction, get_today

INSERT_TRANSACTION_QUERY = sqlalchemy.text("""
INSERT INTO transaction (
  id, symbol, is_long, processor, entry_price, exit_price,
  entry_time, exit_time, qty, gl, gl_pct, slippage, slippage_pct
)
VALUES (
  :id, :symbol, :is_long, :processor, :entry_price, :exit_price,
  :entry_time, :exit_time, :qty, :gl, :gl_pct, :slippage, :slippage_pct
)
""")

UPSERT_TRANSACTION_QUERY = sqlalchemy.text("""
INSERT INTO transaction (
  id, symbol, is_long, processor, entry_price, exit_price,
  entry_time, exit_time, qty, gl, gl_pct, slippage, slippage_pct
)
VALUES (
  :id, :symbol, :is_long, :processor, :entry_price, :exit_price,
  :entry_time, :exit_time, :qty, :gl, :gl_pct, :slippage, :slippage_pct
)
ON CONFLICT (id) DO UPDATE
SET slippage = :slippage,
    slippage_pct = :slippage_pct;
""")

SELECT_TRANSACTION_AGG_QUERY = sqlalchemy.text("""
SELECT
  processor, gl, gl_pct, slippage, slippage_pct, 
  entry_price * qty AS cash_flow
FROM transaction
WHERE
  exit_time >= :start_time AND exit_time < :end_time;
""")

SELECT_TRANSACTION_DETAIL_QUERY = """
SELECT
  symbol, is_long, processor, entry_price, exit_price,
  entry_time, exit_time, qty, gl, gl_pct, slippage, slippage_pct
FROM transaction
{condition}
ORDER BY exit_time DESC
LIMIT :limit
OFFSET :offset;
"""

COUNT_TRANSACTION_QUERY = """
SELECT COUNT(*) FROM transaction {condition};
"""

UPSERT_AGGREGATION_QUERY = sqlalchemy.text("""
INSERT INTO aggregation (
  date, processor, gl, avg_gl_pct, slippage, avg_slippage_pct, 
  count, win_count, lose_count, slippage_count, cash_flow
)
VALUES (
  :date, :processor, :gl, :avg_gl_pct, :slippage,
  :avg_slippage_pct, :count, :win_count, :lose_count,
  :slippage_count, :cash_flow
)
ON CONFLICT (date, processor) DO UPDATE
SET gl = :gl,
    avg_gl_pct = :avg_gl_pct,
    slippage = :slippage,
    avg_slippage_pct = :avg_slippage_pct,
    count = :count,
    win_count = :win_count,
    lose_count = :lose_count,
    slippage_count = :slippage_count,
    cash_flow = :cash_flow;
""")

SELECT_AGGREGATION_QUERY = sqlalchemy.text("""
SELECT   
  date, processor, gl, avg_gl_pct, slippage, avg_slippage_pct, 
  count, win_count, lose_count, slippage_count, cash_flow
FROM aggregation;
""")

UPSERT_LOG_QUERY = sqlalchemy.text("""
INSERT INTO log (
  date, logger, content
)
VALUES (
  :date, :logger, :content
)
ON CONFLICT (date, logger) DO UPDATE
SET content = :content;
""")

SELECT_LOG_DATES_QUERY = sqlalchemy.text("""
SELECT DISTINCT date FROM log;
""")

SELECT_LOG_QUERY = sqlalchemy.text("""
SELECT logger, content
FROM log
WHERE date = :date;
""")

INSERT_BACKTEST_QUERY = sqlalchemy.text("""
INSERT INTO backtest (
  id, symbol, is_long, processor, entry_price, exit_price,
  entry_time, exit_time, qty, gl_pct
)
VALUES (
  :id, :symbol, :is_long, :processor, :entry_price, :exit_price,
  :entry_time, :exit_time, :qty, :gl_pct
)
""")

SELECT_BACKTEST_QUERY = """
SELECT 
  symbol, is_long, processor, entry_price, exit_price,
  entry_time, exit_time, qty, NULL, gl_pct, NULL, NULL
FROM backtest
WHERE
  exit_time >= :start_time AND exit_time < :end_time {condition}
ORDER BY exit_time DESC;
"""


class Aggregation(NamedTuple):
    date: datetime.date
    processor: str
    gl: float
    avg_gl_pct: float
    slippage: Optional[float]
    avg_slippage_pct: Optional[float]
    count: int
    win_count: int
    lose_count: int
    slippage_count: int
    cash_flow: float


class Db:
    def __init__(self) -> None:
        sql_string = os.environ.get('SQL_STRING')
        self._eng = sqlalchemy.create_engine(sql_string, isolation_level='AUTOCOMMIT')

    def upsert_transaction(self, transaction: Transaction) -> None:
        self._insert_transaction(transaction, True)

    def insert_transaction(self, transaction: Transaction) -> None:
        self._insert_transaction(transaction, False)

    def _insert_transaction(self, transaction: Transaction, allow_conflict: bool) -> None:
        transaction_id = transaction.symbol + ' ' + transaction.exit_time.strftime('%F %H:%M')
        query = UPSERT_TRANSACTION_QUERY if allow_conflict else INSERT_TRANSACTION_QUERY
        self._execute(
            query,
            id=transaction_id,
            symbol=transaction.symbol,
            is_long=transaction.is_long,
            processor=transaction.processor if transaction.processor else None,
            entry_price=transaction.entry_price,
            exit_price=transaction.exit_price,
            entry_time=transaction.entry_time.isoformat(),
            exit_time=transaction.exit_time.isoformat(),
            qty=transaction.qty,
            gl=transaction.gl,
            gl_pct=transaction.gl_pct,
            slippage=transaction.slippage,
            slippage_pct=transaction.slippage_pct,
        )

    def insert_backtest(self, transaction: Transaction) -> None:
        transaction_id = transaction.symbol + ' ' + transaction.exit_time.strftime('%F %H:%M')
        self._execute(
            INSERT_BACKTEST_QUERY,
            id=transaction_id,
            symbol=transaction.symbol,
            is_long=transaction.is_long,
            processor=transaction.processor,
            entry_price=transaction.entry_price,
            exit_price=transaction.exit_price,
            entry_time=transaction.entry_time.isoformat(),
            exit_time=transaction.exit_time.isoformat(),
            qty=transaction.qty,
            gl_pct=transaction.gl_pct,
        )

    @retrying.retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
    def _execute(self, query, **kwargs):
        with self._eng.connect() as conn:
            return conn.execute(query, kwargs)

    def update_aggregation(self, date: str) -> None:
        start_time = f'{date} 00:00:00'
        end_time = f'{date} 23:59:59'
        results = self._execute(
            SELECT_TRANSACTION_AGG_QUERY,
            start_time=pd.to_datetime(start_time).tz_localize(TIME_ZONE),
            end_time=pd.to_datetime(end_time).tz_localize(TIME_ZONE),
        )
        processor_aggs = dict()
        for result in results:
            processor, gl, gl_pct, slippage, slippage_pct, cash_flow = result
            if not processor:
                processor = 'UNKNOWN'
            if processor not in processor_aggs:
                processor_aggs[processor] = {
                    'gl': 0,
                    'gl_pct_acc': 0,
                    'slippage': 0,
                    'slippage_pct_acc': 0,
                    'count': 0,
                    'win_count': 0,
                    'lose_count': 0,
                    'slippage_count': 0,
                    'cash_flow': 0,
                }
            agg = processor_aggs[processor]
            agg['gl'] += gl
            agg['gl_pct_acc'] += gl_pct
            agg['cash_flow'] += cash_flow
            if slippage is not None:
                agg['slippage'] += slippage
                agg['slippage_pct_acc'] += slippage_pct
                agg['slippage_count'] += 1
            agg['count'] += 1
            agg['win_count'] += int(gl >= 0)
            agg['lose_count'] += int(gl < 0)

        for processor, agg in processor_aggs.items():
            self._execute(
                UPSERT_AGGREGATION_QUERY,
                date=pd.to_datetime(date).date(),
                processor=processor,
                gl=agg['gl'],
                avg_gl_pct=agg['gl_pct_acc'] / agg['count'],
                slippage=agg['slippage'],
                avg_slippage_pct=(agg['slippage_pct_acc'] / agg['slippage_count'] if agg['slippage_count'] > 0 else 0),
                count=agg['count'],
                win_count=agg['win_count'],
                lose_count=agg['lose_count'],
                slippage_count=agg['slippage_count'],
                cash_flow=agg['cash_flow'],
            )

    def update_log(self, date: str, log_dir: str) -> None:
        if os.path.isdir(log_dir):
            files = os.listdir(log_dir)
            for file in files:
                if file.endswith('.txt'):
                    logger = ''.join([c.capitalize() for c in file[:-4].split('_')])
                    with open(os.path.join(log_dir, file), 'r') as f:
                        content = f.read()
                    if not content:
                        continue
                    self._execute(UPSERT_LOG_QUERY, date=date, logger=logger, content=content)

    def list_transactions(
        self, limit: int, offset: int, start_time=None, end_time=None, processor: Optional[str] = None
    ) -> List[Transaction]:
        conditions = []
        kwargs = {'limit': limit, 'offset': offset}
        if processor:
            conditions.append('processor = :processor')
            kwargs['processor'] = processor
        if start_time:
            conditions.append('exit_time >= :start_time')
            kwargs['start_time'] = start_time
        if end_time:
            conditions.append('exit_time < :end_time')
            kwargs['end_time'] = end_time
        condition = ''
        if conditions:
            condition = 'WHERE ' + ' AND '.join(conditions)
        query = sqlalchemy.text(SELECT_TRANSACTION_DETAIL_QUERY.format(condition=condition))
        results = self._execute(query, **kwargs)
        return [Transaction(*result) for result in results]

    def get_transaction_count(self, processor: Optional[str] = None) -> int:
        kwargs = {}
        condition = ''
        if processor:
            condition = 'WHERE processor = :processor'
            kwargs['processor'] = processor
        query = sqlalchemy.text(COUNT_TRANSACTION_QUERY.format(condition=condition))
        results = self._execute(query, **kwargs)
        return int(next(results)[0])

    def list_aggregations(self) -> List[Aggregation]:
        results = self._execute(SELECT_AGGREGATION_QUERY)
        return [Aggregation(*result) for result in results]

    def list_log_dates(self) -> List[str]:
        results = self._execute(SELECT_LOG_DATES_QUERY)
        return sorted([result[0].strftime('%Y-%m-%d') for result in results])

    def get_logs(self, date: str) -> List[Tuple[str, str]]:
        results = self._execute(SELECT_LOG_QUERY, date=date)
        return [(result[0], result[1]) for result in results]

    def get_backtest(self, start_time, end_time, processor: Optional[str] = None) -> List[Transaction]:
        condition = ''
        kwargs = {'start_time': start_time, 'end_time': end_time}
        if processor:
            condition = 'AND processor = :processor'
            kwargs['processor'] = processor
        query = sqlalchemy.text(SELECT_BACKTEST_QUERY.format(condition=condition))
        results = self._execute(query, **kwargs)
        return [Transaction(*result) for result in results]

    def backfill(self, data_client: data.DataClient, start_date: Optional[str] = None) -> None:
        """Backfills the database from start_date."""
        today = get_today()
        start_date = start_date or today.strftime('%F')
        # Backfill transaction table
        transactions = data.get_transactions(start_date, data_client)
        iterator = tqdm(transactions, ncols=80) if sys.stdout.isatty() else transactions
        for transaction in iterator:
            if transaction.gl_pct is not None:
                self.upsert_transaction(transaction)

        t = pd.to_datetime(start_date)
        while t.date() <= today.date():
            if t.isoweekday() < 6:
                # Backfill aggregation table
                self.update_aggregation(t.strftime('%F'))
            t += datetime.timedelta(days=1)


def main():
    parser = argparse.ArgumentParser(description='Alpharius database backfilling.')
    parser.add_argument('--start_date', default=None, help='Start date of the backfilling.')
    args = parser.parse_args()
    Db().backfill(data.get_default_data_client(), args.start_date)


if __name__ == '__main__':
    main()
