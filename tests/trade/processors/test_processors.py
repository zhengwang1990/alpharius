import re
from datetime import timedelta

import pandas as pd
import pytest

import alpharius.trade.processors as processors
from alpharius.data import TimeInterval
from alpharius.trade import Context

from ...fakes import FakeDataClient


@pytest.mark.parametrize(
    'data,current_time',
    [
        (None, pd.Timestamp('2025-01-15 10:35:00-05')),
        ([100, 101, 102, 103, 104, 105, 106, 107, 108, 109], pd.Timestamp('2025-01-15 10:30:00-05')),
        ([50, 49, 48, 47, 46, 45, 44, 43, 42, 41, 40], pd.Timestamp('2025-01-15 12:30:00-05')),
        ([70, 75, 73, 69, 49, 32, 23, 87, 233, 314, 11, 12, 56], pd.Timestamp('2025-01-15 15:00:00-05')),
        ([11], pd.Timestamp('2025-01-15 15:35:00-05')),
        ([1, 2, 3, 4, 5, 6, 7, 8], pd.Timestamp('2025-01-15 11:05:00-05')),
        ([23, 32, 32, 21, 21, 34], pd.Timestamp('2025-01-15 10:00:00-05')),
        ([20 - i * 0.1 for i in range(100)], pd.Timestamp('2025-01-15 09:50:00-05')),
    ],
)
def test_all_processors(data, current_time):
    pattern = re.compile(r'^[A-Z]\w+Processor$')
    data_client = FakeDataClient(data)
    interday_lookback = data_client.get_data('FAKE',
                                             start_time=pd.Timestamp('2024-01-15'),
                                             end_time=pd.Timestamp('2025-01-14'),
                                             time_interval=TimeInterval.DAY)
    intraday_lookback_start = data_client.get_data('FAKE',
                                                   start_time=pd.Timestamp('2025-01-15 09:00:00-05'),
                                                   end_time=current_time,
                                                   time_interval=TimeInterval.FIVE_MIN)
    end_time = current_time + timedelta(hours=1)
    intraday_lookback_end = data_client.get_data('FAKE',
                                                 start_time=pd.Timestamp('2025-01-15 09:00:00-05'),
                                                 end_time=end_time,
                                                 time_interval=TimeInterval.FIVE_MIN)
    for attr in dir(processors):
        if pattern.match(attr):
            processor_cls = getattr(processors, attr)
            processor = processors.instantiate_processor(processor_cls,
                                                         pd.Timestamp('2024-01-01'),
                                                         pd.Timestamp('2025-01-31'),
                                                         data_client,
                                                         output_dir='/tmp')
            stock_universe = processor.get_stock_universe(pd.Timestamp('2025-01-15'))

            contexts = [Context(symbol,
                                current_time,
                                current_price=data[-1] + 2.5 if data else 100.42,
                                interday_lookback=interday_lookback,
                                intraday_lookback=intraday_lookback_start)
                        for symbol in stock_universe]
            processor.setup([], current_time)
            transactions = processor.process_all_data(contexts)
            for transaction in transactions:
                processor.ack(transaction.symbol)
            # Make a fake ack so we can test close position
            if not transactions and stock_universe:
                processor.ack((stock_universe[0]))
            contexts = [Context(symbol,
                                end_time,
                                current_price=data[-1] + 10 if data else 90.42,
                                interday_lookback=interday_lookback,
                                intraday_lookback=intraday_lookback_end)
                        for symbol in stock_universe]
            processor.process_all_data(contexts)
