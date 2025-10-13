import re

import pandas as pd

import alpharius.trade.processors as processors
from alpharius.data import TimeInterval
from alpharius.trade import Context

from ...fakes import FakeDataClient


def test_all_processors():
    pattern = re.compile(r'^[A-Z]\w+Processor$')
    data_client = FakeDataClient()
    current_time = pd.Timestamp('2025-01-15 10:35:00-04')
    interday_lookback = data_client.get_data('FAKE',
                                             start_time=pd.Timestamp('2024-01-15'),
                                             end_time=pd.Timestamp('2025-01-14'),
                                             time_interval=TimeInterval.DAY)
    intraday_lookback = data_client.get_data('FAKE',
                                             start_time=pd.Timestamp('2025-01-15 09:00:00-04'),
                                             end_time=current_time,
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
                                current_price=100.56,
                                interday_lookback=interday_lookback,
                                intraday_lookback=intraday_lookback)
                        for symbol in stock_universe]
            processor.setup([], current_time)
            transactions = processor.process_all_data(contexts)
            for transaction in transactions:
                processor.ack(transaction.symbol)
