import argparse
import datetime
from typing import Type

import matplotlib
from dateutil.relativedelta import relativedelta

from alpharius.data import get_default_data_client
from alpharius.trade import Backtest, Live, processors
from alpharius.utils import get_latest_day

# Interactive plot is not disabled when trading or backtesting is invoked.
matplotlib.use('agg')

PROCESSORS: list[Type[processors.Processor] | processors.Processor] = [
    processors.AbcdProcessor,
    processors.CrossCloseProcessor,
    processors.DownFourProcessor,
    processors.H2lFiveMinProcessor,
    processors.H2lHourProcessor,
    processors.L2hProcessor,
    processors.O2lProcessor,
    processors.OpenHighProcessor,
    processors.TqqqProcessor,
    processors.OvernightTqqqProcessor,
]


def _normalize_processor_name(name: str) -> str:
    normalized = name.lower().replace('_', '').replace('-', '')
    if normalized.endswith('processor'):
        normalized = normalized[: -len('processor')]
    return normalized


def _filter_processors(
    processors_list: list[Type[processors.Processor] | processors.Processor], processor_names: list[str]
) -> list[Type[processors.Processor] | processors.Processor]:
    allowed_names = {_normalize_processor_name(name) for name in processor_names}
    filtered = []
    for processor in processors_list:
        class_name = processor.__name__ if isinstance(processor, type) else type(processor).__name__
        candidate_names = {_normalize_processor_name(class_name)}
        if not isinstance(processor, type):
            candidate_names.add(_normalize_processor_name(processor.name))
        if candidate_names & allowed_names:
            filtered.append(processor)
    return filtered


def main():
    parser = argparse.ArgumentParser(description='Alpharius stock trading.')

    parser.add_argument(
        '-m', '--mode', help='Running mode. Can be backtest or trade.', required=True, choices=['backtest', 'live']
    )
    parser.add_argument('--start_date', default=None, help='Start date of the backtesting. Only used in backtest mode.')
    parser.add_argument('--end_date', default=None, help='End date of the backtesting. Only used in backtest mode.')
    parser.add_argument('--ack_all', action='store_true', help='Ack all trade actions. Only used in backtest mode.')
    parser.add_argument(
        '--processors',
        nargs='+',
        default=None,
        help='List of processor names to use. Accepts either class names or short names, e.g. Abcd or AbcdProcessor.',
    )
    args = parser.parse_args()
    data_client = get_default_data_client()

    if args.mode == 'backtest':
        latest_day = get_latest_day()
        default_start_date = (latest_day - relativedelta(years=1)).strftime('%F')
        default_end_date = (latest_day + datetime.timedelta(days=1)).strftime('%F')
        start_date = args.start_date or default_start_date
        end_date = args.end_date or default_end_date
        selected_processors = PROCESSORS
        if args.processors:
            selected_processors = _filter_processors(PROCESSORS, args.processors)
            if not selected_processors:
                parser.error(f'No processors matched: {args.processors}')

        runner = Backtest(
            start_date=start_date,
            end_date=end_date,
            processors=selected_processors,
            data_client=data_client,
            ack_all=args.ack_all,
        )
        runner.run()
    else:
        selected_processors = PROCESSORS
        if args.processors:
            selected_processors = _filter_processors(PROCESSORS, args.processors)
            if not selected_processors:
                parser.error(f'No processors matched: {args.processors}')
        runner = Live(processors=selected_processors, data_client=data_client)
        runner.run()


if __name__ == '__main__':
    main()
