import argparse
from dataclasses import dataclass

import pytest

import alpharius.trade.trade as trade


@dataclass
class FakeArgs:
    mode: str
    start_date: str
    end_date: str
    ack_all: bool = False
    processors: list[str] | None = None


@pytest.mark.parametrize('mode', ['backtest', 'live'])
def test_trade(mocker, mode):
    mocker.patch.object(argparse.ArgumentParser, 'parse_args', return_value=FakeArgs(mode, '2025-01-01', '2025-02-01'))
    mocker.patch.object(trade, 'Backtest')
    mocker.patch.object(trade, 'Live')
    trade.main()


def test_trade_with_selected_processors(mocker):
    mocker.patch.object(
        argparse.ArgumentParser,
        'parse_args',
        return_value=FakeArgs(
            'backtest',
            '2025-01-01',
            '2025-02-01',
            False,
            ['Abcd', 'CrossCloseProcessor'],
        ),
    )
    backtest_mock = mocker.patch.object(trade, 'Backtest')
    mocker.patch.object(trade, 'Live')

    trade.main()

    assert backtest_mock.call_args.kwargs['processors'] == [
        trade.processors.AbcdProcessor,
        trade.processors.CrossCloseProcessor,
    ]
