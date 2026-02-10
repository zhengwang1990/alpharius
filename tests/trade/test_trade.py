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


@pytest.mark.parametrize('mode', ['backtest', 'live'])
def test_trade(mocker, mode):
    mocker.patch.object(argparse.ArgumentParser,  'parse_args',
                        return_value=FakeArgs(mode, '2025-01-01', '2025-02-01'))
    mocker.patch.object(trade, 'Backtest')
    mocker.patch.object(trade, 'Live')
    trade.main()
