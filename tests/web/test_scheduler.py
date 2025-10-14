import email.mime.image as image
import email.mime.multipart as multipart
import os
import smtplib
import time
import threading
from concurrent import futures

import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import pandas as pd
import pytest
from alpharius.web import scheduler


@pytest.fixture(autouse=True)
def mock_matplotlib(mocker):
    mocker.patch.object(plt, 'savefig')
    mocker.patch.object(plt, 'tight_layout')
    mocker.patch.object(fm.FontManager, 'addfont')
    mocker.patch.object(fm.FontManager, 'findfont')


@pytest.fixture(autouse=True)
def mock_smtp(mocker):
    return mocker.patch.object(smtplib, 'SMTP', autospec=True)


@pytest.fixture(autouse=True)
def mock_os(mocker):
    mocker.patch('builtins.open', mocker.mock_open(read_data='data'))
    mocker.patch.object(os.path, 'isfile', return_value=False)
    mocker.patch.object(os, 'makedirs')


def test_trigger(client, mocker):
    thread = mocker.patch.object(threading, 'Thread')

    assert client.post('/trigger').status_code == 200
    thread.assert_called_once()


def test_trade(client, mocker):
    thread = mocker.patch.object(threading, 'Thread')

    scheduler.trade()

    thread.assert_called_once()


def test_trade_impl(mocker):
    mock_submit = mocker.Mock()
    mock_pool = mocker.patch.object(futures, 'ProcessPoolExecutor')
    mock_pool.return_value.__enter__.return_value.submit = mock_submit

    scheduler._trade_impl()

    mock_submit.assert_called_once()


def test_backfill(mock_engine):
    scheduler.backfill()

    assert mock_engine.conn.execute.call_count > 0


@pytest.mark.parametrize('job_name',
                         ['trade', 'backfill', 'backtest'])
def test_scheduler(job_name):
    job = scheduler.scheduler.get_job(job_name)
    assert job.next_run_time.timestamp() < time.time() + 86400 * 3


def test_backtest(mocker, mock_trading_client):
    mocker.patch.object(pd.DataFrame, 'to_pickle')
    # Today is set to 2023-08-31
    mocker.patch.object(time, 'time', return_value=1693450000)

    scheduler.backtest()

    assert mock_trading_client.get_calendar_call_count > 0


@pytest.mark.parametrize('method_name',
                         ['backtest', '_trade_run', 'backfill', 'log_scan'])
def test_email_send(mocker, method_name, mock_smtp, mock_alpaca, mock_trading_client, mock_engine):
    mocker.patch.object(image, 'MIMEImage', autospec=True)
    mocker.patch.object(multipart.MIMEMultipart, 'as_string', return_value='')
    mocker.patch.object(mock_alpaca, 'get_calendar', side_effect=Exception())
    mocker.patch.object(mock_trading_client, 'get_calendar', side_effect=Exception())
    mocker.patch.object(mock_engine.conn, 'execute', side_effect=Exception())
    mocker.patch.object(time, 'sleep')

    getattr(scheduler, method_name)()

    mock_smtp.assert_called_once()


def test_log_scan(mocker, mock_smtp, mock_engine):
    mocker.patch.object(mock_engine.conn, 'execute',
                        return_value=[('Trading', '[ERROR] [2025-10-10] Fake error')])
    scheduler.log_scan()
    mock_smtp.assert_called_once()
