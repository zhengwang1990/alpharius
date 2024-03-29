import datetime
import functools
import os
import threading
import traceback
from concurrent import futures

import flask
from alpharius.db import Db
from alpharius.trade import PROCESSOR_FACTORIES, Backtesting, Trading
from alpharius.notification.email_sender import EmailSender
from alpharius.utils import get_latest_day
from flask_apscheduler import APScheduler
from .alpaca_client import AlpacaClient

app = flask.Flask(__name__)
bp = flask.Blueprint('scheduler', __name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()
lock = threading.RLock()
job_status = 'idle'
DISABLE_SCHEDULING = True


def email_on_exception(func):
    """Decorator that sends exceptions via email."""

    @functools.wraps(func)
    def wrap(*args, **kwargs):
        try:
            func(*args, **kwargs)
        except Exception as e:
            error_message = str(e) + '\n' + ''.join(traceback.format_tb(e.__traceback__))
            EmailSender().send_alert(error_message)

    return wrap


@email_on_exception
def _trade_run():
    Trading(processor_factories=PROCESSOR_FACTORIES).run()


def _trade_impl():
    global job_status, lock
    acquired = lock.acquire(blocking=False)
    if acquired:
        job_status = 'running'
        app.logger.info('Start trading')
        # Live trading consumes a significant amount of memory. If the execution runs
        # in the same process, the allocated memory is not returned to the operating
        # system by the garbage collector after running, but owned and managed by Python.
        # Therefore, here it spawns a child process to run trading. The memory will
        # be returned to the OS after child process is shutdown.
        with futures.ProcessPoolExecutor(max_workers=1) as pool:
            pool.submit(_trade_run).result()
        job_status = 'idle'
        app.logger.info('Finish trading')
        lock.release()
    else:
        app.logger.warning('Cannot acquire trade lock')


@email_on_exception
def _backtest_run():
    latest_day = get_latest_day()
    calendar = AlpacaClient().get_calendar()
    if len(calendar) < 2 or calendar[-1].date.strftime('%F') != latest_day.strftime('%F'):
        return
    start_date = calendar[-2].date.strftime('%F')
    end_date = (latest_day + datetime.timedelta(days=1)).strftime('%F')
    transactions = Backtesting(start_date=start_date,
                               end_date=end_date,
                               processor_factories=PROCESSOR_FACTORIES).run()
    db_client = Db()
    for transaction in transactions:
        if transaction.exit_time.date() == latest_day:
            db_client.insert_backtest(transaction)


def get_job_status():
    global job_status
    return job_status


@scheduler.task('cron', id='trade', day_of_week='mon-fri',
                hour='9-15', minute='*/15', timezone='America/New_York')
def trade():
    if DISABLE_SCHEDULING:
        return
    if job_status != 'running':
        t = threading.Thread(target=_trade_impl)
        t.start()


@scheduler.task('cron', id='backfill', day_of_week='mon-fri',
                hour='16,17,22', minute=10, timezone='America/New_York')
@email_on_exception
def backfill():
    if DISABLE_SCHEDULING:
        return
    app.logger.info('Start backfilling')
    Db().backfill()
    app.logger.info('Finish backfilling')


@scheduler.task('cron', id='backtest', day_of_week='mon-fri',
                hour=16, minute=15, timezone='America/New_York')
def backtest():
    if DISABLE_SCHEDULING:
        return
    app.logger.info('Start backtesting')
    with futures.ProcessPoolExecutor(max_workers=1) as pool:
        pool.submit(_backtest_run).result()
    app.logger.info('Finish backtesting')


@bp.route('/trigger', methods=['POST'])
def trigger():
    app.logger.info('Trade triggered manually')
    trade()
    return ''
