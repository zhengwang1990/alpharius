import collections
import datetime
import functools
import json
import math
import os
import re
from concurrent import futures
from typing import List, Tuple

import flask
import numpy as np
import pandas as pd

from alpharius.db import Aggregation, Db
from alpharius.utils import (
    construct_charts_link, compute_drawdown, compute_risks, get_today,
    get_signed_percentage, get_colored_value, get_current_time, TIME_ZONE,
    compute_bernoulli_ci95, Transaction
)
from .client import Client
from .scheduler import get_job_status, get_backtest_finish_time

bp = flask.Blueprint('web', __name__)

# First time backtest is introduced
FIRST_BACKTEST_DATE = '2023-01-06'
# The time backtest cron job should finish.
DEFAULT_BACKTEST_FINISH_TIME = datetime.time(16, 30)
ACCESS_KEY = 'access'
ACCESS_VAL = os.environ.get('ACCESS_CODE')


def access_control(f):
    @functools.wraps(f)
    def wrapper():
        if flask.request.cookies.get(ACCESS_KEY) == ACCESS_VAL:
            return f()
        elif flask.request.args.get(ACCESS_KEY) == ACCESS_VAL:
            resp = flask.make_response(flask.redirect(flask.request.path))
            resp.set_cookie(ACCESS_KEY, ACCESS_VAL, max_age=datetime.timedelta(days=356))
            return resp
        else:
            return flask.render_template('access.html')

    return wrapper if ACCESS_VAL else f


def _get_dashboard_data():
    client = Client()
    tasks = dict()
    with futures.ThreadPoolExecutor(max_workers=4) as pool:
        tasks['histories'] = pool.submit(client.get_portfolio_histories)
        tasks['orders'] = pool.submit(client.get_recent_orders)
        tasks['positions'] = pool.submit(client.get_current_positions)
        tasks['watch'] = pool.submit(client.get_market_watch)
        response = {'histories': tasks['histories'].result(),
                    'orders': tasks['orders'].result(),
                    'positions': tasks['positions'].result(),
                    'watch': tasks['watch'].result()}
    return response


@bp.route('/')
@access_control
def dashboard():
    data = _get_dashboard_data()
    return flask.render_template('dashboard.html',
                                 histories=json.dumps(data['histories']),
                                 orders=data['orders'],
                                 positions=data['positions'],
                                 watch=data['watch'])


@bp.route('/dashboard_data')
def dashboard_data():
    data = _get_dashboard_data()
    return json.dumps(data)


def _list_processors(db_client: Db) -> List[str]:
    aggs = db_client.list_aggregations()
    processors = sorted(list(set([agg.processor for agg in aggs
                                  if agg.processor != 'UNKNOWN'])))
    return processors


@bp.route('/transactions')
@access_control
def transactions():
    items_per_page = 20
    page = flask.request.args.get('page')
    if page and page.isdigit():
        page = int(page)
    else:
        page = 1
    client = Db()
    processors = _list_processors(client)
    active_processor = flask.request.args.get('processor')
    if active_processor not in processors:
        active_processor = None
    processors = ['ALL PROCESSORS'] + processors
    count = client.get_transaction_count(active_processor)
    total_page = max(int(np.ceil(count / items_per_page)), 1)
    page = max(min(page, total_page), 1)
    offset = (page - 1) * items_per_page
    trans = []
    time_fmt = f'<span class="lg-hidden">%Y-%m-%d </span>%H:%M'
    for t in client.list_transactions(limit=items_per_page, offset=offset, processor=active_processor):
        trans.append({
            'symbol': t.symbol,
            'side': 'long' if t.is_long else 'short',
            'processor': t.processor if t.processor is not None else '',
            'entry_price': f'{t.entry_price:.4g}',
            'exit_price': f'{t.exit_price:.4g}',
            'entry_time': pd.to_datetime(t.entry_time).tz_convert(TIME_ZONE).strftime(time_fmt),
            'exit_time': pd.to_datetime(t.exit_time).tz_convert(TIME_ZONE).strftime(time_fmt),
            'gl': get_colored_value(f'{t.gl:+,.2f} ({t.gl_pct * 100:+.2f}%)',
                                    'green' if t.gl >= 0 else 'red'),
            'gl_pct': get_signed_percentage(t.gl_pct),
            'slippage': get_colored_value(f'{t.slippage:+,.2f} ({t.slippage_pct * 100:+.2f}%)',
                                          'green' if t.slippage >= 0 else 'red') if t.slippage is not None else '',
            'slippage_pct': get_signed_percentage(t.slippage_pct) if t.slippage_pct is not None else '',
            'link': construct_charts_link(
                t.symbol,
                pd.to_datetime(t.exit_time).tz_convert(TIME_ZONE).strftime('%F')),
        })
    return flask.render_template('transactions.html',
                                 transactions=trans,
                                 current_page=page,
                                 total_page=total_page,
                                 active_processor=active_processor,
                                 processors=processors)


def _shift_to_last(arr, target_value):
    for i in range(len(arr)):
        if arr[i] == target_value:
            for j in range(i + 1, len(arr)):
                arr[j], arr[j - 1] = arr[j - 1], arr[j]
            break


def _get_stats(aggs: List[Aggregation]):
    stats = dict()
    transaction_cnt = []
    cash_flows = []
    three_month_ago = (get_today() - datetime.timedelta(days=90)).date()
    for agg in aggs:
        processor = agg.processor
        if processor not in stats:
            stats[processor] = {'gl': 0, 'gl_pct_acc': 0, 'cnt': 0, 'win_cnt': 0,
                                'slip': 0, 'slip_pct_acc': 0, 'slip_cnt': 0, 'cash_flow': 0,
                                'gl_3m': 0, 'win_cnt_3m': 0, 'cnt_3m': 0, 'slip_3m': 0,
                                'slip_pct_acc_3m': 0, 'slip_cnt_3m': 0}
        stats[processor]['gl'] += agg.gl
        stats[processor]['cnt'] += agg.count
        stats[processor]['win_cnt'] += agg.win_count
        stats[processor]['slip'] += agg.slippage
        stats[processor]['cash_flow'] += agg.cash_flow
        if agg.date >= three_month_ago:
            stats[processor]['gl_3m'] += agg.gl
            stats[processor]['cnt_3m'] += agg.count
            stats[processor]['win_cnt_3m'] += agg.win_count
            stats[processor]['slip_3m'] += agg.slippage
        if agg.slippage_count > 0:
            stats[processor]['slip_pct_acc'] += agg.avg_slippage_pct * agg.slippage_count
            stats[processor]['slip_cnt'] += agg.slippage_count
            if agg.date >= three_month_ago:
                stats[processor]['slip_pct_acc_3m'] += agg.avg_slippage_pct * agg.slippage_count
                stats[processor]['slip_cnt_3m'] += agg.slippage_count

    total_stats = dict()
    for processor, stat in stats.items():
        transaction_cnt.append({'processor': processor, 'cnt': stat['cnt']})
        cash_flows.append({'processor': processor, 'cash_flow': int(stat['cash_flow'])})
        for k, v in stat.items():
            if processor == 'UNKNOWN' and k in ['slip', 'slip_pct_acc', 'slip_cnt']:
                continue
            if k not in total_stats:
                total_stats[k] = 0
            total_stats[k] += v
    stats['ALL'] = total_stats
    transaction_cnt.sort(key=lambda entry: entry['cnt'], reverse=True)
    cash_flows.sort(key=lambda entry: entry['cash_flow'], reverse=True)

    for processor, stat in stats.items():
        stat['processor'] = processor
        stat['avg_slip_pct'] = (get_signed_percentage(stat['slip_pct_acc'] / stat['slip_cnt'])
                                if stat.get('slip_cnt', 0) > 0 and processor != 'UNKNOWN' else 'N/A')
        stat['avg_slip_pct_3m'] = (get_signed_percentage(stat['slip_pct_acc_3m'] / stat['slip_cnt_3m'])
                                   if stat.get('slip_cnt_3m', 0) > 0 and processor != 'UNKNOWN' else 'N/A')
        win_rate = stat['win_cnt'] / stat['cnt'] if stat.get('cnt', 0) > 0 else None
        win_rate_ci = compute_bernoulli_ci95(win_rate, stat['cnt']) if win_rate else None
        stat['win_rate'] = f'{win_rate * 100:.2f}%' if win_rate is not None else 'N/A'
        stat['win_rate_ci'] = f'&plusmn; {win_rate_ci * 100:.2f}%' if win_rate_ci else ''
        win_rate_3m = stat['win_cnt_3m'] / stat['cnt_3m'] if stat.get('cnt_3m', 0) > 0 else None
        win_rate_ci_3m = compute_bernoulli_ci95(win_rate_3m, stat['cnt_3m']) if win_rate_3m else None
        stat['win_rate_3m'] = f'{win_rate_3m * 100:.2f}%' if win_rate_3m is not None else 'N/A'
        stat['win_rate_ci_3m'] = f'&plusmn; {win_rate_ci_3m * 100: .2f}%' if win_rate_ci_3m else ''
        for k in ['gl', 'slip', 'gl_3m', 'slip_3m']:
            v = stat.get(k, 0)
            color = 'green' if v >= 0 else 'red'
            if k == 'slip' and processor == 'UNKNOWN':
                stat[k] = 'N/A'
            else:
                stat[k] = get_colored_value(f'{v:,.2f}', color)

    # Order stats alphabetically with 'UNKNOWN' and 'ALL' appearing at last
    processors = sorted(stats.keys())
    _shift_to_last(processors, 'UNKNOWN')
    _shift_to_last(processors, 'ALL')
    return [stats[processor] for processor in processors], transaction_cnt, cash_flows


def _get_gl_bars(aggs: List[Aggregation]):
    dated_values = {'Daily': collections.defaultdict(int),
                    'Monthly': collections.defaultdict(int)}
    processors = set()
    processors_aggs = collections.defaultdict(list)
    for agg in aggs:
        dated_values['Daily'][agg.date.strftime('%F')] += agg.gl
        dated_values['Monthly'][agg.date.strftime('%Y-%m')] += agg.gl
        processors.add(agg.processor)
        processors_aggs[agg.processor].append(agg)
    num_cuts = {'Daily': 60, 'Monthly': 48}
    labels = dict()
    values = dict()
    all_processors = 'ALL PROCESSORS'
    for timeframe in ['Daily', 'Monthly']:
        labels[timeframe] = sorted(dated_values[timeframe].keys())[-num_cuts[timeframe]:]
        all_gl = [dated_values[timeframe][label] for label in labels[timeframe]]
        values[timeframe] = {all_processors: all_gl}
        for processor in processors:
            processor_values = collections.defaultdict(int)
            for agg in processors_aggs[processor]:
                processor_values[agg.date.strftime('%F')] = agg.gl
                processor_values[agg.date.strftime('%Y-%m')] += agg.gl
            processor_gl = [processor_values.get(label, 0) for label in labels[timeframe]]
            values[timeframe][processor] = processor_gl
    processors = [all_processors] + sorted(processors)
    _shift_to_last(processors, 'UNKNOWN')
    gl_bars = {'labels': labels, 'values': values}
    return gl_bars, processors


def _get_annual_return(daily_price):
    dates = daily_price['dates']
    res = {
        'symbols': daily_price['symbols'],
        'years': [],
        'returns': [[] for _ in daily_price['symbols']],
    }
    if len(dates) < 2:
        return res
    years = []
    values = daily_price['values']
    spots = [[value[0]] for value in values]
    for i in range(len(dates) - 1):
        if dates[i][:4] != dates[i + 1][:4]:
            years.append(dates[i][:4])
            for j in range(len(values)):
                spots[j].append(values[j][i])
    years.append(dates[-1][:4])
    for j in range(len(values)):
        spots[j].append(values[j][-1])
    res['years'] = years
    for j in range(len(spots)):
        res['returns'][j] = [(spots[j][k + 1] / spots[j][k] - 1) * 100
                             for k in range(len(spots[j]) - 1)]
    return res


def _get_risks(daily_prices):
    def get_factors(v, mv):
        a, b, s = compute_risks(v, mv)
        d, _, _ = compute_drawdown(v)
        r = v[-1] / v[0] - 1
        return {'alpha': get_signed_percentage(a) if not math.isnan(a) else 'N/A',
                'beta': f'{b:.2f}' if not math.isnan(b) else 'N/A',
                'sharpe': f'{s:.2f}' if not math.isnan(s) else 'N/A',
                'drawdown': get_signed_percentage(d),
                'return': get_signed_percentage(r)}

    dates = daily_prices['dates']
    values = daily_prices['values'][daily_prices['symbols'].index('My Portfolio')]
    market_values = daily_prices['values'][daily_prices['symbols'].index('SPY')]
    current_start = 0
    res = []
    for i in range(len(dates)):
        if i != len(dates) - 1 and dates[i][:4] == dates[i + 1][:4]:
            continue
        current_values = values[current_start:i + 1]
        current_market_values = market_values[current_start:i + 1]
        factors = get_factors(current_values, current_market_values)
        factors['year'] = dates[i][:4]
        res.append(factors)
        current_start = i
    overall_factors = get_factors(values, market_values)
    overall_factors['year'] = 'ALL'
    annualized_return = (values[-1] / values[0]) ** (252 / len(values)) - 1
    overall_factors['return'] = get_signed_percentage(annualized_return)
    res.append(overall_factors)
    return res[-6:]  # only show risk factors for last 5 years


@bp.route('/analytics')
@access_control
def analytics():
    client = Client()
    with futures.ThreadPoolExecutor(max_workers=1) as pool:
        get_daily_price_task = pool.submit(client.get_daily_prices)
    db_client = Db()
    aggs = db_client.list_aggregations()
    stats, transaction_cnt, cash_flows = _get_stats(aggs)
    gl_bars, processors = _get_gl_bars(aggs)
    daily_price = get_daily_price_task.result()
    annual_return = _get_annual_return(daily_price)
    risks = _get_risks(daily_price)
    return flask.render_template('analytics.html',
                                 stats=stats,
                                 transaction_cnt=transaction_cnt,
                                 cash_flows=cash_flows,
                                 gl_bars=gl_bars,
                                 annual_return=annual_return,
                                 risks=risks,
                                 processors=processors)


def _parse_log_content(content: str, date: str):
    def is_entry_start(lin: str):
        ll = lin.lower()
        return (ll.startswith('[info] [') or ll.startswith('[warning] [')
                or ll.startswith('[debug] [') or ll.startswith('[error] ['))

    log_lines = content.split('\n')
    log_entries = []
    i = 0
    link = construct_charts_link(r'\1', date)
    while i < len(log_lines):
        line = log_lines[i]
        if is_entry_start(line):
            span_start, span_end = 0, 0
            spans = []
            for _ in range(3):
                span_start = line.find('[', span_end)
                span_end = line.find(']', span_start)
                spans.append(line[span_start + 1:span_end])
            message = line[span_end + 1:]
            message = re.sub(r'\[([A-Z]+)\]', f'[<a href={link}>\\1</a>]', message)
            log_type = spans[0].lower()
            log_entry = {'type': log_type,
                         'type_initial': log_type[0],
                         'time': pd.to_datetime(spans[1]).strftime('%H:%M:%S'),
                         'time_short': pd.to_datetime(spans[1]).strftime('%H:%M'),
                         'code': spans[2],
                         'message': message}
            i += 1
            while i < len(log_lines) and not is_entry_start(log_lines[i]):
                log_entry['message'] += '\n' + log_lines[i]
                i += 1
            log_entry['message'] = log_entry['message'].lstrip()
            log_entries.append(log_entry)
        else:
            i += 1
    return log_entries


@bp.route('/logs')
@access_control
def logs():
    client = Db()
    dates = client.list_log_dates()
    date = flask.request.args.get('date')
    if (not date or date not in dates) and dates:
        date = dates[-1]
    results = client.get_logs(date) if date else []
    loggers = []
    log_entries = dict()
    for logger, content in results:
        loggers.append(logger)
        log_entries[logger] = _parse_log_content(content, date)
    loggers.sort()
    for i in range(len(loggers)):
        if loggers[i] == 'Trading':
            for j in range(i - 1, -1, -1):
                loggers[j + 1] = loggers[j]
            loggers[0] = 'Trading'

    return flask.render_template('logs.html',
                                 loggers=loggers,
                                 log_entries=log_entries,
                                 date=date,
                                 dates=dates)


@bp.route('/charts')
@access_control
def charts():
    client = Client()
    date = flask.request.args.get('date')
    start_date = flask.request.args.get('start_date')
    end_date = flask.request.args.get('end_date')
    if start_date and end_date:
        pd_start = pd.to_datetime(start_date)
        if pd_start.isoweekday() > 5:
            pd_start += datetime.timedelta(days=8 - pd_start.isoweekday())
        start_date = pd_start.strftime('%F')
        pd_end = pd.to_datetime(end_date)
        if pd_end.isoweekday() > 5:
            pd_end -= datetime.timedelta(days=pd_end.isoweekday() - 5)
        end_date = pd_end.strftime('%F')
    symbol = flask.request.args.get('symbol')
    all_symbols = client.get_all_symbols()
    return flask.render_template('charts.html',
                                 all_symbols=all_symbols,
                                 init_date=date,
                                 init_start_date=start_date,
                                 init_end_date=end_date,
                                 init_symbol=symbol)


@bp.route('/charts_data')
def charts_data():
    client = Client()
    timeframe = flask.request.args.get('timeframe')
    if timeframe == 'intraday':
        start_date = flask.request.args.get('date')
        end_date = start_date
    else:
        start_date = flask.request.args.get('start_date')
        end_date = flask.request.args.get('end_date')
    symbol = flask.request.args.get('symbol')
    res = client.get_charts(start_date=start_date, end_date=end_date,
                            symbol=symbol, timeframe=timeframe)
    return json.dumps(res)


def _get_transaction_of_day(day, transaction_list, start_index) -> Tuple[List[Transaction], int]:
    res = []
    ind = start_index
    while ind < len(transaction_list):
        if transaction_list[ind].exit_time.date() == day:
            res.append(transaction_list[ind])
            ind += 1
        else:
            break
    return res, ind


def _get_diff_table(a_transactions: List[Transaction], b_transactions: List[Transaction]):
    def _convert_time(dt):
        return pd.to_datetime(dt).tz_convert(TIME_ZONE).strftime('%H:%M')

    def _get_row(t, html_class=None):
        template = ('<tr><td {cls}><a href={link}>{symbol}</a></td><td {cls_xs}>{processor}</td>'
                    '<td {cls_lg}>{side}</td><td {cls}>{entry_time}</td><td {cls}>{exit_time}</td>'
                    '<td {cls}><span class="lg-hidden">{gl}</span><span class="lg-show">{arrow}</span></td></tr>')
        cls = ''
        cls_lg = 'class="lg-hidden"'
        cls_xs = 'class="xs-hidden"'
        if html_class:
            cls = f'class="{html_class}"'
            cls_lg = f'class="lg-hidden {html_class}"'
            cls_xs = f'class="xs-hidden {html_class}"'
        side = ('<span class="badge-shape ' +
                ('badge-blue' if t.is_long else 'badge-purple') + '">' +
                ('long' if t.is_long else 'short') + '</span>')
        link = construct_charts_link(t.symbol,
                                     pd.to_datetime(t.exit_time).tz_convert(TIME_ZONE).strftime('%F'))
        return template.format(cls=cls,
                               cls_xs=cls_xs,
                               cls_lg=cls_lg,
                               symbol=t.symbol,
                               link=link,
                               processor=t.processor or 'UNKNOWN',
                               side=side,
                               entry_time=_convert_time(t.entry_time),
                               exit_time=_convert_time(t.exit_time),
                               gl=get_signed_percentage(t.gl_pct),
                               arrow=get_colored_value('', 'green' if t.gl_pct > 0 else 'red', with_arrow=True))

    for trans in [a_transactions, b_transactions]:
        trans.sort(key=lambda t: (t.exit_time, t.symbol))
    t0 = a_transactions[0] if a_transactions else b_transactions[0]
    table = {'date': t0.exit_time.strftime('%F'),
             'backtest': '',
             'trade': ''}
    miss, extra, time_diff, comm = 0, 0, 0, 0
    i, j = 0, 0
    empty_row = ('<tr><td>&nbsp</td><td class="xs-hidden"></td><td class="lg-hidden"></td>'
                 '<td></td><td></td><td></td></tr>')
    a_set = {(t.symbol, t.processor, t.entry_time, t.exit_time) for t in a_transactions}
    b_set = {(t.symbol, t.processor, t.entry_time, t.exit_time) for t in b_transactions}
    while i < len(a_transactions) or j < len(b_transactions):
        if i == len(a_transactions):
            extra += 1
            table['backtest'] += empty_row
            table['trade'] += _get_row(b_transactions[j], html_class='diff_add')
            j += 1
            continue
        elif j == len(b_transactions):
            miss += 1
            table['backtest'] += _get_row(a_transactions[i], html_class='diff_sub')
            table['trade'] += empty_row
            i += 1
            continue
        a = a_transactions[i]
        b = b_transactions[j]
        if (a.symbol == b.symbol and a.processor == b.processor
                and a.entry_time == b.entry_time
                and a.exit_time == b.exit_time):
            comm += 1
            table['backtest'] += _get_row(a_transactions[i])
            table['trade'] += _get_row(b_transactions[j])
            i += 1
            j += 1
        elif (a.symbol, a.processor, a.entry_time, a.exit_time) in b_set:
            extra += 1
            table['backtest'] += empty_row
            table['trade'] += _get_row(b_transactions[j], html_class='diff_add')
            j += 1
        elif (b.symbol, b.processor, b.entry_time, b.exit_time) in a_set:
            miss += 1
            table['backtest'] += _get_row(a_transactions[i], html_class='diff_sub')
            table['trade'] += empty_row
            i += 1
        elif a.symbol == b.symbol and a.processor == b.processor:
            time_diff += 1
            table['backtest'] += _get_row(a_transactions[i], html_class='diff_time')
            table['trade'] += _get_row(b_transactions[j], html_class='diff_time')
            i += 1
            j += 1
        elif a.exit_time <= b.exit_time:
            miss += 1
            table['backtest'] += _get_row(a_transactions[i], html_class='diff_sub')
            table['trade'] += empty_row
            i += 1
        else:
            extra += 1
            table['backtest'] += empty_row
            table['trade'] += _get_row(b_transactions[j], html_class='diff_add')
            j += 1

    return table, miss, extra, time_diff, comm


@bp.route('/backtest')
@access_control
def backtest():
    current_time = get_current_time()
    backtest_finish_time = get_backtest_finish_time()
    if backtest_finish_time is None or backtest_finish_time.date() != current_time.date():
        backtest_finish_time = DEFAULT_BACKTEST_FINISH_TIME
    else:
        backtest_finish_time = backtest_finish_time.time()
    if current_time.time() < backtest_finish_time:
        current_time -= datetime.timedelta(days=1)
    ndays = flask.request.args.get('ndays')
    ndays = int(ndays) if ndays and ndays.isdigit() else 7
    start_time = pd.to_datetime(current_time.strftime('%F')) - datetime.timedelta(days=ndays)
    start_time = max(start_time, pd.to_datetime(FIRST_BACKTEST_DATE))
    start_time = start_time.tz_localize(TIME_ZONE)
    end_time = pd.to_datetime(pd.to_datetime(current_time).strftime('%F 23:59:59')).tz_localize(TIME_ZONE)
    client = Db()
    processors = _list_processors(client)
    active_processor = flask.request.args.get('processor')
    if active_processor not in processors:
        active_processor = None
    processors = ['ALL PROCESSORS'] + processors

    backtest_transactions = [t for t in client.get_backtest(start_time, end_time, active_processor)
                             if abs(t.qty) > 1E-7]
    actual_transactions = client.list_transactions(limit=len(backtest_transactions) * 2 + 1000,
                                                   offset=0,
                                                   start_time=start_time,
                                                   end_time=end_time,
                                                   processor=active_processor)
    t = end_time.date()
    i, j = 0, 0
    miss, extra, time_diff, comm = 0, 0, 0, 0
    tables = []
    while t >= start_time.date():
        a, i = _get_transaction_of_day(t, backtest_transactions, i)
        b, j = _get_transaction_of_day(t, actual_transactions, j)
        if a or b:
            table, t_miss, t_extra, t_time_diff, t_comm = _get_diff_table(a, b)
            tables.append(table)
            miss += t_miss
            extra += t_extra
            time_diff += t_time_diff
            comm += t_comm
        t -= datetime.timedelta(days=1)
    rate = (miss + extra + time_diff) / max(miss + extra + time_diff + comm, 1)
    return flask.render_template('backtest.html',
                                 tables=tables,
                                 miss=miss,
                                 extra=extra,
                                 time_diff=time_diff,
                                 comm=comm,
                                 rate=f'{rate * 100:.2f}%',
                                 active_processor=active_processor,
                                 processors=processors,
                                 ndays=ndays)


@bp.route('/job_status')
def job_status():
    return get_job_status()


@bp.route('/robots.txt')
def robots_txt():
    return flask.send_from_directory('static', 'txt/robots.txt')


@bp.route('/file/<path:filepath>')
def get_file(filepath):
    with open(filepath, 'r') as f:
        response = flask.make_response(f.read(), 200)
        response.mimetype = 'text/plain'
        return response
