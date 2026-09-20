import collections
import datetime
import functools
import html
import json
import math
import os
import re
import urllib.parse
from concurrent import futures

import flask
import numpy as np
import pandas as pd

from alpharius.db import Aggregation, Db
from alpharius.utils import (
    TIME_ZONE,
    Transaction,
    compute_bernoulli_ci95,
    compute_drawdown,
    compute_risks,
    construct_charts_link,
    get_colored_value,
    get_current_time,
    get_day_range,
    get_signed_percentage,
    get_today,
)

from .client import Client
from .scheduler import get_backtest_finish_time, get_job_status

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
        response = {
            'histories': tasks['histories'].result(),
            'orders': tasks['orders'].result(),
            'positions': tasks['positions'].result(),
            'watch': tasks['watch'].result(),
        }
    return response


@bp.route('/')
@access_control
def dashboard():
    data = _get_dashboard_data()
    return flask.render_template(
        'dashboard.html',
        histories=json.dumps(data['histories']),
        orders=data['orders'],
        positions=data['positions'],
        watch=data['watch'],
    )


@bp.route('/dashboard_data')
def dashboard_data():
    data = _get_dashboard_data()
    return json.dumps(data)


def _list_processors(db_client: Db) -> list[str]:
    aggs = db_client.list_aggregations()
    processors = sorted({agg.processor for agg in aggs if agg.processor != 'UNKNOWN'})
    return processors


def _parse_date(value: str | None) -> datetime.date | None:
    """Parses a YYYY-MM-DD request argument. Missing or malformed values give None instead of an error."""
    try:
        return datetime.date.fromisoformat(value) if value else None
    except ValueError:
        return None


def _format_date(day: datetime.date | None) -> str:
    return day.isoformat() if day else ''


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
    # Optional date range (inclusive), on exit time in the market time zone. Anything that isn't a date means no
    # filter. With only one end given the range is that single day.
    active_start = _parse_date(flask.request.args.get('start_date'))
    active_end = _parse_date(flask.request.args.get('end_date'))
    active_start = active_start or active_end
    active_end = active_end or active_start
    if active_start and active_end and active_start > active_end:
        active_start, active_end = active_end, active_start
    start_time, end_time = None, None
    if active_start and active_end:
        start_time, end_time = get_day_range(active_start)[0], get_day_range(active_end)[1]
    count = client.get_transaction_count(active_processor, start_time=start_time, end_time=end_time)
    total_page = max(int(np.ceil(count / items_per_page)), 1)
    page = max(min(page, total_page), 1)
    offset = (page - 1) * items_per_page
    trans = []
    time_fmt = '<span class="lg-hidden">%Y-%m-%d </span>%H:%M'
    for t in client.list_transactions(
        limit=items_per_page, offset=offset, start_time=start_time, end_time=end_time, processor=active_processor
    ):
        entry_time = pd.Timestamp(t.entry_time).tz_convert(TIME_ZONE)
        exit_time = pd.Timestamp(t.exit_time).tz_convert(TIME_ZONE)
        marks = None
        if entry_time.date() == exit_time.date():
            marks = [entry_time.strftime('%H:%M'), exit_time.strftime('%H:%M')]
        trans.append(
            {
                'symbol': t.symbol,
                'side': 'long' if t.is_long else 'short',
                'processor': t.processor if t.processor is not None else '',
                'entry_price': f'{t.entry_price:.4g}',
                'exit_price': f'{t.exit_price:.4g}',
                'entry_time': entry_time.strftime(time_fmt),
                'exit_time': exit_time.strftime(time_fmt),
                'entry_full': entry_time.strftime('%Y-%m-%d %H:%M'),
                'exit_full': exit_time.strftime('%Y-%m-%d %H:%M'),
                'gl': get_colored_value(f'{t.gl:+,.2f} ({t.gl_pct * 100:+.2f}%)', 'green' if t.gl >= 0 else 'red'),
                'gl_pct': get_signed_percentage(t.gl_pct),
                'slippage': get_colored_value(
                    f'{t.slippage:+,.2f} ({t.slippage_pct * 100:+.2f}%)', 'green' if t.slippage >= 0 else 'red'
                )
                if t.slippage is not None
                else '',
                'slippage_pct': get_signed_percentage(t.slippage_pct) if t.slippage_pct is not None else '',
                'link': construct_charts_link(t.symbol, exit_time.strftime('%F'), marks),
            }
        )
    # Filters the pagination links carry along
    filters = {}
    if active_processor:
        filters['processor'] = active_processor
    if active_start and active_end:
        filters['start_date'] = active_start.isoformat()
        filters['end_date'] = active_end.isoformat()
    return flask.render_template(
        'transactions.html',
        transactions=trans,
        current_page=page,
        total_page=total_page,
        active_processor=active_processor,
        active_start=_format_date(active_start),
        active_end=_format_date(active_end),
        extra_query='&' + urllib.parse.urlencode(filters) if filters else '',
        processors=processors,
    )


def _shift_to_last(arr, target_value):
    for i in range(len(arr)):
        if arr[i] == target_value:
            for j in range(i + 1, len(arr)):
                arr[j], arr[j - 1] = arr[j - 1], arr[j]
            break


# Time ranges of the profit and slippage tables: name -> days back from today (None is all history)
STATS_RANGES = {'3M': 90, '6M': 182, '1Y': 365, 'ALL': None}
DEFAULT_STATS_RANGE = '3M'
_STAT_KEYS = ['gl', 'cnt', 'win_cnt', 'slip', 'slip_pct_acc', 'slip_cnt', 'cash_flow']
_SLIPPAGE_KEYS = {'slip', 'slip_pct_acc', 'slip_cnt'}


def _format_stat(processor: str, stat: dict) -> dict:
    win_rate = stat['win_cnt'] / stat['cnt'] if stat['cnt'] > 0 else None
    win_rate_ci = compute_bernoulli_ci95(win_rate, stat['cnt']) if win_rate else None
    has_slippage = processor != 'UNKNOWN'
    return {
        'processor': processor,
        'cnt': f'{stat["cnt"]:,}',
        'gl': get_colored_value(f'{stat["gl"]:,.2f}', 'green' if stat['gl'] >= 0 else 'red'),
        'win_rate': f'{win_rate * 100:.2f}%' if win_rate is not None else 'N/A',
        'win_rate_ci': f'&plusmn; {win_rate_ci * 100:.2f}%' if win_rate_ci else '',
        'slip': get_colored_value(f'{stat["slip"]:,.2f}', 'green' if stat['slip'] >= 0 else 'red')
        if has_slippage
        else 'N/A',
        'avg_slip_pct': get_signed_percentage(stat['slip_pct_acc'] / stat['slip_cnt'])
        if has_slippage and stat['slip_cnt'] > 0
        else 'N/A',
    }


def _get_stats(aggs: list[Aggregation]):
    """Gets per-processor stats and pie chart cash flows for every range in STATS_RANGES."""
    today = get_today().date()
    cutoffs = {name: today - datetime.timedelta(days=days) if days else None for name, days in STATS_RANGES.items()}
    processors = sorted({agg.processor for agg in aggs})
    # Every processor starts at zero in every range, so a processor with nothing traded in one still sums cleanly
    sums = {name: {processor: dict.fromkeys(_STAT_KEYS, 0) for processor in processors} for name in STATS_RANGES}
    for agg in aggs:
        for name, cutoff in cutoffs.items():
            if cutoff is not None and agg.date < cutoff:
                continue
            stat = sums[name][agg.processor]
            stat['gl'] += agg.gl
            stat['cnt'] += agg.count
            stat['win_cnt'] += agg.win_count
            stat['slip'] += agg.slippage
            stat['cash_flow'] += agg.cash_flow
            if agg.slippage_count > 0:
                stat['slip_pct_acc'] += agg.avg_slippage_pct * agg.slippage_count
                stat['slip_cnt'] += agg.slippage_count

    stats = {}
    cash_flows = {}
    for name, processor_sums in sums.items():
        # Pie chart entries, biggest first. Processors with nothing in the range are left out.
        cash_flows[name] = sorted(
            (
                {'processor': p, 'cash_flow': int(stat['cash_flow'])}
                for p, stat in processor_sums.items()
                if int(stat['cash_flow']) != 0
            ),
            key=lambda entry: entry['cash_flow'],
            reverse=True,
        )
        total = dict.fromkeys(_STAT_KEYS, 0)
        for processor, stat in processor_sums.items():
            for key, value in stat.items():
                # UNKNOWN transactions have no slippage, so they stay out of the slippage totals
                if not (processor == 'UNKNOWN' and key in _SLIPPAGE_KEYS):
                    total[key] += value
        # Order alphabetically with 'UNKNOWN' and then the 'ALL' total at last. Rows with no transactions are left out.
        order = sorted(processor_sums)
        _shift_to_last(order, 'UNKNOWN')
        rows = [_format_stat(p, processor_sums[p]) for p in order if processor_sums[p]['cnt'] > 0]
        if total['cnt'] > 0:
            rows.append(_format_stat('ALL', total))
        stats[name] = rows
    return stats, cash_flows


def _get_gl_bars(aggs: list[Aggregation]):
    dated_values = {'Daily': collections.defaultdict(int), 'Monthly': collections.defaultdict(int)}
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
        labels[timeframe] = sorted(dated_values[timeframe].keys())[-num_cuts[timeframe] :]
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
        res['returns'][j] = [(spots[j][k + 1] / spots[j][k] - 1) * 100 for k in range(len(spots[j]) - 1)]
    return res


def _get_risks(daily_prices):
    def get_factors(v, mv):
        a, b, s = compute_risks(v, mv)
        if v:
            d, _, _ = compute_drawdown(v)
            d = get_signed_percentage(d)
            r = get_signed_percentage(v[-1] / v[0] - 1)
        else:
            d = 'N/A'
            r = 'N/A'
        return {
            'alpha': get_signed_percentage(a) if not math.isnan(a) else 'N/A',
            'beta': f'{b:.2f}' if not math.isnan(b) else 'N/A',
            'sharpe': f'{s:.2f}' if not math.isnan(s) else 'N/A',
            'drawdown': d,
            'return': r,
        }

    dates = daily_prices['dates']
    values = daily_prices['values'][daily_prices['symbols'].index('My Portfolio')]
    market_values = daily_prices['values'][daily_prices['symbols'].index('SPY')]
    current_start = 0
    res = []
    for i in range(len(dates)):
        if i != len(dates) - 1 and dates[i][:4] == dates[i + 1][:4]:
            continue
        current_values = values[current_start : i + 1]
        current_market_values = market_values[current_start : i + 1]
        factors = get_factors(current_values, current_market_values)
        factors['year'] = dates[i][:4]
        res.append(factors)
        current_start = i
    overall_factors = get_factors(values, market_values)
    overall_factors['year'] = 'ALL'
    if values:
        annualized_return = get_signed_percentage((values[-1] / values[0]) ** (252 / len(values)) - 1)
    else:
        annualized_return = 'N/A'
    overall_factors['return'] = annualized_return
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
    stats, cash_flows = _get_stats(aggs)
    gl_bars, processors = _get_gl_bars(aggs)
    daily_price = get_daily_price_task.result()
    annual_return = _get_annual_return(daily_price)
    risks = _get_risks(daily_price)
    return flask.render_template(
        'analytics.html',
        stats=stats,
        stats_ranges=list(STATS_RANGES),
        default_stats_range=DEFAULT_STATS_RANGE,
        cash_flows=cash_flows,
        gl_bars=gl_bars,
        annual_return=annual_return,
        risks=risks,
        processors=processors,
    )


def _parse_log_content(content: str, date: str):
    def is_entry_start(lin: str):
        ll = lin.lower()
        return (
            ll.startswith('[info] [')
            or ll.startswith('[warning] [')
            or ll.startswith('[debug] [')
            or ll.startswith('[error] [')
        )

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
                spans.append(line[span_start + 1 : span_end])
            message = line[span_end + 1 :]
            message = re.sub(r'\[([A-Z]+)\]', f'[<a href={link}>\\1</a>]', message)
            log_type = spans[0].lower()
            log_entry = {
                'type': log_type,
                'type_initial': log_type[0],
                'time': pd.to_datetime(spans[1]).strftime('%H:%M:%S'),
                'time_short': pd.to_datetime(spans[1]).strftime('%H:%M'),
                'code': spans[2],
                'message': message,
            }
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

    return flask.render_template('logs.html', loggers=loggers, log_entries=log_entries, date=date, dates=dates)


@bp.route('/charts')
@access_control
def charts():
    client = Client()
    date = _parse_date(flask.request.args.get('date'))
    start_date = _parse_date(flask.request.args.get('start_date'))
    end_date = _parse_date(flask.request.args.get('end_date'))
    marks = flask.request.args.get('marks')
    if start_date and end_date:
        # Weekends have no data: move the range inward to trading days
        if start_date.isoweekday() > 5:
            start_date += datetime.timedelta(days=8 - start_date.isoweekday())
        if end_date.isoweekday() > 5:
            end_date -= datetime.timedelta(days=end_date.isoweekday() - 5)
    symbol = flask.request.args.get('symbol')
    all_symbols = client.get_all_symbols()
    return flask.render_template(
        'charts.html',
        all_symbols=all_symbols,
        init_date=_format_date(date),
        init_start_date=_format_date(start_date),
        init_end_date=_format_date(end_date),
        init_marks=marks,
        init_symbol=symbol,
    )


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
    res = client.get_charts(start_date=start_date, end_date=end_date, symbol=symbol, timeframe=timeframe)
    marks = flask.request.args.get('marks')
    if marks:
        marks = marks.split(',')
        for i, t in enumerate(marks):
            try:
                marks[i] = (datetime.datetime.strptime(t, '%H:%M') - datetime.timedelta(minutes=5)).strftime('%H:%M')
            except ValueError as e:
                print('value', e)
                break
        else:
            res['marks'] = marks
    return json.dumps(res)


def _get_transaction_of_day(day, transaction_list, start_index) -> tuple[list[Transaction], int]:
    res = []
    ind = start_index
    while ind < len(transaction_list):
        if transaction_list[ind].exit_time.date() == day:
            res.append(transaction_list[ind])
            ind += 1
        else:
            break
    return res, ind


def _get_diff_table(a_transactions: list[Transaction], b_transactions: list[Transaction]):
    def _convert_time(dt):
        return pd.to_datetime(dt).tz_convert(TIME_ZONE).strftime('%H:%M')

    def _get_row(t, html_class=None):
        template = (
            '<tr class="tip-row" data-processor="{processor}" data-side="{side_text}" data-gl="{gl_text}">'
            '<td {cls}><a href={link}>{symbol}</a></td><td {cls_xs}>{processor}</td>'
            '<td {cls_lg}>{side}</td><td {cls}>{entry_time}</td><td {cls}>{exit_time}</td>'
            '<td {cls}><span class="lg-hidden">{gl}</span><span class="lg-show">{arrow}</span></td></tr>'
        )
        cls = ''
        cls_lg = 'class="lg-hidden"'
        cls_xs = 'class="xs-hidden"'
        if html_class:
            cls = f'class="{html_class}"'
            cls_lg = f'class="lg-hidden {html_class}"'
            cls_xs = f'class="xs-hidden {html_class}"'
        side = (
            '<span class="badge-shape '
            + ('badge-blue' if t.is_long else 'badge-purple')
            + '">'
            + ('long' if t.is_long else 'short')
            + '</span>'
        )
        entry_time = pd.to_datetime(t.entry_time).tz_convert(TIME_ZONE)
        exit_time = pd.to_datetime(t.exit_time).tz_convert(TIME_ZONE)
        marks = None
        if entry_time.date() == exit_time.date():
            marks = [entry_time.strftime('%H:%M'), exit_time.strftime('%H:%M')]
        link = construct_charts_link(t.symbol, exit_time.strftime('%F'), marks)
        return template.format(
            cls=cls,
            cls_xs=cls_xs,
            cls_lg=cls_lg,
            symbol=t.symbol,
            link=link,
            processor=html.escape(t.processor or 'UNKNOWN'),
            side=side,
            side_text='long' if t.is_long else 'short',
            gl_text=f'{t.gl_pct * 100:+.2f}%',
            entry_time=_convert_time(t.entry_time),
            exit_time=_convert_time(t.exit_time),
            gl=get_signed_percentage(t.gl_pct),
            arrow=get_colored_value('', 'green' if t.gl_pct > 0 else 'red', with_arrow=True),
        )

    def _normalize_time(t: pd.Timestamp | None) -> pd.Timestamp | None:
        # Normalize transactions whose orders are not filled in time
        if t is None:
            return t
        return t - datetime.timedelta(minutes=t.minute % 5)

    for trans in [a_transactions, b_transactions]:
        trans.sort(key=lambda t: (t.exit_time, t.symbol))
    t0 = a_transactions[0] if a_transactions else b_transactions[0]
    table = {'date': t0.exit_time.strftime('%F'), 'backtest': '', 'trade': ''}
    miss, extra, time_diff, comm = 0, 0, 0, 0
    i, j = 0, 0
    empty_row = (
        '<tr><td>&nbsp</td><td class="xs-hidden"></td><td class="lg-hidden"></td><td></td><td></td><td></td></tr>'
    )
    a_set = {(t.symbol, t.processor, t.entry_time, _normalize_time(t.exit_time)) for t in a_transactions}
    b_set = {(t.symbol, t.processor, t.entry_time, _normalize_time(t.exit_time)) for t in b_transactions}
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
        if (
            a.symbol == b.symbol
            and a.processor == b.processor
            and a.entry_time == b.entry_time
            and _normalize_time(a.exit_time) == _normalize_time(b.exit_time)
        ):
            comm += 1
            table['backtest'] += _get_row(a_transactions[i])
            table['trade'] += _get_row(b_transactions[j])
            i += 1
            j += 1
        elif (a.symbol, a.processor, a.entry_time, _normalize_time(a.exit_time)) in b_set:
            extra += 1
            table['backtest'] += empty_row
            table['trade'] += _get_row(b_transactions[j], html_class='diff_add')
            j += 1
        elif (b.symbol, b.processor, b.entry_time, _normalize_time(b.exit_time)) in a_set:
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
        elif _normalize_time(a.exit_time) <= _normalize_time(b.exit_time):
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

    backtest_transactions = [
        t for t in client.get_backtest(start_time, end_time, active_processor) if abs(t.qty) > 1e-7
    ]
    actual_transactions = client.list_transactions(
        limit=len(backtest_transactions) * 2 + 1000,
        offset=0,
        start_time=start_time,
        end_time=end_time,
        processor=active_processor,
    )
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
    total = miss + extra + time_diff + comm
    match_rate = comm / total if total else None
    return flask.render_template(
        'backtest.html',
        tables=tables,
        miss=miss,
        extra=extra,
        time_diff=time_diff,
        comm=comm,
        match_rate=match_rate * 100 if match_rate is not None else None,
        active_processor=active_processor,
        processors=processors,
        ndays=ndays,
    )


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
