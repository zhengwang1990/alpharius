import copy
import datetime
import math
import os
import re
import threading
import time
from concurrent import futures
from typing import List, Tuple

import alpaca_trade_api as tradeapi
import numpy as np
import pandas as pd
import retrying
from dateutil.relativedelta import relativedelta
from flask import Flask

import alpharius.data as data
from alpharius.utils import (
    construct_charts_link, get_signed_percentage, get_colored_value,
    get_latest_day, TIME_ZONE,
)

app = Flask(__name__)

START_DATE = '2023-03-14'


def get_time_vs_equity(history_equity: List[float],
                       history_time: List[int],
                       time_format: str,
                       cash_reserve: float) -> Tuple[List[str], List[float]]:
    time_list = []
    equity_list = []
    for i, (e, t) in enumerate(zip(history_equity, history_time)):
        if e is None:
            continue
        dt = pd.to_datetime(t, utc=True, unit='s').tz_convert(TIME_ZONE)
        equity_list.append(max(e - cash_reserve, 0))
        time_list.append(dt.strftime(time_format))
    start = 0
    while start < len(equity_list) and equity_list[start] == 0:
        start += 1
    time_list = time_list[start:]
    equity_list = equity_list[start:]
    if len(time_list) > 1 and time_list[-2] == time_list[-1]:
        time_list.pop(-2)
        equity_list.pop(-2)
    return time_list, equity_list


def round_time(t: pd.Timestamp, time_fmt_with_year: bool):
    fmt = f'<span class="xs-hidden">{"%Y-" if time_fmt_with_year else ""}%m-%d </span>%H:%M'
    if t.second < 30:
        return t.strftime(fmt)
    return (t + datetime.timedelta(minutes=1)).strftime(fmt)


class Client:

    def __init__(self):
        self._alpaca = tradeapi.REST()
        self._data_client = data.FmpClient()

    def get_calendar(self):
        latest_day = get_latest_day()
        calendar = self._alpaca.get_calendar(
            start=max((latest_day - relativedelta(years=5)).strftime('%F'),
                      START_DATE),
            end=latest_day.strftime('%F'))
        return calendar

    @retrying.retry(stop_max_attempt_number=2, wait_exponential_multiplier=1000)
    def get_portfolio_histories(self):
        start = time.time()
        result = dict()
        latest_day = get_latest_day()
        calendar = self.get_calendar()
        result['market_close'] = calendar[-1].close.strftime('%H:%M')
        market_dates = [c.date for c in calendar]
        tasks = dict()
        with futures.ThreadPoolExecutor(max_workers=2) as pool:
            for start_index, timeframe in [(-1, '5Min'), (0, '1D')]:
                extended_hours = True if start_index == - 1 else False
                tasks[timeframe] = pool.submit(self._alpaca.get_portfolio_history,
                                               date_start=market_dates[start_index].strftime('%F'),
                                               date_end=market_dates[-1].strftime('%F'),
                                               period=None,
                                               timeframe=timeframe,
                                               extended_hours=extended_hours)
            histories = dict()
            for timeframe in ['5Min', '1D']:
                histories[timeframe] = tasks[timeframe].result()
        cash_reserve = float(os.environ.get('CASH_RESERVE', 0))
        result['time_1d'], result['equity_1d'] = get_time_vs_equity(
            histories['5Min'].equity,
            histories['5Min'].timestamp,
            '%H:%M',
            cash_reserve)
        result['time_5y'], result['equity_5y'] = get_time_vs_equity(
            histories['1D'].equity,
            histories['1D'].timestamp,
            '%F',
            cash_reserve)
        # Current equity value is wrong from get_portfolio_history
        current_equity = max(float(self._alpaca.get_account().equity) - cash_reserve, 0)
        result['current_equity'] = f'{current_equity:,.2f}'
        if result['time_5y'][-1] != calendar[-1].date.strftime('%F'):
            result['time_5y'].append(calendar[-1].date.strftime('%F'))
            result['equity_5y'].append(current_equity)
        else:
            result['equity_5y'][-1] = current_equity
        time_points = copy.copy(result['time_5y'])
        result['equity_1d'][-1] = current_equity
        result['prev_close'] = (result['equity_5y'][-2]
                                if len(result['equity_5y']) > 2 else math.nan)
        for time_period, time_delta in [('1w', relativedelta(weeks=1)),
                                        ('2w', relativedelta(weeks=2)),
                                        ('1m', relativedelta(months=1)),
                                        ('6m', relativedelta(months=6)),
                                        ('ytd', None),
                                        ('1y', relativedelta(years=1))]:
            if time_period == 'ytd':
                new_year_day = f'{latest_day.year}-01-01'
                cut_i = len(result['time_5y']) - 1
                cut = result['time_5y'][cut_i]
                while cut_i > 0 and cut >= new_year_day:
                    cut_i -= 1
                    cut = result['time_5y'][cut_i]
            else:
                cut = (calendar[-1].date - time_delta).strftime('%F')
            time_str = 'time_' + time_period
            equity_str = 'equity_' + time_period
            result[time_str] = result[equity_str] = []
            for i in range(len(result['time_5y'])):
                if result['time_5y'][i] >= cut and result['equity_5y'][i] > 0:
                    result[time_str] = result['time_5y'][i:]
                    result[equity_str] = result['equity_5y'][i:]
                    break
        for time_period in ['1d', '1w', '2w', '1m', '6m', 'ytd', '1y', '5y']:
            if time_period == '1d':
                base_value = (result['prev_close']
                              if result['prev_close'] > 0 else current_equity)
            else:
                base_value = (result['equity_' + time_period][0]
                              if result['equity_' + time_period] else current_equity)
            change = current_equity - base_value
            percent = current_equity / base_value - 1
            result['change_' + time_period] = get_colored_value(
                f'{change:+.2f} ({percent * 100:+.2f}%)',
                'green' if change >= 0 else 'red',
                with_arrow=True)
            result['color_' + time_period] = 'green' if change >= 0 else 'red'
        i = 0
        month_max = month_min = 0
        for j in range(len(result['time_5y'])):
            if j % 30 == 0:
                month_max = np.max(result['equity_5y'][j:j + 30])
                month_min = np.min(result['equity_5y'][j:j + 30])
            equity = result['equity_5y'][j]
            if (j % 3 == 0 or j == len(result['time_5y']) - 1 or
                    equity == month_max or equity == month_min):
                result['time_5y'][i] = result['time_5y'][j]
                result['equity_5y'][i] = equity
                i += 1
        result['time_5y'] = result['time_5y'][:i]
        result['equity_5y'] = result['equity_5y'][:i]
        if result['time_1d'][-1] > '09:30':
            for i in range(len(result['time_1d'])):
                if result['time_1d'][i] >= '09:30':
                    result['time_1d'] = result['time_1d'][i:]
                    result['equity_1d'] = result['equity_1d'][i:]
                    break
        compare_symbols = ['QQQ', 'SPY', 'TQQQ']
        with futures.ThreadPoolExecutor(max_workers=3) as pool:
            lock = threading.RLock()
            for symbol in compare_symbols:
                tasks[symbol] = pool.submit(self.get_compare_symbol, symbol,
                                            market_dates[-1], time_points,
                                            result, lock)
            for symbol in compare_symbols:
                tasks[symbol].result()
        app.logger.info('Time cost for get_portfolio_histories: [%.2fs]', time.time() - start)
        return result

    def get_compare_symbol(self,
                           symbol: str,
                           latest_day,
                           time_points,
                           portfolio_histories,
                           lock: threading.RLock):
        day_bars = self._data_client.get_data(
            symbol,
            pd.Timestamp.combine(latest_day, datetime.time(0, 0)).tz_localize(TIME_ZONE),
            pd.Timestamp.combine(latest_day, datetime.time(23, 0)).tz_localize(TIME_ZONE),
            data.TimeInterval.FIVE_MIN,
        )
        dict_1d = dict()
        for index, bar in day_bars.iterrows():
            index: pd.Timestamp
            t_open = index.strftime('%H:%M')
            t_close = (index + datetime.timedelta(minutes=5)).strftime('%H:%M')
            if t_open not in dict_1d:
                dict_1d[t_open] = bar['Open']
            dict_1d[t_close] = bar['Close']
        year_bars = self._data_client.get_data(
            symbol,
            pd.Timestamp(time_points[0]),
            pd.Timestamp(time_points[-1]),
            data.TimeInterval.DAY)
        dict_5y = dict()
        for index, bar in year_bars.iterrows():
            index: pd.Timestamp
            t = index.strftime('%F')
            dict_5y[t] = bar['Close']
        symbol_values = dict()
        current_symbol_value = day_bars['Close'].iloc[-1] if len(day_bars) else year_bars['Close'].iloc[-1]
        timeframes = ['1d', '1w', '2w', '1m', '6m', 'ytd', '1y', '5y']
        for timeframe in timeframes:
            symbol_values[timeframe] = []
            timeline = portfolio_histories['time_' + timeframe]
            dict_ref = dict_1d if timeframe == '1d' else dict_5y
            for t in timeline:
                symbol_values[timeframe].append(dict_ref.get(t))
            if symbol_values[timeframe] and symbol_values[timeframe][-1]:
                symbol_values[timeframe][-1] = current_symbol_value
        for timeframe in timeframes:
            if timeframe == '1d':
                symbol_base = dict_5y[time_points[-2]] if len(time_points) > 1 else dict_5y[time_points[-1]]
                portfolio_base = portfolio_histories['prev_close']
            else:
                symbol_base = symbol_values[timeframe][0] if symbol_values[timeframe] else current_symbol_value
                portfolio_base = (portfolio_histories['equity_' + timeframe][0]
                                  if portfolio_histories['equity_' + timeframe]
                                  else portfolio_histories['equity_1d'][-1])
            for i in range(len(symbol_values[timeframe])):
                if symbol_values[timeframe][i] is not None:
                    symbol_values[timeframe][i] = symbol_values[timeframe][i] / symbol_base * portfolio_base
            # Using a lock as multiple threads are writing this object simultaneously
            with lock:
                portfolio_histories[symbol.lower() + '_' + timeframe] = symbol_values[timeframe]

    def get_recent_orders(self):
        return self.get_orders(-1, False)

    @retrying.retry(stop_max_attempt_number=2, wait_exponential_multiplier=1000)
    def get_orders(self, calendar_index: int, time_fmt_with_year: bool):
        start = time.time()
        result = []
        calendar = self.get_calendar()
        orders = self._alpaca.list_orders(status='closed',
                                          after=calendar[calendar_index - 1].date.strftime('%F'),
                                          direction='desc')
        orders_used = [False] * len(orders)
        positions = self._alpaca.list_positions()
        position_symbols = set([position.symbol for position in positions])
        cut_time = calendar[calendar_index].date
        for i in range(len(orders)):
            order = orders[i]
            if order.filled_at is None:
                continue
            filled_at = order.filled_at.tz_convert(TIME_ZONE)
            if filled_at < pd.Timestamp(cut_time).tz_localize(TIME_ZONE):
                break
            price = float(order.filled_avg_price)
            qty = float(order.filled_qty)
            order_obj = {'symbol': order.symbol,
                         'side': order.side,
                         'price': f'{price:.4g}',
                         'value': f'{price * qty:.2f}',
                         'link': construct_charts_link(order.symbol, filled_at.strftime('%F')),
                         'gl': '',
                         'time': round_time(filled_at, time_fmt_with_year)}
            if order.symbol in position_symbols:
                position_symbols.remove(order.symbol)
            elif not orders_used[i]:
                for j in range(i + 1, len(orders)):
                    prev_order = orders[j]
                    if prev_order.filled_at is None or prev_order.symbol != order.symbol:
                        continue
                    prev_filled_at = prev_order.filled_at.tz_convert(TIME_ZONE)
                    if prev_filled_at < filled_at and prev_order.side != order.side:
                        entry_price = float(prev_order.filled_avg_price)
                        order_gl = price / entry_price - 1
                        if prev_order.side == 'sell':
                            order_gl *= -1
                        order_obj['entry_price'] = f'{entry_price:.4g}'
                        order_obj['entry_time'] = round_time(prev_filled_at, time_fmt_with_year)
                        order_obj['gl'] = get_signed_percentage(order_gl)
                        orders_used[j] = True
                        break
            result.append(order_obj)
        app.logger.info('Time cost for get_orders: [%.2fs]', time.time() - start)
        return result

    @retrying.retry(stop_max_attempt_number=2, wait_exponential_multiplier=1000)
    def get_current_positions(self):
        start = time.time()
        result = []
        calendar = self.get_calendar()
        last_trading_day = calendar[-1].date.strftime('%F')
        positions = self._alpaca.list_positions()
        infos = self.get_info_today([p.symbol for p in positions])
        for position in positions:
            symbol = position.symbol
            qty = float(position.qty)
            info = infos[symbol]
            entry_price = float(position.avg_entry_price)
            current_price = info['price']
            value = abs(qty * current_price)
            side = 'long' if qty > 0 else 'short'
            gl = current_price / entry_price - 1
            if side == 'short':
                gl *= -1
            result.append({
                'symbol': symbol,
                'current_price': f'{current_price:.4g}',
                'value': f'{value:.2f}' if value < 1000 else f'{value:.0f}',
                'side': side,
                'day_change': get_signed_percentage(info['change']),
                'gl': get_signed_percentage(gl),
                'link': construct_charts_link(symbol, last_trading_day),
            })
        result.sort(key=lambda p: p['symbol'])
        app.logger.info('Time cost for get_current_positions: [%.2fs]', time.time() - start)
        return result

    @retrying.retry(stop_max_attempt_number=2, wait_exponential_multiplier=1000)
    def get_info_today(self, symbols: List[str]):
        if not symbols:
            return dict()
        result = dict()
        calendar = self.get_calendar()
        current_prices = self._data_client.get_last_trades(symbols)
        prev_day = calendar[-2].date.strftime('%F')
        tasks = dict()
        with futures.ThreadPoolExecutor(max_workers=4) as pool:
            for symbol in symbols:
                tasks[symbol] = pool.submit(self._data_client.get_daily,
                                            symbol=symbol,
                                            day=pd.Timestamp(prev_day),
                                            time_interval=data.TimeInterval.DAY)
            today_str = datetime.datetime.today().astimezone(TIME_ZONE).strftime('%b %d')
            trading_day = calendar[-1].date.strftime('%b %d')
            for symbol in symbols:
                bars = tasks[symbol].result()
                result[symbol] = {'price': current_prices[symbol],
                                  'change': current_prices[symbol] / bars['Close'].iloc[0] - 1,
                                  'date': trading_day if trading_day != today_str else 'Today'}
        return result

    def get_market_watch(self):
        start = time.time()
        result = dict()
        watch_symbols = ['QQQ', 'SPY', 'DIA', 'TQQQ']
        infos = self.get_info_today(watch_symbols)
        for symbol, info in infos.items():
            result[symbol] = {'price': f'{info["price"]:.2f}',
                              'change': get_signed_percentage(info['change'], with_arrow=True),
                              'date': info['date']}
        app.logger.info('Time cost for get_market_watch: [%.2fs]', time.time() - start)
        return result

    @retrying.retry(stop_max_attempt_number=2, wait_exponential_multiplier=1000)
    def get_daily_prices(self):
        start = time.time()
        calendar = self.get_calendar()
        end_date = calendar[-1].date.strftime('%F')
        portfolio_result = self._alpaca.get_portfolio_history(date_start=START_DATE,
                                                              date_end=end_date,
                                                              timeframe='1D',
                                                              extended_hours=False)
        cash_reserve = float(os.environ.get('CASH_RESERVE', 0))
        portfolio_dates = []
        portfolio_values = []
        for t, e in zip(portfolio_result.timestamp, portfolio_result.equity):
            if e is None:
                continue
            portfolio_dates.append(
                pd.to_datetime(t, utc=True, unit='s').tz_convert(TIME_ZONE).strftime('%F'))
            portfolio_values.append(max(e - cash_reserve, 0))
        compare_symbols = ['QQQ', 'SPY']
        tasks, bars = dict(), dict()
        with futures.ThreadPoolExecutor(max_workers=2) as pool:
            for symbol in compare_symbols:
                tasks[symbol] = pool.submit(self._data_client.get_data,
                                            symbol=symbol,
                                            start_time=pd.Timestamp(portfolio_dates[0]),
                                            end_time=pd.Timestamp(end_date),
                                            time_interval=data.TimeInterval.DAY)
            for symbol in compare_symbols:
                bars[symbol] = tasks[symbol].result()
        current_value = max(float(self._alpaca.get_account().equity) - cash_reserve, 0)
        if portfolio_dates[-1] != end_date:
            # Portfolio history may not include today's data
            portfolio_dates.append(end_date)
            portfolio_values.append(current_value)
        else:
            # Current equity value can be wrong from get_portfolio_history
            portfolio_values[-1] = current_value
        start_index = 0
        while start_index < len(portfolio_values) and portfolio_values[start_index] == 0:
            start_index += 1
        result = {'dates': portfolio_dates[start_index:],
                  'symbols': ['My Portfolio'] + compare_symbols,
                  'values': [portfolio_values[start_index:]]}
        all_dates = set(portfolio_dates)
        for symbol in compare_symbols:
            # Make it resilient when portfolio_values misses some dates
            for ind in bars[symbol].index:
                if ind.strftime('%F') not in all_dates:
                    bars[symbol].drop(ind, inplace=True)
            symbol_values = bars[symbol]['Close'].tolist()
            # In case symbol values do not include today's data
            if bars[symbol].index[-1].strftime('%F') != end_date:
                symbol_values.append(self._alpaca.get_latest_trades([symbol])[symbol].p)
            assert len(symbol_values) == len(portfolio_values), f'Symbol {symbol} has a different length'
            result['values'].append(symbol_values[start_index:])
        app.logger.info('Time cost for get_daily_prices: [%.2fs]', time.time() - start)
        return result

    @retrying.retry(stop_max_attempt_number=2, wait_exponential_multiplier=1000)
    def get_charts(self, start_date: str, end_date: str, symbol: str, timeframe: str):
        start_time = pd.to_datetime(
            pd.Timestamp.combine(pd.to_datetime(start_date).date(),
                                 datetime.time(0, 0))).tz_localize(TIME_ZONE)
        end_time = pd.to_datetime(
            pd.Timestamp.combine(pd.to_datetime(end_date).date(),
                                 datetime.time(23, 59))).tz_localize(TIME_ZONE)
        if timeframe == 'intraday':
            time_interval = data.TimeInterval.FIVE_MIN
        else:
            time_interval = data.TimeInterval.DAY
        bars = self._data_client.get_data(symbol=symbol,
                                          start_time=start_time,
                                          end_time=end_time,
                                          time_interval=time_interval)
        if timeframe == 'intraday':
            prev_close = self._data_client.get_data(symbol=symbol,
                                                    start_time=start_time - datetime.timedelta(days=7),
                                                    end_time=start_time - datetime.timedelta(days=1),
                                                    time_interval=data.TimeInterval.DAY)['Close'].iloc[-1]
            prev_close = float(prev_close)
        else:
            prev_close = None
        name = self._alpaca.get_asset(symbol).name
        labels = []
        prices = []
        volumes = []
        if timeframe == 'intraday':
            time_format = '%H:%M'
        else:
            time_format = '%m-%d' if len(bars) < 200 else '%F'
        for index, bar in bars.iterrows():
            index: pd.Timestamp
            label = index.strftime(time_format)
            if timeframe == 'intraday' and not '04:00' <= label <= '19:55':
                continue
            labels.append(label)
            price = {'h': float(bar['High']), 'l': float(bar['Low']), 'o': float(bar['Open']),
                     'c': float(bar['Close']), 'x': label, 's': [float(bar['Open']), float(bar['Close'])]}
            prices.append(price)
            volume = {'x': label, 's': int(bar['Volume']), 'g': int(bar['Close'] >= bar['Open'])}
            volumes.append(volume)
        return {'labels': labels, 'prices': prices, 'volumes': volumes,
                'prev_close': prev_close, 'name': name}

    @retrying.retry(stop_max_attempt_number=2, wait_exponential_multiplier=1000)
    def get_all_symbols(self):
        assets = self._alpaca.list_assets()
        return sorted(list(set([asset.symbol for asset in assets
                                if re.match('^[A-Z]*$', asset.symbol)])))
