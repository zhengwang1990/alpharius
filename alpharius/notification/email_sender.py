import argparse
import datetime
import email.mime.image as image
import email.mime.multipart as multipart
import email.mime.text as text
import html
import io
import logging
import os
import smtplib
import time
from typing import Optional

import alpaca_trade_api as tradeapi
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pytz
import retrying

import alpharius.data as data
from alpharius.utils import get_today

_TIME_ZONE = pytz.timezone('America/New_York')
_SMTP_HOST = 'smtp.163.com'
_SMTP_PORT = 25
_HTML_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'html')


class EmailSender:
    def __init__(self,
                 logger: Optional[logging.Logger] = None) -> None:
        username = os.environ.get('EMAIL_USERNAME')
        password = os.environ.get('EMAIL_PASSWORD')
        receiver = os.environ.get('EMAIL_RECEIVER')
        self._logger = logger or logging.getLogger()
        self._client = None
        if not username or not password or not receiver:
            self._logger.warning('Email config not provided')
            return
        self._client = self._create_client(username, password)
        self._sender = f'Stock Trading System <{username}@163.com>'
        self._receiver = receiver
        self._alpaca = tradeapi.REST()

    @retrying.retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
    def _create_client(self, username: str, password: str) -> smtplib.SMTP:
        self._logger.info('Logging into email server')
        client = smtplib.SMTP(_SMTP_HOST, _SMTP_PORT)
        client.starttls()
        client.ehlo()
        client.login(username, password)
        return client

    def _create_message(self, category: str, title: str):
        message = multipart.MIMEMultipart('alternative')
        message['From'] = self._sender
        message['To'] = self._receiver
        message['Subject'] = f'[{category}] [{get_today().strftime("%F")}] {title}'
        return message

    @staticmethod
    def _get_color_style(value: float):
        return 'style="color:{};"'.format('green' if value >= 0 else 'red')

    def send_summary(self, data_client: data.DataClient):
        if not self._client:
            self._logger.warning('Email client not created')
            return
        else:
            self._logger.info('Sending email')
        message = self._create_message('Summary', 'Trade summary of the day')
        today = get_today()
        alpaca = tradeapi.REST()
        calendar = alpaca.get_calendar(start=(today - datetime.timedelta(days=40)).strftime('%F'),
                                       end=today.strftime('%F'))
        market_dates = [market_day.date for market_day in calendar]
        positions_html = ''
        positions = self._alpaca.list_positions()
        for position in positions:
            change_today = float(position.change_today)
            gain = float(position.unrealized_plpc)
            positions_html += (f'<tr><th scope="row">{position.symbol}</th>'
                               f'<td>{float(position.qty):.5g}</td>'
                               f'<td>{float(position.avg_entry_price):.5g}</td>'
                               f'<td>{float(position.current_price):.5g}</td>'
                               f'<td {self._get_color_style(change_today)}>{change_today * 100:+.2f}%</td>'
                               f'<td {self._get_color_style(gain)}>{gain * 100:+.2f}%</td>'
                               '</td>\n')

        transactions_html = ''
        transactions = data.get_transactions(market_dates[-1].strftime('%F'), data_client)
        for transaction in transactions:
            gl_str = ''
            if transaction.gl_pct is not None:
                style = self._get_color_style(transaction.gl_pct)
                gl_str = f'<span {style}>{transaction.gl_pct * 100:+.2f}%</span>'
            slippage_str = ''
            if transaction.slippage_pct is not None:
                style = self._get_color_style(transaction.slippage_pct)
                slippage_str = f'<span {style}>{transaction.slippage_pct * 100:+.2f}%</span>'
            exit_price_str = f'{transaction.exit_price:.5g}' if transaction.exit_price else ''
            exit_time_str = transaction.exit_time.strftime("%H:%M") if transaction.exit_time else ''
            transactions_html += (f'<tr><th scope="row">{transaction.symbol}</th>'
                                  f'<td>{"long" if transaction.is_long else "short"}</td>'
                                  f'<td>{transaction.entry_price:.5g}</td>'
                                  f'<td>{exit_price_str}</td>'
                                  f'<td>{transaction.entry_time.strftime("%H:%M")}</td>'
                                  f'<td>{exit_time_str}</td>'
                                  f'<td>{gl_str}</td>'
                                  f'<td>{slippage_str}</td>'
                                  '</tr>\n')

        account = self._alpaca.get_account()
        cash_reserve = float(os.environ.get('CASH_RESERVE', 0))
        account_equity = float(account.equity) - cash_reserve
        account_cash = float(account.cash) - cash_reserve
        history_length = 10
        history = self._alpaca.get_portfolio_history(date_start=market_dates[-history_length].strftime('%F'),
                                                     date_end=market_dates[-2].strftime('%F'),
                                                     timeframe='1D')
        for i in range(len(history.equity)):
            history.equity[i] = (history.equity[i] - cash_reserve
                                 if history.equity[i] > cash_reserve else history.equity[i])
        equity_denominator = 1
        historical_start = 0
        for i, equity in enumerate(history.equity):
            if equity > 0:
                equity_denominator = equity
                historical_start = i
                break
        historical_value = [equity / equity_denominator for equity in history.equity]
        historical_value.append(account_equity / equity_denominator)
        for i in range(historical_start):
            historical_value[i] = None
        historical_date = [market_day for market_day in market_dates[-history_length:]]
        market_symbols = ['DIA', 'SPY', 'QQQ']
        market_values = {}
        for symbol in market_symbols:
            df = data_client.get_data(symbol,
                                      pd.Timestamp(historical_date[0]),
                                      pd.Timestamp(historical_date[-1]),
                                      data.TimeInterval.DAY)
            symbol_close = df['Close'].to_numpy()
            market_values[symbol] = symbol_close

        intraday_gain = account_equity - history.equity[-1]
        row_style = 'scope="row" class="narrow-col"'
        account_html = (f'<tr><th {row_style}>Equity</th><td>{account_equity:.2f}</td></tr>'
                        f'<tr><th {row_style}>Cash</th><td>{account_cash:.2f}</td></tr>')
        if cash_reserve > 0:
            account_html += f'<tr><th {row_style}>Reserve</th><td>{cash_reserve:.2f}</td></tr>'
        account_html += (f'<tr><th {row_style}>Gain / Loss</th>'
                         f'<td {self._get_color_style(intraday_gain)}>{intraday_gain:+.2f}'
                         f'({(account_equity / history.equity[-1] - 1) * 100:+.2f}%)</td></tr>\n'
                         f'<tr><th {row_style}>Market Change</th>'
                         '<td style="padding: 0px;"><table>')
        for symbol in market_symbols:
            if symbol not in market_values:
                continue
            symbol_value = market_values[symbol]
            symbol_gain = (symbol_value[-1] / symbol_value[-2] - 1) * 100
            account_html += (f'<tr><td style="border: none; padding: 0.5rem;">{symbol}</td>'
                             f'<td style="border: none; padding: 0.5rem;">{symbol_value[-1]:.2f}'
                             f'<span {self._get_color_style(symbol_gain)}>({symbol_gain:+.2f}%)</span>'
                             '</td></tr>')
        account_html += '</table></td></tr>'

        color_map = {'QQQ': '#78d237', 'SPY': '#FF6358', 'DIA': '#aa46be'}
        pd.plotting.register_matplotlib_converters()
        plt.figure(figsize=(10, 4))
        portfolio_color = '#28b4c8'
        plt.plot(historical_value, marker='o',
                 label=f'My Portfolio ({(historical_value[-1] - 1) * 100:+.2f}%)',
                 color=portfolio_color)
        if historical_start > 0:
            plt.plot([1] * (historical_start + 1), '--',
                     marker='o', color=portfolio_color)
        for symbol in market_symbols:
            if symbol not in market_values:
                continue
            symbol_value = market_values[symbol]
            normalized_value = np.array(symbol_value) / symbol_value[0]
            if len(historical_date) != len(normalized_value):
                continue
            plt.plot(normalized_value, marker='o',
                     label=f'{symbol} ({(normalized_value[-1] - 1) * 100:+.2f}%)',
                     color=color_map[symbol])
        text_kwargs = {'family': 'monospace'}
        plt.xticks(range(len(historical_date)), [date.strftime('%m-%d') for date in historical_date],
                   **text_kwargs)
        plt.xlabel('Date', **text_kwargs)
        plt.ylabel('Normalized Value', **text_kwargs)
        plt.yticks(fontname='monospace')
        plt.grid(linestyle='--', alpha=0.5)
        plt.legend(ncol=len(market_symbols) + 1, bbox_to_anchor=(0, 1),
                   loc='lower left', prop=text_kwargs)
        ax = plt.gca()
        ax.spines['right'].set_color('none')
        ax.spines['top'].set_color('none')
        buf = io.BytesIO()
        plt.savefig(buf, format='png')
        buf.seek(0)
        history_image = image.MIMEImage(buf.read())
        history_image.add_header(
            'Content-Disposition', 'attachment; filename=history.png')
        history_image.add_header('Content-ID', '<history>')

        html_template_path = os.path.join(_HTML_DIR, 'summary.html')
        with open(html_template_path, 'r') as f:
            html_template = f.read()
        message.attach(text.MIMEText(html_template.format(
            account_html=account_html, transactions_html=transactions_html,
            positions_html=positions_html), 'html'))
        message.attach(history_image)
        self._send_mail(message)

    def send_alert(self, error_message: Optional[str], title: str = 'An unexpected error was encountered'):
        if not self._client:
            self._logger.warning('Email client not created')
            return
        else:
            self._logger.info('Sending alert')
        message = self._create_message('Alert', title)
        html_template_path = os.path.join(_HTML_DIR, 'alert.html')
        with open(html_template_path, 'r') as f:
            html_template = f.read()
        error_time = pd.Timestamp(
            int(time.time()), unit='s', tz=_TIME_ZONE).strftime('%F %H:%M')
        error_message = error_message or ''
        error_message = html.escape(error_message)
        message.attach(text.MIMEText(html_template.format(
            error_time=error_time, error_message=error_message), 'html'))
        self._send_mail(message)

    @retrying.retry(stop_max_attempt_number=3, wait_exponential_multiplier=1000)
    def _send_mail(self, message):
        self._client.sendmail(
            self._sender, [self._receiver], message.as_string())
        self._client.close()


def main():
    parser = argparse.ArgumentParser(description='Alpharius email client.')
    parser.add_argument('-m', '--mode', help='Running mode. Can be summary or alert.',
                        required=True, choices=['summary', 'alert'])
    parser.add_argument('--error_message',
                        help='Error message. Only used in alert mode.')
    args = parser.parse_args()
    data_client = data.get_default_data_client()

    if args.mode == 'summary':
        EmailSender().send_summary(data_client)
    else:
        EmailSender().send_alert(args.error_message)


if __name__ == '__main__':
    main()
