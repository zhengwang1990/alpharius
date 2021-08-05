import argparse
import os
import mplfinance as fplt
import pandas as pd

CACHE_ROOT = os.path.join(os.path.dirname(os.path.dirname(os.path.realpath(__file__))), 'cache')

def vwap(df):
    res = []
    vwaps = df['VWAP']
    volumes = df['Volume']
    total_dolloar = 0
    total_volume = 0
    for i in range(len(df)):
        total_dolloar += vwaps[i] * volumes[i]
        total_volume += volumes[i]
        res.append((df.index[i], total_dolloar / total_volume))
    return res


def plot(date, symbol):
    file_path = os.path.join(CACHE_ROOT, 'FIVE_MIN', date, f'history_{symbol}.csv')
    df = pd.read_csv(file_path, index_col=0, parse_dates=True)
    fplt.plot(df, type='candle', style='charles', title=f'{symbol} on {date}', figsize=(20, 9),
              alines=vwap(df), volume=True)


def main():
    parser = argparse.ArgumentParser(description='Plot candles.')
    parser.add_argument('--date', default=None)
    parser.add_argument('--symbol', default=None)
    args = parser.parse_args()

    if args.date is None or args.symbol is None:
        raise ValueError('Flag date and symbol must be specified')

    plot(args.date, args.symbol)


if __name__ == '__main__':
    main()
