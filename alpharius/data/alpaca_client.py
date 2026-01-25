import os
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import retrying
from alpaca.data import (
    Adjustment,
    StockBarsRequest,
    StockLatestTradeRequest,
    StockHistoricalDataClient,
    TimeFrame,
    TimeFrameUnit,
)

from alpharius.utils import TIME_ZONE, ALPACA_API_KEY_ENV, ALPACA_SECRET_KEY_ENV
from .base import DATA_COLUMNS, DataClient, TimeInterval


class AlpacaClient(DataClient):

    def __init__(self,
                 api_key: Optional[str] = None,
                 secret_key: Optional[str] = None) -> None:
        """Instantiates an Alpaca Data Client.

        Parameters:
            api_key: Alpaca API key.
            secret_key: Alpaca API secret key.
        """
        api_key = api_key or os.environ[ALPACA_API_KEY_ENV]
        secret_key = secret_key or os.environ[ALPACA_SECRET_KEY_ENV]
        self._client = StockHistoricalDataClient(api_key=api_key, secret_key=secret_key)

    @retrying.retry(stop_max_attempt_number=3, wait_exponential_multiplier=500)
    def get_data(self,
                 symbol: str,
                 start_time: pd.Timestamp,
                 end_time: pd.Timestamp,
                 time_interval: TimeInterval) -> pd.DataFrame:
        """Loads data with specified start and end time.

        start_time and end_time are inclusive.
        """
        if not start_time.tzinfo:
            start_time = start_time.tz_localize(TIME_ZONE)
        if not end_time.tzinfo:
            end_time = end_time.tz_localize(TIME_ZONE)
        if time_interval == TimeInterval.FIVE_MIN:
            timeframe = TimeFrame(5, TimeFrameUnit.Minute)
        elif time_interval == TimeInterval.HOUR:
            timeframe = TimeFrame(1, TimeFrameUnit.Hour)
        elif time_interval == TimeInterval.DAY:
            timeframe = TimeFrame(1, TimeFrameUnit.Day)
        else:
            raise ValueError(f'time_interval {time_interval} not supported')
        request = StockBarsRequest(
            symbol_or_symbols=symbol,
            start=start_time.to_pydatetime(),
            end=end_time.to_pydatetime(),
            timeframe=timeframe,
            adjustment=Adjustment.SPLIT,
        )
        try:
            bars = self._client.get_stock_bars(request).data[symbol]
            bars.sort(key=lambda b: b.timestamp)
        except AttributeError:
            bars = []
        index = pd.DatetimeIndex([pd.Timestamp(b.timestamp).tz_convert(TIME_ZONE) for b in bars])
        data = [[np.float32(b.open), np.float32(b.high), np.float32(b.low), np.float32(b.close), np.uint32(b.volume)]
                for b in bars]
        return pd.DataFrame(data, index=index, columns=DATA_COLUMNS)

    @retrying.retry(stop_max_attempt_number=3, wait_exponential_multiplier=500)
    def get_last_trades(self, symbols: List[str]) -> Dict[str, float]:
        """Gets the last trade prices of a list of symbols."""
        request = StockLatestTradeRequest(symbol_or_symbols=symbols)
        trades = self._client.get_stock_latest_trade(request)
        return {symbol: t.price for symbol, t in trades.items()}
