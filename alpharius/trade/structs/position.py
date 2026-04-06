from typing import NamedTuple

import pandas as pd


class Position(NamedTuple):
    symbol: str
    qty: int
    entry_price: float
    entry_time: pd.Timestamp | None
    entry_portion: float
