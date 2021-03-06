from alpharius.common import *
from alpharius.stock_universe import IntradayVolatilityStockUniverse
from typing import List
import datetime
import numpy as np

NUM_UNIVERSE_SYMBOLS = 20
ENTRY_TIME = datetime.time(10, 0)
EXIT_TIME = datetime.time(16, 0)


class ZScoreProcessor(Processor):

    def __init__(self,
                 lookback_start_date: DATETIME_TYPE,
                 lookback_end_date: DATETIME_TYPE,
                 data_source: DataSource,
                 output_dir: str) -> None:
        super().__init__()
        self._positions = dict()
        self._stock_universe = IntradayVolatilityStockUniverse(lookback_start_date,
                                                               lookback_end_date,
                                                               data_source,
                                                               num_stocks=NUM_UNIVERSE_SYMBOLS)
        self._output_dir = output_dir
        self._logger = logging_config(os.path.join(self._output_dir, 'z_score_processor.txt'),
                                      detail=True,
                                      name='z_score_processor')

    def get_trading_frequency(self) -> TradingFrequency:
        return TradingFrequency.FIVE_MIN

    def get_stock_universe(self, view_time: DATETIME_TYPE) -> List[str]:
        return list(set(self._stock_universe.get_stock_universe(view_time) +
                        list(self._positions.keys())))

    def process_data(self, context: Context) -> Optional[Action]:
        if context.symbol in self._positions:
            return self._close_position(context)
        else:
            return self._open_position(context)

    def _open_position(self, context: Context) -> Optional[Action]:
        t = context.current_time.time()
        if t <= ENTRY_TIME or t >= EXIT_TIME:
            return
        market_open_ind = 0
        while context.intraday_lookback.index[market_open_ind].time() < datetime.time(9, 30):
            market_open_ind += 1
        intraday_closes = context.intraday_lookback['Close'][market_open_ind:]
        intraday_volumes = context.intraday_lookback['Volume'][market_open_ind:]
        if len(intraday_closes) < 6:
            return
        price_changes = [abs(intraday_closes[i] - intraday_closes[i - 1])
                         for i in range(1, len(intraday_closes))]
        z_price = (price_changes[-1] - np.mean(price_changes)) / (np.std(price_changes) + 1E-7)
        z_volume = (intraday_volumes[-1] - np.mean(intraday_volumes)) / (np.std(intraday_volumes) + 1E-7)
        is_trade = False
        is_trade = is_trade or (z_price > 3 and z_volume > 6 and intraday_closes[-1] > intraday_closes[-2])
        threshold = 2.5 if t < datetime.time(11, 0) else 3.5
        is_trade = is_trade or (z_price > threshold and intraday_closes[-1] < intraday_closes[-2])
        is_trade = is_trade and (context.current_price / context.prev_day_close < 2)
        if is_trade or (context.mode == Mode.TRADE and z_price > 1):
            self._logger.debug(f'[{context.current_time.strftime("%F %H:%M")}] [{context.symbol}] '
                               f'Price z-score: {z_price:.1f}. Volume z-score: {z_volume:.1f}. '
                               f'Current price {context.current_price}.')
        if not is_trade:
            return
        self._positions[context.symbol] = {'entry_time': context.current_time}
        return Action(context.symbol, ActionType.BUY_TO_OPEN, 1, context.current_price)

    def _close_position(self, context: Context) -> Optional[Action]:
        position = self._positions[context.symbol]
        if context.current_time < position['entry_time'] + datetime.timedelta(minutes=5):
            return
        self._positions.pop(context.symbol)
        return Action(context.symbol, ActionType.SELL_TO_CLOSE, 1, context.current_price)


class ZScoreProcessorFactory(ProcessorFactory):

    def __init__(self):
        super().__init__()

    def create(self,
               lookback_start_date: DATETIME_TYPE,
               lookback_end_date: DATETIME_TYPE,
               data_source: DataSource,
               output_dir: str,
               *args, **kwargs) -> ZScoreProcessor:
        return ZScoreProcessor(lookback_start_date, lookback_end_date, data_source, output_dir)
