import abc
import functools
import inspect
import re
import os
from typing import Optional, List, Type, Union
from zoneinfo import ZoneInfo

import pandas as pd

from alpharius.data import DataClient
from ..common import logging_config, Context, ProcessorAction, Position, TradingFrequency, PositionStatus


class Processor(abc.ABC):

    def __init__(self, output_dir: str, logging_timezone: Optional[ZoneInfo] = None) -> None:
        split = re.findall('[A-Z][^A-Z]*', type(self).__name__)
        logger_name = '_'.join([s.lower() for s in split])
        self._output_dir = output_dir
        self._logger = logging_config(os.path.join(self._output_dir, logger_name + '.txt'),
                                      detail=True,
                                      name=logger_name,
                                      timezone=logging_timezone)
        self._positions = dict()

    @property
    def name(self) -> str:
        processor_name = type(self).__name__
        suffix = 'Processor'
        assert processor_name.endswith(suffix)
        return processor_name[:-len(suffix)]

    @abc.abstractmethod
    def get_stock_universe(self, view_time: pd.Timestamp) -> List[str]:
        raise NotImplementedError('Calling parent interface')

    def process_data(self, context: Context) -> Optional[ProcessorAction]:
        return None

    def process_all_data(self, contexts: List[Context]) -> List[ProcessorAction]:
        actions = []
        for context in contexts:
            action = self.process_data(context)
            if action:
                actions.append(action)
        return actions

    def setup(self, hold_positions: List[Position], current_time: Optional[pd.Timestamp]) -> None:
        return

    def teardown(self) -> None:
        return

    @abc.abstractmethod
    def get_trading_frequency(self) -> TradingFrequency:
        raise NotImplementedError('Calling parent interface')

    def ack(self, symbol: str) -> None:
        """Acknowledges the action is taken and updates position status."""
        if symbol in self._positions:
            self._positions[symbol]['status'] = PositionStatus.ACTIVE
            self._logger.debug('[%s] acked.', symbol)

    def is_active(self, symbol: str) -> bool:
        return symbol in self._positions and self._positions[symbol].get('status') == PositionStatus.ACTIVE


def instantiate_processor(
        processor_class: Union[Type[Processor], Processor],
        lookback_start_date: pd.Timestamp,
        lookback_end_date: pd.Timestamp,
        data_client: DataClient,
        output_dir: str,
        logging_timezone: Optional[ZoneInfo] = None,
        **kwargs) -> Processor:

    if isinstance(processor_class, Processor):
        return processor_class

    if isinstance(processor_class, functools.partial):
        signature = inspect.signature(processor_class.func.__init__)
        class_name = processor_class.func.__name__
        existing_keywords = processor_class.keywords
        parameters = [param for param in signature.parameters.values() if param.name not in existing_keywords]
    else:
        signature = inspect.signature(processor_class.__init__)
        class_name = processor_class.__name__
        parameters = signature.parameters.values()

    init_kwargs = {}
    for param in parameters:
        if param.name == 'lookback_start_date':
            init_kwargs[param.name] = lookback_start_date
        elif param.name == 'lookback_end_date':
            init_kwargs[param.name] = lookback_end_date
        elif param.name == 'data_client':
            init_kwargs[param.name] = data_client
        elif param.name == 'output_dir':
            init_kwargs[param.name] = output_dir
        elif param.name == 'logging_timezone':
            init_kwargs[param.name] = logging_timezone
        elif param.name in kwargs:
            init_kwargs[param.name] = kwargs[param.name]
        elif param.name != 'self':
            raise ValueError(f'Input parameter {param.name} not defined in {class_name}')
    return processor_class(**init_kwargs)
