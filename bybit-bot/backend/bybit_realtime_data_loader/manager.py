"""
manager.py

Основной координатор realtime-загрузчика данных.
Инициализирует WebSocket-клиент, обработчик свечей и триггеры расчётов.
"""

from backend.bybit_realtime_data_loader.ws_client import WSClient
from backend.bybit_realtime_data_loader.candle_handler import CandleHandler
from backend.bybit_realtime_data_loader.indicator_trigger import IndicatorTrigger


class Manager:
    def __init__(self, symbols, intervals):
        self.symbols = symbols
        self.intervals = intervals
        self.candle_handler = CandleHandler()
        self.indicator_trigger = IndicatorTrigger()

    def _on_candle(self, candle: dict):
        self.candle_handler.handle_candle(candle)
        self.indicator_trigger.trigger(
            symbol=candle["symbol"], timeframe=candle["interval"]
        )

    def run(self):
        ws = WSClient(
            symbols=self.symbols,
            intervals=self.intervals,
            callback=self._on_candle,
        )
        ws.run_forever()
