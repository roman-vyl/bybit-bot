"""
ws_client.py

Подключение к WebSocket Bybit и обработка real-time свечей.
Фильтрует только закрытые свечи (confirm: true) и вызывает callback.
Поддерживает как одиночные свечи (data: {}) так и массивы (data: []) — например, при snapshot.
"""

from typing import Callable, List
from pybit.unified_trading import WebSocket
import time
import logging

logger = logging.getLogger(__name__)


def normalize_interval(raw_interval: str) -> str:
    """
    Преобразует WebSocket-таймфрейм Bybit в ключ из TIMEFRAMES_CONFIG.
    Примеры:
        "1"   -> "1m"
        "5"   -> "5m"
        "60"  -> "1h"
        "240" -> "4h"
        "360" -> "6h"
        "720" -> "12h"
        "D"   -> "1d"
        "W"   -> "1w"
    """
    mapping = {
        "1": "1m",
        "5": "5m",
        "30": "30m",
        "60": "1h",
        "240": "4h",
        "360": "6h",
        "720": "12h",
        "D": "1d",
        "W": "1w",
    }
    return mapping.get(str(raw_interval), str(raw_interval))


class WSClient:
    def __init__(
        self,
        symbols: List[str],
        intervals: List[str],
        callback: Callable[[dict], None],
        testnet: bool = False,
    ):
        self.symbols = symbols
        self.intervals = intervals
        self.callback = callback
        self.testnet = testnet
        self.ws = None

    def connect(self):
        logger.info("⏳ Подключаемся к WebSocket Bybit...")
        self.ws = WebSocket(
            testnet=self.testnet,
            channel_type="linear",  # для фьючерсов
        )

        for symbol in self.symbols:
            for interval in self.intervals:
                logger.info(f"📡 Подписка: kline.{interval}.{symbol}")
                self.ws.kline_stream(
                    symbol=symbol,
                    interval=interval,
                    callback=self._on_message,
                )

    def _on_message(self, message: dict):
        logger.debug(f"\ud83d\udcec Входящее сообщение от WS: {message}")
        try:
            topic = message.get("topic", "")
            data = message.get("data")

            # Парсим interval и symbol из topic (например, kline.1.BTCUSDT)
            interval = symbol = None
            if topic.startswith("kline."):
                parts = topic.split(".")
                if len(parts) == 3:
                    _, raw_interval, symbol = parts
                    interval = normalize_interval(raw_interval)

            if not data:
                return

            if isinstance(data, list):
                for candle in data:
                    if interval is not None:
                        candle["interval"] = interval
                    if symbol is not None:
                        candle["symbol"] = symbol
                    if candle.get("confirm"):
                        logger.debug(f"✅ [list] Закрытая свеча: {topic}")
                        self.callback(candle)
                        print("🟢 Получена и обработана свеча.")
            elif isinstance(data, dict):
                if interval is not None:
                    data["interval"] = interval
                if symbol is not None:
                    data["symbol"] = symbol
                if data.get("confirm"):
                    logger.debug(f"✅ [dict] Закрытая свеча: {topic}")
                    self.callback(data)
                    print("🟢 Получена и обработана свеча.")

        except Exception as e:
            logger.exception(f"Ошибка обработки сообщения: {e}")

    def run_forever(self):
        self.connect()
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("⏹ Остановлено пользователем")
