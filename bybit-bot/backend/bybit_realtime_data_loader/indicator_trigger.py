"""
indicator_trigger.py

Модуль для запуска расчётов EMA и RSI по новой закрытой свече.
Вызывается после записи свечи в БД.
"""

import logging
from backend.core.indicators.calculate_ema_combined import run_ema_incremental
from backend.config.timeframes_config import TIMEFRAMES_CONFIG

logger = logging.getLogger(__name__)


class IndicatorTrigger:
    def __init__(self):
        pass

    def trigger(self, symbol: str, timeframe: str):
        """
        Запускает инкрементальный расчёт EMA и RSI по указанному символу и таймфрейму.
        :param symbol: Например, "BTCUSDT"
        :param timeframe: Например, "1m", "5m", "1h" — в формате TIMEFRAMES_CONFIG
        """
        if timeframe not in TIMEFRAMES_CONFIG:
            logger.warning(f"❌ Таймфрейм '{timeframe}' не поддерживается.")
            return

        try:
            logger.info(f"📊 Расчёт EMA/RSI: {symbol} {timeframe}")
            run_ema_incremental(symbol=symbol, timeframe=timeframe)
        except Exception as e:
            logger.exception(f"Ошибка при расчёте индикаторов: {e}")
