"""
indicator_trigger.py

Минимальный триггер для пересчёта EMA по новой свече.
Просто вызывает calc_ema() на переданном timestamp.
"""

import logging
import sqlite3

from backend.core.indicators.calc_ema import (
    calc_ema,
    EMA_PERIODS,
    DB_PATH,
)

logger = logging.getLogger(__name__)


class IndicatorTrigger:
    def __init__(self):
        self.ema_periods = EMA_PERIODS
        logger.info(
            f"🚀 IndicatorTrigger инициализирован: EMA периоды = {self.ema_periods}"
        )

    def trigger_candle(self, candle: dict):
        """
        Пересчёт EMA для одной свечи.

        Args:
            candle: Словарь с данными свечи (symbol, interval, start)
        """
        # Извлекаем данные из словаря свечи
        symbol = candle["symbol"]
        timeframe = candle["interval"]
        ts = candle["start"] // 1000  # преобразовать ms → s

        logger.info(f"🚀 Пересчёт EMA для {symbol} {timeframe} @ {ts}")
        try:
            with sqlite3.connect(str(DB_PATH)) as conn:
                updated = calc_ema(symbol, timeframe, self.ema_periods, ts, ts, conn)
                if updated > 0:
                    logger.info(f"✅ EMA обновлено для {symbol} {timeframe} @ {ts}")
                else:
                    logger.info(f"ℹ️ EMA уже актуально для {symbol} {timeframe} @ {ts}")
        except Exception as e:
            logger.error(f"❌ Ошибка пересчёта EMA {symbol} {timeframe} @ {ts}: {e}")
            logger.exception("Детали ошибки:")
