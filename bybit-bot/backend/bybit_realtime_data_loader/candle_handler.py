"""
Модуль для обработки и сохранения свечей в базу данных (таблицы candles_<interval> через sqlite), с обновлением по symbol+timestamp.
Участвует как обработчик и хранилище поступающих real-time свечей.
Использует: sqlite, данные свечей. Предоставляет: функции сохранения и обновления свечей в БД.
"""

import sqlite3
import logging
from typing import Dict
from backend.config.timeframes_config import TIMEFRAMES_CONFIG
from backend.core.dim.ezdim import EzDIM


logger = logging.getLogger(__name__)
DB_PATH = "db/market_data.sqlite"


class CandleHandler:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path

    def handle_candle(self, data: Dict):
        """
        Обрабатывает подтверждённую свечу от Bybit:
        сохраняет или обновляет в таблице candles_<timeframe>
        """
        try:
            symbol = data["symbol"]
            interval = data["interval"]
            timestamp = int(
                data["start"] // 1000
            )  # start of candle, приводим к секундам
            table = f"candles_{interval}"

            if interval not in TIMEFRAMES_CONFIG:
                logger.warning(f"⏭ Неизвестный интервал: {interval}")
                return

            # Создаем DataFrame для валидации
            import pandas as pd

            df = pd.DataFrame(
                [
                    {
                        "timestamp": timestamp,
                        "open": float(data["open"]),
                        "high": float(data["high"]),
                        "low": float(data["low"]),
                        "close": float(data["close"]),
                        "volume": float(data["volume"]),
                    }
                ]
            )

            # Валидация данных перед сохранением
            EzDIM.preflight(
                df,
                required_cols=["timestamp", "open", "high", "low", "close"],
                min_rows=1,
                tf_sec=TIMEFRAMES_CONFIG[interval]["interval_sec"],
            )

            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()

                query = f"""
                INSERT INTO {table} (symbol, timestamp, open, high, low, close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(symbol, timestamp) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume
                """

                cursor.execute(
                    query,
                    (
                        symbol,
                        timestamp,
                        float(data["open"]),
                        float(data["high"]),
                        float(data["low"]),
                        float(data["close"]),
                        float(data["volume"]),
                    ),
                )
                conn.commit()

            logger.info(f"💾 Обновлена свеча {symbol} {interval} @ {timestamp}")

        except Exception as e:
            logger.exception(f"Ошибка сохранения свечи: {e}")
