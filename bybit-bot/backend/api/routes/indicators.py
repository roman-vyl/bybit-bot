from fastapi import APIRouter, Query
from typing import List, Dict, Any, Union, Optional
from backend.core.data.db_loader import get_ema_data_multi_timeframe
from backend.config.timeframes_config import TIMEFRAMES_CONFIG
import logging
import traceback
import sqlite3
from pathlib import Path
import os

# Путь до корня проекта
BASE_DIR = Path(__file__).resolve().parents[3]
PROJECT_ROOT = BASE_DIR.parent
DB_PATH = Path(
    os.getenv("DB_PATH", PROJECT_ROOT / "db" / "market_data.sqlite")
).resolve()

router = APIRouter()


@router.get("/ema")
def get_ema_data(
    symbol: str,
    start: int,
    end: int,
    timeframes: str = Query(
        None, description="Таймфреймы через запятую (по умолчанию все)"
    ),
    periods: str = Query("20,50,100,200,500", description="Периоды EMA через запятую"),
    align_to: Optional[str] = Query(
        None, description="Подогнать EMA к таймфрейму (например, 1m)"
    ),
) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """
    Возвращает EMA данные для выбранных таймфреймов и периодов.

    Returns:
        {timeframe: {period: [{'time': timestamp, 'value': ema_value}]}}
    """

    logging.info(
        f"⚙️ get_ema_data: symbol={symbol}, timeframes={timeframes}, periods={periods}, align_to={align_to}"
    )

    # Парсим таймфреймы
    if timeframes:
        tf_list = [
            tf.strip()
            for tf in timeframes.split(",")
            if tf.strip() and tf.strip() in TIMEFRAMES_CONFIG
        ]
    else:
        tf_list = list(TIMEFRAMES_CONFIG.keys())

    # Парсим периоды
    period_list = [p.strip() for p in periods.split(",") if p.strip()]

    if not tf_list or not period_list:
        return {}

    try:
        results = get_ema_data_multi_timeframe(symbol, tf_list, start, end, period_list)

        # --- Фильтрация по align_to ---
        if align_to and align_to in TIMEFRAMES_CONFIG:
            # Получаем все timestamps из таблицы candles_<align_to> для symbol, start, end
            table = f"candles_{align_to}"
            base_timestamps = set()
            try:
                with sqlite3.connect(DB_PATH) as conn:
                    cursor = conn.cursor()
                    cursor.execute(
                        f"SELECT timestamp FROM {table} WHERE symbol = ? AND timestamp BETWEEN ? AND ? ORDER BY timestamp",
                        (symbol, start, end),
                    )
                    base_timestamps = set(row[0] for row in cursor.fetchall())
            except Exception as e:
                logging.error(
                    f"Ошибка при получении timestamps для align_to={align_to}: {e}"
                )
                base_timestamps = set()

            # Фильтруем результаты EMA по base_timestamps
            for tf in results:
                for period in results[tf]:
                    results[tf][period] = [
                        p for p in results[tf][period] if p["time"] in base_timestamps
                    ]

        # Логирование результата
        for tf, periods_data in results.items():
            print(
                f"📈 {tf}: {list(periods_data.keys())} periods, {sum(len(data) for data in periods_data.values())} points"
            )

        return results

    except Exception as e:
        logging.error(f"❌ Ошибка при загрузке EMA данных: {e}")
        traceback.print_exc()
        return {}
