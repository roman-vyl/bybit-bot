from fastapi import APIRouter, Query
from typing import List, Dict, Any, Union, Optional
from backend.core.data.db_loader import get_ema_data_multi_timeframe
from backend.config.timeframes_config import TIMEFRAMES_CONFIG
import logging
import traceback

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
) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    """
    Возвращает EMA данные для выбранных таймфреймов и периодов.

    Returns:
        {timeframe: {period: [{'time': timestamp, 'value': ema_value}]}}
    """

    logging.info(
        f"⚙️ get_ema_data: symbol={symbol}, timeframes={timeframes}, periods={periods}"
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


@router.get("/ema/all")
def get_all_emas(
    symbol: str,
    start: int,
    end: int,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    DEPRECATED: Возвращает EMA для всех таймфреймов из базы в старом формате.
    Используйте /ema вместо этого эндпоинта.
    """
    from backend.core.data.db_loader import get_candles_from_db

    logging.info(
        f"⚙️ get_all_emas (deprecated): symbol={symbol}, start={start}, end={end}"
    )

    results = {}

    for tf in TIMEFRAMES_CONFIG:
        try:
            candles = get_candles_from_db(symbol, tf, start, end)
            if not candles:
                continue

            ema_keys = [k for k in candles[0] if k.startswith("ema")]
            filtered = [{k: c[k] for k in ["timestamp"] + ema_keys} for c in candles]

            results[tf] = filtered
        except Exception as e:
            logging.error(f"❌ Ошибка при загрузке EMA для {tf}: {e}")
            traceback.print_exc()

    return results
