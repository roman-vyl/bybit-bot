from fastapi import APIRouter
from typing import List, Dict, Any, Union
from backend.core.data.db_loader import get_candles_from_db
import logging

router = APIRouter()


@router.get("/ema")
def get_all_emas(
    symbol: str,
    start: int,
    end: int,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Возвращает EMA для всех таймфреймов из базы.
    """
    from backend.config.timeframes_config import TIMEFRAMES_CONFIG
    import traceback
    import logging

    logging.info(f"⚙️ get_all_emas: symbol={symbol}, start={start}, end={end}")

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
