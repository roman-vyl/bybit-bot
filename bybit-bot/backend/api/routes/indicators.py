from fastapi import APIRouter
from typing import List, Dict, Any, Union
from backend.core.data.db_loader import get_candles_from_db
import logging

router = APIRouter()


@router.get("/ema")
def get_ema(
    symbol: str,
    timeframe: str,
    start: int,
    end: int,
) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    logging.info(
        f"⚙️ get_ema called with symbol={symbol}, timeframe={timeframe}, start={start}, end={end}"
    )

    candles = get_candles_from_db(symbol, timeframe, start, end)

    if not candles:
        return {"message": "Нет данных за указанный диапазон", "candles": []}

    return candles
