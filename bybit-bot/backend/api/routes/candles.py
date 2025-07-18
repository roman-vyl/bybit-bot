# backend/api/routes/candles.py

from fastapi import APIRouter, Query
from datetime import datetime
from backend.core.data.db_loader import get_candles_from_db

router = APIRouter()


@router.get("/")
def get_candles(
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
):
    print(f"[API] /candles: start={start}, end={end}")
    candles = get_candles_from_db(
        symbol=symbol,
        timeframe=timeframe,
        start=int(start.timestamp()),
        end=int(end.timestamp()),
    )
    return {"candles": candles}
