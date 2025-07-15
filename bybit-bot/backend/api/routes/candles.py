# backend/api/routes/candles.py

from fastapi import APIRouter, Query
from datetime import datetime
from backend.core.data._db_loader_rabotaet_s_zazorami import get_candles_from_db

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
        symbol=symbol, timeframe=timeframe, start=start.isoformat(), end=end.isoformat()
    )
    return {"candles": candles}
