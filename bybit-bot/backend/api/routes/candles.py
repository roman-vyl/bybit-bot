# backend/api/routes/candles.py

from fastapi import APIRouter, Query
from backend.core.data.db_loader import get_candles_from_db
from fastapi.responses import JSONResponse
import traceback
from datetime import datetime


router = APIRouter()


@router.get("/")
def get_candles(symbol: str, timeframe: str, start: int, end: int):
    try:
        candles = get_candles_from_db(symbol, timeframe, start, end)

        # Логирование для анализа объема и диапазона данных
        print(f"📥 /candles → {symbol=}, {timeframe=}, {start=}, {end=}")
        print(
            f"🕓 unix range → {datetime.utcfromtimestamp(start)} → {datetime.utcfromtimestamp(end)}"
        )
        print(f"📊 count: {len(candles)}")

        return candles
    except Exception as e:
        print("❌ Ошибка в get_candles endpoint:")
        traceback.print_exc()
        return JSONResponse(
            status_code=500, content={"error": f"Internal Server Error: {str(e)}"}
        )
