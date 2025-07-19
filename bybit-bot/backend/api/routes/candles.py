# backend/api/routes/candles.py

from fastapi import APIRouter, Query
from backend.core.data.db_loader import get_candles_from_db
from fastapi.responses import JSONResponse
import traceback


router = APIRouter()


@router.get("/")
def get_candles(symbol: str, timeframe: str, start: int, end: int):
    try:
        candles = get_candles_from_db(symbol, timeframe, start, end)
        return candles
    except Exception as e:
        print("❌ Ошибка в get_candles endpoint:")
        traceback.print_exc()
        return JSONResponse(
            status_code=500, content={"error": f"Internal Server Error: {str(e)}"}
        )
