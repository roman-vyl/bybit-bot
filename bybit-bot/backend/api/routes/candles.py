# backend/api/routes/candles.py

from fastapi import APIRouter, Query
from backend.core.data.db_loader import (
    get_candles_from_db,
    get_candles_with_ema_from_db,
)
from fastapi.responses import JSONResponse
from typing import Optional, List
import traceback
from datetime import datetime


router = APIRouter()


@router.get("/candles")
def get_candles(
    symbol: str,
    timeframe: str,
    start: int,
    end: int,
    include_ema: bool = Query(False, description="Включить EMA данные"),
    ema_periods: str = Query("20,50,200", description="Периоды EMA через запятую"),
):
    try:
        # Парсим периоды EMA
        periods = (
            [p.strip() for p in ema_periods.split(",") if p.strip()]
            if include_ema
            else []
        )

        if include_ema and periods:
            # Загружаем свечи с EMA данными
            candles, ema_data = get_candles_with_ema_from_db(
                symbol, timeframe, start, end, include_ema=True, ema_periods=periods
            )
            ema_response = {timeframe: ema_data} if ema_data else {}
        else:
            # Загружаем только свечи
            candles = get_candles_from_db(symbol, timeframe, start, end)
            ema_response = {}

        # Логирование для анализа объема и диапазона данных
        print(
            f"📥 /candles → {symbol=}, {timeframe=}, {start=}, {end=}, {include_ema=}"
        )
        print(
            f"🕓 unix range → {datetime.utcfromtimestamp(start)} → {datetime.utcfromtimestamp(end)}"
        )
        print(f"📊 count: {len(candles)}")
        if include_ema:
            print(
                f"📈 EMA periods: {periods}, data keys: {list(ema_response.get(timeframe, {}).keys())}"
            )

        # Возвращаем структуру, ожидаемую фронтом
        return {"candles": candles, "ema": ema_response}
    except Exception as e:
        print("❌ Ошибка в get_candles endpoint:")
        traceback.print_exc()
        return JSONResponse(
            status_code=500, content={"error": f"Internal Server Error: {str(e)}"}
        )
