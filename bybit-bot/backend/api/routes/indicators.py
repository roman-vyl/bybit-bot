from fastapi import APIRouter
from typing import List, Dict, Any, Union
from backend.core.data._db_loader_rabotaet_s_zazorami import get_candles_from_db
import pandas as pd
import logging
import os
import orjson

router = APIRouter()


@router.get("/ema")
def get_ema(
    symbol: str,
    timeframe: str,
    start: str,
    end: str,
) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
    logging.info(
        f"⚙️ get_ema called with symbol={symbol}, timeframe={timeframe}, start={start}, end={end}"
    )

    candles = get_candles_from_db(symbol, timeframe, start, end)

    if not candles:
        return {"message": "Нет данных за указанный диапазон", "candles": []}

    df = pd.DataFrame(candles)

    if "close" not in df.columns:
        return {"error": "'close' колонка не найдена в данных"}

    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"])  # 🧹 Убираем строки без close

    ema_path = os.path.join("backend", "config", "ema_periods.txt")
    with open(ema_path, "r") as f:
        ema_periods = [int(line.strip()) for line in f if line.strip().isdigit()]

    for period in ema_periods:
        df[f"ema_{period}"] = df["close"].ewm(span=period, adjust=False).mean()

    df.replace([float("inf"), float("-inf")], None, inplace=True)
    df = df.where(pd.notna(df), None)

    # 🧹 Удаляем строки, где все EMA пустые
    ema_cols = [f"ema_{period}" for period in ema_periods]
    df = df.dropna(subset=ema_cols, how="all")

    return orjson.loads(orjson.dumps(df.to_dict(orient="records")))
