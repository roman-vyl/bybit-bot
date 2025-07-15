from fastapi import APIRouter
from backend.config.timeframes_config import TIMEFRAMES_CONFIG
import os

router = APIRouter()


@router.get("/config")
def get_config():
    ema_path = os.path.join("backend", "config", "ema_periods.txt")

    with open(ema_path, "r") as f:
        ema_periods = [int(line.strip()) for line in f if line.strip().isdigit()]

    return {
        "timeframes": list(TIMEFRAMES_CONFIG.keys()),
        "ema_periods": ema_periods,
    }
