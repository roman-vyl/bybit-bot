# backend/api/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.api.routes import candles, indicators
from backend.api.routes import config

app = FastAPI(title="Bybit Bot API")

# 👇 Разрешаем фронтенд доступ
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # ← разрешаем всё для разработки
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Роуты
app.include_router(candles.router, prefix="/candles", tags=["Candles"])
app.include_router(indicators.router, prefix="/indicators", tags=["Indicators"])
app.include_router(config.router)
