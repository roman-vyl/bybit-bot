# backend/api/main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.api.routes import candles, indicators
from backend.api.routes import config
from fastapi.responses import ORJSONResponse

app = FastAPI(title="Bybit Bot API", default_response_class=ORJSONResponse)


# 👇 Разрешаем фронтенд доступ
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # ← разрешаем всё для разработки
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Роуты
app.include_router(candles.router, tags=["Candles"])
app.include_router(indicators.router, prefix="/indicators", tags=["Indicators"])
app.include_router(config.router)
