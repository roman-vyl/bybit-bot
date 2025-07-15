
-- 📊 Структура таблиц свечей с EMA для каждого таймфрейма
-- Каждая таблица включает стандартные OHLCV-поля + ema9, ema20, ema50, ema100, ema200

CREATE TABLE IF NOT EXISTS candles_1m (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema9 REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL
);

CREATE TABLE IF NOT EXISTS candles_5m (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema9 REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL
);

CREATE TABLE IF NOT EXISTS candles_30m (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema9 REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL
);

CREATE TABLE IF NOT EXISTS candles_1h (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema9 REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL
);

CREATE TABLE IF NOT EXISTS candles_4h (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema9 REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL
);

CREATE TABLE IF NOT EXISTS candles_6h (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema9 REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL
);

CREATE TABLE IF NOT EXISTS candles_12h (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema9 REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL
);

CREATE TABLE IF NOT EXISTS candles_1d (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema9 REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL
);

CREATE TABLE IF NOT EXISTS candles_1w (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema9 REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL
);
