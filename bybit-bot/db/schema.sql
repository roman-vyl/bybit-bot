
-- 📊 Структура таблиц свечей с EMA для каждого таймфрейма
-- Каждая таблица включает стандартные OHLCV-поля + ema20, ema50, ema100, ema200, ema500

-- SQL-скрипт для обновления структуры таблиц candles_*
-- Добавляет колонку ema500 и удаляет ema9 из всех таблиц

-- Обновление таблицы candles_1m
BEGIN TRANSACTION;

-- Создаем временную таблицу с новой структурой
CREATE TABLE candles_1m_temp (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    timestamp_ns INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL,
    ema500 REAL
);

-- Копируем данные из старой таблицы (исключая ema9)
INSERT INTO candles_1m_temp (symbol, timestamp, timestamp_ns, open, high, low, close, volume, ema20, ema50, ema100, ema200)
SELECT symbol, timestamp, timestamp_ns, open, high, low, close, volume, ema20, ema50, ema100, ema200
FROM candles_1m;

-- Удаляем старую таблицу и переименовываем новую
DROP TABLE candles_1m;
ALTER TABLE candles_1m_temp RENAME TO candles_1m;

COMMIT;

-- Обновление таблицы candles_5m
BEGIN TRANSACTION;

CREATE TABLE candles_5m_temp (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    timestamp_ns INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL,
    ema500 REAL
);

INSERT INTO candles_5m_temp (symbol, timestamp, timestamp_ns, open, high, low, close, volume, ema20, ema50, ema100, ema200)
SELECT symbol, timestamp, timestamp_ns, open, high, low, close, volume, ema20, ema50, ema100, ema200
FROM candles_5m;

DROP TABLE candles_5m;
ALTER TABLE candles_5m_temp RENAME TO candles_5m;

COMMIT;

-- Обновление таблицы candles_30m
BEGIN TRANSACTION;

CREATE TABLE candles_30m_temp (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    timestamp_ns INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL,
    ema500 REAL
);

INSERT INTO candles_30m_temp (symbol, timestamp, timestamp_ns, open, high, low, close, volume, ema20, ema50, ema100, ema200)
SELECT symbol, timestamp, timestamp_ns, open, high, low, close, volume, ema20, ema50, ema100, ema200
FROM candles_30m;

DROP TABLE candles_30m;
ALTER TABLE candles_30m_temp RENAME TO candles_30m;

COMMIT;

-- Обновление таблицы candles_1h
BEGIN TRANSACTION;

CREATE TABLE candles_1h_temp (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    timestamp_ns INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL,
    ema500 REAL
);

INSERT INTO candles_1h_temp (symbol, timestamp, timestamp_ns, open, high, low, close, volume, ema20, ema50, ema100, ema200)
SELECT symbol, timestamp, timestamp_ns, open, high, low, close, volume, ema20, ema50, ema100, ema200
FROM candles_1h;

DROP TABLE candles_1h;
ALTER TABLE candles_1h_temp RENAME TO candles_1h;

COMMIT;

-- Обновление таблицы candles_4h
BEGIN TRANSACTION;

CREATE TABLE candles_4h_temp (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    timestamp_ns INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL,
    ema500 REAL
);

INSERT INTO candles_4h_temp (symbol, timestamp, timestamp_ns, open, high, low, close, volume, ema20, ema50, ema100, ema200)
SELECT symbol, timestamp, timestamp_ns, open, high, low, close, volume, ema20, ema50, ema100, ema200
FROM candles_4h;

DROP TABLE candles_4h;
ALTER TABLE candles_4h_temp RENAME TO candles_4h;

COMMIT;

-- Обновление таблицы candles_6h
BEGIN TRANSACTION;

CREATE TABLE candles_6h_temp (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    timestamp_ns INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL,
    ema500 REAL
);

INSERT INTO candles_6h_temp (symbol, timestamp, timestamp_ns, open, high, low, close, volume, ema20, ema50, ema100, ema200)
SELECT symbol, timestamp, timestamp_ns, open, high, low, close, volume, ema20, ema50, ema100, ema200
FROM candles_6h;

DROP TABLE candles_6h;
ALTER TABLE candles_6h_temp RENAME TO candles_6h;

COMMIT;

-- Обновление таблицы candles_12h
BEGIN TRANSACTION;

CREATE TABLE candles_12h_temp (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    timestamp_ns INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL,
    ema500 REAL
);

INSERT INTO candles_12h_temp (symbol, timestamp, timestamp_ns, open, high, low, close, volume, ema20, ema50, ema100, ema200)
SELECT symbol, timestamp, timestamp_ns, open, high, low, close, volume, ema20, ema50, ema100, ema200
FROM candles_12h;

DROP TABLE candles_12h;
ALTER TABLE candles_12h_temp RENAME TO candles_12h;

COMMIT;

-- Обновление таблицы candles_1d
BEGIN TRANSACTION;

CREATE TABLE candles_1d_temp (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    timestamp_ns INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL,
    ema500 REAL
);

INSERT INTO candles_1d_temp (symbol, timestamp, timestamp_ns, open, high, low, close, volume, ema20, ema50, ema100, ema200)
SELECT symbol, timestamp, timestamp_ns, open, high, low, close, volume, ema20, ema50, ema100, ema200
FROM candles_1d;

DROP TABLE candles_1d;
ALTER TABLE candles_1d_temp RENAME TO candles_1d;

COMMIT;

-- Обновление таблицы candles_1w
BEGIN TRANSACTION;

CREATE TABLE candles_1w_temp (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    timestamp_ns INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL,
    ema500 REAL
);

INSERT INTO candles_1w_temp (symbol, timestamp, timestamp_ns, open, high, low, close, volume, ema20, ema50, ema100, ema200)
SELECT symbol, timestamp, timestamp_ns, open, high, low, close, volume, ema20, ema50, ema100, ema200
FROM candles_1w;

DROP TABLE candles_1w;
ALTER TABLE candles_1w_temp RENAME TO candles_1w;

COMMIT;

-- Альтернативный вариант для новых таблиц (если нужно создать с нуля)
-- Раскомментируйте, если нужно создать таблицы заново:

/*
CREATE TABLE IF NOT EXISTS candles_1m (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    timestamp_ns INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL,
    ema500 REAL
);

CREATE TABLE IF NOT EXISTS candles_5m (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    timestamp_ns INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL,
    ema500 REAL
);

CREATE TABLE IF NOT EXISTS candles_30m (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    timestamp_ns INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL,
    ema500 REAL
);

CREATE TABLE IF NOT EXISTS candles_1h (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    timestamp_ns INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL,
    ema500 REAL
);

CREATE TABLE IF NOT EXISTS candles_4h (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    timestamp_ns INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL,
    ema500 REAL
);

CREATE TABLE IF NOT EXISTS candles_6h (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    timestamp_ns INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL,
    ema500 REAL
);

CREATE TABLE IF NOT EXISTS candles_12h (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    timestamp_ns INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL,
    ema500 REAL
);

CREATE TABLE IF NOT EXISTS candles_1d (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    timestamp_ns INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL,
    ema500 REAL
);

CREATE TABLE IF NOT EXISTS candles_1w (
    symbol TEXT NOT NULL,
    timestamp INTEGER PRIMARY KEY,
    timestamp_ns INTEGER NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    volume REAL,
    ema20 REAL,
    ema50 REAL,
    ema100 REAL,
    ema200 REAL,
    ema500 REAL
);
*/
