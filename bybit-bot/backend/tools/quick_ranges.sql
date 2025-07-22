-- quick_ranges.sql
-- SQL-скрипт для быстрого просмотра диапазонов данных во всех таблицах свечей
-- 
-- Использование:
-- sqlite3 db/market_data.sqlite < backend/tools/quick_ranges.sql

.echo on
.headers on
.mode column

SELECT '===== ОБЗОР ВСЕХ ТАБЛИЦ СВЕЧЕЙ =====' as title;

-- Показать общую информацию по всем таблицам
WITH table_stats AS (
    SELECT 
        'candles_1m' as table_name,
        COUNT(*) as records,
        MIN(timestamp) as earliest_ts,
        MAX(timestamp) as latest_ts,
        datetime(MIN(timestamp), 'unixepoch') as earliest_date,
        datetime(MAX(timestamp), 'unixepoch') as latest_date
    FROM candles_1m 
    WHERE timestamp > 0
    
    UNION ALL
    
    SELECT 
        'candles_5m' as table_name,
        COUNT(*) as records,
        MIN(timestamp) as earliest_ts,
        MAX(timestamp) as latest_ts,
        datetime(MIN(timestamp), 'unixepoch') as earliest_date,
        datetime(MAX(timestamp), 'unixepoch') as latest_date
    FROM candles_5m 
    WHERE timestamp > 0
    
    UNION ALL
    
    SELECT 
        'candles_30m' as table_name,
        COUNT(*) as records,
        MIN(timestamp) as earliest_ts,
        MAX(timestamp) as latest_ts,
        datetime(MIN(timestamp), 'unixepoch') as earliest_date,
        datetime(MAX(timestamp), 'unixepoch') as latest_date
    FROM candles_30m 
    WHERE timestamp > 0
    
    UNION ALL
    
    SELECT 
        'candles_1h' as table_name,
        COUNT(*) as records,
        MIN(timestamp) as earliest_ts,
        MAX(timestamp) as latest_ts,
        datetime(MIN(timestamp), 'unixepoch') as earliest_date,
        datetime(MAX(timestamp), 'unixepoch') as latest_date
    FROM candles_1h 
    WHERE timestamp > 0
    
    UNION ALL
    
    SELECT 
        'candles_4h' as table_name,
        COUNT(*) as records,
        MIN(timestamp) as earliest_ts,
        MAX(timestamp) as latest_ts,
        datetime(MIN(timestamp), 'unixepoch') as earliest_date,
        datetime(MAX(timestamp), 'unixepoch') as latest_date
    FROM candles_4h 
    WHERE timestamp > 0
    
    UNION ALL
    
    SELECT 
        'candles_6h' as table_name,
        COUNT(*) as records,
        MIN(timestamp) as earliest_ts,
        MAX(timestamp) as latest_ts,
        datetime(MIN(timestamp), 'unixepoch') as earliest_date,
        datetime(MAX(timestamp), 'unixepoch') as latest_date
    FROM candles_6h 
    WHERE timestamp > 0
    
    UNION ALL
    
    SELECT 
        'candles_12h' as table_name,
        COUNT(*) as records,
        MIN(timestamp) as earliest_ts,
        MAX(timestamp) as latest_ts,
        datetime(MIN(timestamp), 'unixepoch') as earliest_date,
        datetime(MAX(timestamp), 'unixepoch') as latest_date
    FROM candles_12h 
    WHERE timestamp > 0
    
    UNION ALL
    
    SELECT 
        'candles_1d' as table_name,
        COUNT(*) as records,
        MIN(timestamp) as earliest_ts,
        MAX(timestamp) as latest_ts,
        datetime(MIN(timestamp), 'unixepoch') as earliest_date,
        datetime(MAX(timestamp), 'unixepoch') as latest_date
    FROM candles_1d 
    WHERE timestamp > 0
    
    UNION ALL
    
    SELECT 
        'candles_1w' as table_name,
        COUNT(*) as records,
        MIN(timestamp) as earliest_ts,
        MAX(timestamp) as latest_ts,
        datetime(MIN(timestamp), 'unixepoch') as earliest_date,
        datetime(MAX(timestamp), 'unixepoch') as latest_date
    FROM candles_1w 
    WHERE timestamp > 0
)
SELECT 
    table_name as "Таблица",
    records as "Записей",
    earliest_date as "Начало данных",
    latest_date as "Конец данных",
    ROUND((latest_ts - earliest_ts) / 86400.0, 1) as "Дней покрытия"
FROM table_stats
WHERE records > 0
ORDER BY table_name;

SELECT '';
SELECT '===== ДЕТАЛИ ПО КАЖДОЙ ТАБЛИЦЕ =====';

-- Детальная информация с символами
SELECT '';
SELECT '--- CANDLES_1M ---';
SELECT 
    symbol as "Символ",
    COUNT(*) as "Записей",
    datetime(MIN(timestamp), 'unixepoch') as "Начало",
    datetime(MAX(timestamp), 'unixepoch') as "Конец",
    ROUND((MAX(timestamp) - MIN(timestamp)) / 86400.0, 1) as "Дней"
FROM candles_1m 
WHERE timestamp > 0
GROUP BY symbol;

SELECT '';
SELECT '--- CANDLES_5M ---';
SELECT 
    symbol as "Символ",
    COUNT(*) as "Записей", 
    datetime(MIN(timestamp), 'unixepoch') as "Начало",
    datetime(MAX(timestamp), 'unixepoch') as "Конец",
    ROUND((MAX(timestamp) - MIN(timestamp)) / 86400.0, 1) as "Дней"
FROM candles_5m 
WHERE timestamp > 0
GROUP BY symbol;

SELECT '';
SELECT '--- CANDLES_30M ---';
SELECT 
    symbol as "Символ",
    COUNT(*) as "Записей",
    datetime(MIN(timestamp), 'unixepoch') as "Начало", 
    datetime(MAX(timestamp), 'unixepoch') as "Конец",
    ROUND((MAX(timestamp) - MIN(timestamp)) / 86400.0, 1) as "Дней"
FROM candles_30m 
WHERE timestamp > 0
GROUP BY symbol;

SELECT '';
SELECT '--- CANDLES_1H ---';
SELECT 
    symbol as "Символ",
    COUNT(*) as "Записей",
    datetime(MIN(timestamp), 'unixepoch') as "Начало",
    datetime(MAX(timestamp), 'unixepoch') as "Конец", 
    ROUND((MAX(timestamp) - MIN(timestamp)) / 86400.0, 1) as "Дней"
FROM candles_1h 
WHERE timestamp > 0
GROUP BY symbol;

SELECT '';
SELECT '--- CANDLES_4H ---';
SELECT 
    symbol as "Символ",
    COUNT(*) as "Записей",
    datetime(MIN(timestamp), 'unixepoch') as "Начало",
    datetime(MAX(timestamp), 'unixepoch') as "Конец",
    ROUND((MAX(timestamp) - MIN(timestamp)) / 86400.0, 1) as "Дней"
FROM candles_4h 
WHERE timestamp > 0
GROUP BY symbol;

SELECT '';
SELECT '--- CANDLES_6H ---';
SELECT 
    symbol as "Символ", 
    COUNT(*) as "Записей",
    datetime(MIN(timestamp), 'unixepoch') as "Начало",
    datetime(MAX(timestamp), 'unixepoch') as "Конец",
    ROUND((MAX(timestamp) - MIN(timestamp)) / 86400.0, 1) as "Дней"
FROM candles_6h 
WHERE timestamp > 0
GROUP BY symbol;

SELECT '';
SELECT '--- CANDLES_12H ---';
SELECT 
    symbol as "Символ",
    COUNT(*) as "Записей",
    datetime(MIN(timestamp), 'unixepoch') as "Начало", 
    datetime(MAX(timestamp), 'unixepoch') as "Конец",
    ROUND((MAX(timestamp) - MIN(timestamp)) / 86400.0, 1) as "Дней"
FROM candles_12h 
WHERE timestamp > 0
GROUP BY symbol;

SELECT '';
SELECT '--- CANDLES_1D ---'; 
SELECT 
    symbol as "Символ",
    COUNT(*) as "Записей",
    datetime(MIN(timestamp), 'unixepoch') as "Начало",
    datetime(MAX(timestamp), 'unixepoch') as "Конец",
    ROUND((MAX(timestamp) - MIN(timestamp)) / 86400.0, 1) as "Дней"
FROM candles_1d 
WHERE timestamp > 0
GROUP BY symbol;

SELECT '';
SELECT '--- CANDLES_1W ---';
SELECT 
    symbol as "Символ",
    COUNT(*) as "Записей", 
    datetime(MIN(timestamp), 'unixepoch') as "Начало",
    datetime(MAX(timestamp), 'unixepoch') as "Конец",
    ROUND((MAX(timestamp) - MIN(timestamp)) / 86400.0, 1) as "Дней"
FROM candles_1w 
WHERE timestamp > 0
GROUP BY symbol; 