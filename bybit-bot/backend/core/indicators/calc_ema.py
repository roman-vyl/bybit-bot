"""
Универсальный расчёт EMA (Exponential Moving Average)

Единая точка входа: calc_ema(...)
Вызовы извне (sync_all, indicator_trigger, realtime, EzDIM) идут только сюда.

Логика:
1. Загружаем свечи + контекст
2. Валидируем (validate_for_indicator)
   - если ок → считаем EMA step-by-step
   - если не ок → ставим -1
3. Записываем в БД (NaN для первых n-1 свечей, значения или -1 далее)
4. Запускаем postflight
   - если всё ок → конец
   - если есть -1 или дыры → запускаем find_and_fix_gaps, который снова вызовет calc_ema
"""

import sqlite3
import pandas as pd
import pandas_ta as ta
from pathlib import Path
from typing import List

from backend.config.timeframes_config import TIMEFRAMES_CONFIG
from backend.core.validation.data_integrity import validate_for_indicator
from backend.core.dim.ezdim import EzDIM

PROJECT_ROOT = Path(__file__).resolve().parents[3]
DB_PATH = PROJECT_ROOT / "db" / "market_data.sqlite"
EMA_FILE = PROJECT_ROOT / "backend" / "config" / "ema_periods.txt"

# Загружаем список EMA периодов
with EMA_FILE.open("r") as f:
    EMA_PERIODS = [int(line.strip()) for line in f if line.strip().isdigit()]
if not EMA_PERIODS:
    raise ValueError("Список EMA-периодов пуст")


def calc_ema(
    symbol: str,
    timeframe: str,
    ema_periods: List[int],
    start_ts: int,
    end_ts: int,
    conn: sqlite3.Connection,
) -> int:
    """
    Универсальный пересчёт EMA step-by-step.
    """

    table = f"candles_{timeframe}"
    tf_sec = TIMEFRAMES_CONFIG[timeframe]["interval_sec"]
    max_period = max(ema_periods)

    # Загружаем свечи + контекст
    gap_len = int((end_ts - start_ts) / tf_sec) + 1
    context_start_ts = start_ts - (max_period + gap_len) * tf_sec

    df = pd.read_sql_query(
        f"""
        SELECT * FROM {table}
        WHERE symbol = ? AND timestamp >= ? AND timestamp <= ?
        ORDER BY timestamp
        """,
        conn,
        params=(symbol, context_start_ts, end_ts),
    )

    if df.empty:
        print(f"[calc_ema] ❌ Нет данных для {symbol} {timeframe}")
        return 0

    # Индекс стартовой свечи
    gap_start_idx = df[df["timestamp"] == start_ts].index
    if gap_start_idx.empty:
        print(f"[calc_ema] ❌ Не найден start_ts {start_ts}")
        return 0
    gap_start_idx = gap_start_idx[0]

    # Контекст до start_ts
    context_df = df.iloc[max(0, gap_start_idx - max_period) : gap_start_idx]

    # Валидация
    is_valid, reason = validate_for_indicator(context_df, max_period, tf_sec)
    if not is_valid:
        print(f"[calc_ema] ❌ Валидация провалена: {reason}")
        _mark_invalid(symbol, timeframe, ema_periods, start_ts, end_ts, conn)
        return 0

    # Расчёт EMA напрямую от свечей
    total_updated = 0
    for period in ema_periods:
        ema_col = f"ema{period}"

        # Проверяем достаточность данных
        if len(df) < period:
            _mark_invalid(symbol, timeframe, [period], start_ts, end_ts, conn)
            continue

        # Рассчитываем EMA от всех свечей
        ema_series = ta.ema(df["close"], length=period)

        # Обновляем значения в БД
        updated_rows = 0
        with conn:
            for idx, (timestamp, ema_value) in enumerate(
                zip(df["timestamp"], ema_series)
            ):
                if pd.isna(ema_value):
                    # NaN значения записываем как NULL
                    cursor = conn.execute(
                        f"UPDATE {table} SET {ema_col}=NULL WHERE symbol=? AND timestamp=?",
                        [symbol, timestamp],
                    )
                else:
                    # Числовые значения записываем как float
                    cursor = conn.execute(
                        f"UPDATE {table} SET {ema_col}=? WHERE symbol=? AND timestamp=?",
                        [float(ema_value), symbol, timestamp],
                    )
                updated_rows += cursor.rowcount

        total_updated += updated_rows
        print(f"[calc_ema] ✅ ema{period}: рассчитано {updated_rows} строк")

    # Postflight
    ema_cols = [f"ema{p}" for p in ema_periods]
    df_post = pd.read_sql_query(
        f"SELECT timestamp, {','.join(ema_cols)} FROM {table} WHERE symbol=? AND timestamp>=? AND timestamp<=?",
        conn,
        params=(symbol, start_ts, end_ts),
    )
    df_post = EzDIM.postflight(
        df_post, check_cols=ema_cols, symbol=symbol, timeframe=timeframe, silent=True
    )

    # Собираем статистику по всем EMA периодам
    postflight_stats = {}
    total_nan = 0
    total_minus_one = 0

    for col in ema_cols:
        nan_count = df_post[col].isna().sum()
        minus_one_count = (df_post[col] == -1).sum()
        postflight_stats[col] = {"nan": nan_count, "-1": minus_one_count}
        total_nan += nan_count
        total_minus_one += minus_one_count

    # Если дыр нет → выводим только успех
    if total_nan == 0 and total_minus_one == 0:
        print(f"[calc_ema] ✅ Постфлайт успешен для {symbol} {timeframe}")
        return total_updated

    # Если есть дырки → выводим компактный лог и чиним
    problem_parts = []
    for col, stats in postflight_stats.items():
        if stats["nan"] > 0 or stats["-1"] > 0:
            problem_parts.append(f"{col} NaN={stats['nan']} -1={stats['-1']}")

    if problem_parts:
        print(f"[calc_ema] ⚠️ Постфлайт: {', '.join(problem_parts)}")

    print(f"[calc_ema] 🔍 Найдены дыры, запускаю find_and_fix_gaps...")
    period_map = {f"ema{p}": p for p in ema_periods}
    fixed_rows = EzDIM.find_and_fix_gaps(
        df_post,
        ema_cols,
        tf_sec,
        period_map,
        symbol,
        timeframe,
        conn,
        full_scan=False,
    )
    print(f"[calc_ema] 🔧 Исправлено {fixed_rows} строк через find_and_fix_gaps")

    return total_updated + fixed_rows


def _mark_invalid(
    symbol: str,
    timeframe: str,
    ema_periods: List[int],
    start_ts: int,
    end_ts: int,
    conn: sqlite3.Connection,
):
    """Помечает EMA -1 в диапазоне, если расчёт невозможен"""
    table = f"candles_{timeframe}"
    for period in ema_periods:
        ema_col = f"ema{period}"
        with conn:
            conn.execute(
                f"UPDATE {table} SET {ema_col}=-1 WHERE symbol=? AND timestamp>=? AND timestamp<=?",
                [symbol, start_ts, end_ts],
            )
    print(f"[calc_ema] ⚠️ Помечено -1 для {ema_periods} {symbol} {timeframe}")


def main():
    """CLI"""
    import argparse

    parser = argparse.ArgumentParser(description="Универсальный расчёт EMA")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--timeframe", default="1m")
    parser.add_argument("--start", type=int, required=True)
    parser.add_argument("--end", type=int, required=True)
    parser.add_argument("--periods", nargs="+", type=int, default=EMA_PERIODS)
    args = parser.parse_args()

    with sqlite3.connect(str(DB_PATH)) as conn:
        updated = calc_ema(
            args.symbol, args.timeframe, args.periods, args.start, args.end, conn
        )
    print(f"\n✅ Завершено. Всего обновлено {updated} строк.")


if __name__ == "__main__":
    main()
