class EzDIM:
    """
    Easy Data Integrity Manager - простой менеджер целостности данных
    """

    # Атрибут класса для накопления статистики
    stats = {}

    # Атрибут класса для накопления статистики дыр в индикаторах
    gap_stats = {}

    @staticmethod
    def preflight(df, required_cols=None, min_rows=1, tf_sec=None):
        """
        Предварительная проверка данных

        Args:
            df: DataFrame для проверки
            required_cols: список обязательных колонок
            min_rows: минимальное количество строк
            tf_sec: временной интервал в секундах

        Returns:
            bool: True если всё ок, False если есть проблемы
        """
        print("[ezDIM preflight]")

        has_problems = False

        # 1. Проверка обязательных колонок
        if required_cols:
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                print(
                    f"[ezDIM preflight] ❌ Отсутствуют обязательные колонки: {missing_cols}"
                )
                has_problems = True
            else:
                print(
                    f"[ezDIM preflight] ✅ Все обязательные колонки присутствуют: {required_cols}"
                )

        # 2. Проверка минимального количества строк
        if len(df) < min_rows:
            print(f"[ezDIM preflight] ❌ Недостаточно строк: {len(df)} < {min_rows}")
            has_problems = True
        else:
            print(f"[ezDIM preflight] ✅ Количество строк: {len(df)} >= {min_rows}")

        # 3. Подсчёт NaN по OHLC колонкам
        ohlc_cols = ["open", "high", "low", "close"]
        nan_counts = {}
        for col in ohlc_cols:
            if col in df.columns:
                nan_count = df[col].isna().sum()
                nan_counts[col] = nan_count
                if nan_count > 0:
                    print(f"[ezDIM preflight] ⚠️  NaN в колонке {col}: {nan_count}")
                    has_problems = True
                else:
                    print(f"[ezDIM preflight] ✅ NaN в колонке {col}: 0")
            else:
                print(f"[ezDIM preflight] ⚠️  Колонка {col} отсутствует")

        # 4. Проверка разрывов во времени
        if tf_sec and "timestamp" in df.columns:
            df_sorted = df.sort_values("timestamp").copy()
            time_diffs = df_sorted["timestamp"].diff().dropna()

            # Находим дыры (разрывы больше 1.1 * tf_sec)
            holes = time_diffs[time_diffs > 1.1 * tf_sec]
            hole_count = len(holes)

            if hole_count > 0:
                print(f"[ezDIM preflight] ⚠️  Найдено дыр во времени: {hole_count}")
                print(
                    f"[ezDIM preflight]    Максимальный разрыв: {time_diffs.max():.2f} сек"
                )
                has_problems = True
            else:
                print(f"[ezDIM preflight] ✅ Дыр во времени не найдено")
        elif tf_sec and "timestamp" not in df.columns:
            print(
                "[ezDIM preflight] ⚠️  tf_sec указан, но колонка timestamp отсутствует"
            )

        return not has_problems

    @staticmethod
    def postflight(df, check_cols=None, symbol=None, timeframe=None, silent=False):
        """
        Пост-проверка данных

        Args:
            df: DataFrame для проверки
            check_cols: список колонок для проверки
            symbol: символ торговой пары
            timeframe: временной интервал
            silent: если True, отключает вывод логов

        Returns:
            DataFrame: очищенный DataFrame
        """
        if not silent:
            print("[ezDIM postflight]")

        if not check_cols:
            if not silent:
                print("[ezDIM postflight] ✅ check_cols не указан, пропускаем проверку")
            return df

        has_problems = False
        df_cleaned = df.copy()

        # Создаем ключ для статистики
        stats_key = f"{symbol}_{timeframe}" if symbol and timeframe else "unknown"
        if stats_key not in EzDIM.stats:
            EzDIM.stats[stats_key] = {}

        # EMA периоды для обработки
        ema_periods = [20, 50, 100, 200, 500]

        for col in check_cols:
            if col not in df.columns:
                if not silent:
                    print(
                        f"[ezDIM postflight] ⚠️  Колонка {col} отсутствует в DataFrame"
                    )
                has_problems = True
                continue

            # Проверяем, является ли это EMA колонкой
            is_ema_col = False
            ema_period = None

            for period in ema_periods:
                if col == f"ema{period}":
                    is_ema_col = True
                    ema_period = period
                    break

            if is_ema_col and ema_period:
                # Для EMA колонок: первые period-1 строк оставляем NaN, остальные NaN заменяем на -1
                if df_cleaned[col].isna().any():
                    # Первые period-1 строк оставить NaN
                    if len(df_cleaned) >= ema_period:
                        df_cleaned.loc[ema_period - 1 :, col] = df_cleaned.loc[
                            ema_period - 1 :, col
                        ].fillna(-1)
                    else:
                        # Если данных меньше чем period, все NaN заменяем на -1
                        df_cleaned[col] = df_cleaned[col].fillna(-1)

                    df_cleaned[col] = df_cleaned[col].astype(float)
            else:
                # Для не-EMA колонок: все NaN заменяем на -1 (старая логика)
                if df_cleaned[col].isna().any():
                    df_cleaned[col] = df_cleaned[col].fillna(-1).astype(float)

            # Подсчитываем количество NaN и -1
            nan_count = df_cleaned[col].isna().sum()
            minus_one_count = (df_cleaned[col] == -1).sum()

            # Сохраняем статистику
            EzDIM.stats[stats_key][col] = {
                "nan": int(nan_count),
                "-1": int(minus_one_count),
            }

            # Логируем статистику только если не silent
            if not silent:
                print(
                    f"[ezDIM postflight] ⚠️  {col}: {nan_count} NaN, {minus_one_count} -1"
                )
            has_problems = True

        if not has_problems and not silent:
            print("[ezDIM postflight] Looks fine")

        return df_cleaned

    @staticmethod
    def find_gaps_for_indicators(
        df, indicator_cols, tf_sec, period_map, symbol=None, timeframe=None
    ):
        """
        Находит интервалы дырок (NaN или -1) для индикаторов (например EMA).
        Первые period-1 свечей считаются валидными NaN и не обрабатываются.

        Args:
            df: DataFrame с колонкой timestamp и индикаторными колонками
            indicator_cols: список колонок индикаторов, где ищем дыры
            tf_sec: шаг таймфрейма в секундах
            period_map: dict { "ema20": 20, "ema50": 50, ... }
            symbol: символ торговой пары
            timeframe: временной интервал

        Returns:
            Список словарей с интервалами дыр:
            [{"col": "ema20", "start_ts": ..., "end_ts": ..., "period": 20}, ...]
        """
        gaps = []
        for col in indicator_cols:
            if col not in df.columns:
                continue

            period = period_map.get(col)
            if not period:
                continue

            # Маска дыр: NaN или -1
            mask = df[col].isna() | (df[col] == -1)
            if not mask.any():
                continue

            df_gaps = df[mask]

            # Игнорируем первые period-1 строк (естественные NaN)
            if len(df) >= period:
                valid_start_idx = df.index[period - 1]
                df_gaps = df_gaps[df_gaps.index > valid_start_idx]
                print(
                    f"[ezDIM gaps] {col}: первые {period-1} строк считаем нормой, дыры ищем начиная с индекса {valid_start_idx}"
                )
            else:
                # Если данных меньше чем period, все строки считаются дырами
                print(
                    f"[ezDIM gaps] {col}: данных меньше периода {period}, все строки считаются дырами"
                )

            if df_gaps.empty:
                continue

            # Группируем дырки по разрывам timestamp
            prev_ts, start_ts = None, None
            for ts in df_gaps["timestamp"]:
                if start_ts is None:
                    start_ts = ts
                elif prev_ts and ts - prev_ts > 1.1 * tf_sec:
                    gaps.append(
                        {
                            "col": col,
                            "start_ts": start_ts,
                            "end_ts": prev_ts,
                            "period": period,
                        }
                    )
                    print(
                        f"[ezDIM gaps] Найдена дыра: {col} {start_ts} → {prev_ts}, период={period}, длина={(prev_ts-start_ts)//tf_sec+1}"
                    )
                    start_ts = ts
                prev_ts = ts
            if start_ts is not None:
                gaps.append(
                    {
                        "col": col,
                        "start_ts": start_ts,
                        "end_ts": prev_ts,
                        "period": period,
                    }
                )
                print(
                    f"[ezDIM gaps] Найдена дыра: {col} {start_ts} → {prev_ts}, период={period}, длина={(prev_ts-start_ts)//tf_sec+1}"
                )

        # Накопление статистики дыр
        if symbol and timeframe:
            key = f"{symbol}_{timeframe}"
            if key not in EzDIM.gap_stats:
                EzDIM.gap_stats[key] = {}

            for gap in gaps:
                col = gap["col"]
                start_ts = gap["start_ts"]
                end_ts = gap["end_ts"]

                # Считаем количество строк в интервале
                points_count = int((end_ts - start_ts) / tf_sec) + 1

                if col not in EzDIM.gap_stats[key]:
                    EzDIM.gap_stats[key][col] = {"gaps_found": 0, "fixed": 0}

                EzDIM.gap_stats[key][col]["gaps_found"] += 1
                EzDIM.gap_stats[key][col]["fixed"] += points_count

        return gaps

    @staticmethod
    def find_and_fix_gaps(
        df,
        indicator_cols,
        tf_sec,
        period_map,
        symbol=None,
        timeframe=None,
        conn=None,
        full_scan=False,
    ):
        """
        Находит и автоматически исправляет дыры в индикаторах.
        Использует локальный импорт для избежания циклических зависимостей.

        Args:
            df: DataFrame с колонкой timestamp и индикаторными колонками
            indicator_cols: список колонок индикаторов, где ищем дыры
            tf_sec: шаг таймфрейма в секундах
            period_map: dict { "ema20": 20, "ema50": 50, ... }
            symbol: символ торговой пары
            timeframe: временной интервал
            conn: соединение с БД (обязательно для исправления дыр)
            full_scan: если True, проверяет весь диапазон (для batch режима)

        Returns:
            int: общее количество исправленных строк
        """
        if not conn:
            print("[ezDIM find_and_fix_gaps] ❌ Соединение с БД не предоставлено")
            return 0

        # Находим дыры
        gaps = EzDIM.find_gaps_for_indicators(
            df, indicator_cols, tf_sec, period_map, symbol, timeframe
        )

        # Если full_scan=True и дыр не найдено, проверяем на пустые таблицы
        if not gaps and full_scan:
            print(
                f"[ezDIM find_and_fix_gaps] 🔍 full_scan=True, проверяем на пустые таблицы..."
            )

            # Проверяем, есть ли вообще данные в индикаторных колонках
            has_data = False
            for col in indicator_cols:
                if col in df.columns and not df[col].isna().all():
                    has_data = True
                    break

            if not has_data:
                # Таблица пустая - создаем одну большую дыру на весь диапазон
                min_ts = df["timestamp"].min()
                max_ts = df["timestamp"].max()

                for col in indicator_cols:
                    period = period_map.get(col)
                    if period:
                        gaps.append(
                            {
                                "col": col,
                                "start_ts": min_ts,
                                "end_ts": max_ts,
                                "period": period,
                            }
                        )
                        print(
                            f"[ezDIM find_and_fix_gaps] 🔍 Создана большая дыра для пустой таблицы: {col} {min_ts} → {max_ts}"
                        )

        if not gaps:
            print(
                f"[ezDIM find_and_fix_gaps] ✅ Дыр не найдено для {symbol} {timeframe}"
            )
            return 0

        print(f"[ezDIM find_and_fix_gaps] 🔧 Найдено {len(gaps)} дыр, исправляем...")

        # Локальный импорт для избежания циклических зависимостей
        from backend.core.indicators.calc_ema import recalculate_range

        total_fixed = 0

        # Исправляем каждую дыру
        for gap in gaps:
            col = gap["col"]
            start_ts = gap["start_ts"]
            end_ts = gap["end_ts"]
            period = gap["period"]

            print(
                f"[ezDIM find_and_fix_gaps] Исправляю {col} {start_ts} → {end_ts}, период={period}"
            )

            try:
                fixed_rows = recalculate_range(
                    symbol=symbol,
                    timeframe=timeframe,
                    start_ts=start_ts,
                    end_ts=end_ts,
                    ema_periods=[period],
                    conn=conn,
                )
                total_fixed += fixed_rows

                if fixed_rows > 0:
                    print(
                        f"[ezDIM find_and_fix_gaps] ✅ Исправлено {fixed_rows} строк для {col}"
                    )
                else:
                    print(f"[ezDIM find_and_fix_gaps] ⚠️ Не удалось исправить {col}")

            except Exception as e:
                print(f"[ezDIM find_and_fix_gaps] ❌ Ошибка при исправлении {col}: {e}")

        print(
            f"[ezDIM find_and_fix_gaps] ✅ Всего исправлено {total_fixed} строк для {symbol} {timeframe}"
        )
        return total_fixed

    @staticmethod
    def report():
        """
        Выводит накопленную статистику и очищает её
        """
        if not EzDIM.stats:
            print("[ezDIM] Нет накопленной статистики")
            return

        for key, columns_stats in EzDIM.stats.items():
            stats_parts = []
            for col, stats in columns_stats.items():
                nan_count = stats.get("nan", 0)
                minus_one_count = stats.get("-1", 0)
                stats_parts.append(f"{col}: {nan_count} NaN, {minus_one_count} -1")

            print(f"{key} | {' | '.join(stats_parts)}")

        # Очищаем статистику
        EzDIM.stats = {}

    @staticmethod
    def report_gaps():
        """
        Выводит сводку по найденным и исправленным дыркам в индикаторах
        """
        if not EzDIM.gap_stats:
            print("[ezDIM] Нет информации о дырках в индикаторах")
            return

        print("\n📊 Сводка по дыркам в индикаторах:")
        print("-------------------------------------------------")
        for key, cols in EzDIM.gap_stats.items():
            print(key)
            for col, stats in cols.items():
                print(
                    f"  {col}: {stats['gaps_found']} интервалов, {stats['fixed']} точек исправлено"
                )

        # Очищаем статистику дыр
        EzDIM.gap_stats = {}


# CLI-блок для запуска как скрипта
if __name__ == "__main__":
    import sqlite3
    import pandas as pd
    from backend.config.timeframes_config import TIMEFRAMES_CONFIG
    from backend.core.indicators.calc_ema import calc_ema, EMA_PERIODS, DB_PATH

    print("🔍 EzDIM CLI: Поиск и исправление дыр в EMA индикаторах")
    print("=" * 60)

    # Подключаемся к БД
    try:
        conn = sqlite3.connect(DB_PATH)
        print(f"✅ Подключение к БД: {DB_PATH}")
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        exit(1)

    # Получаем список всех символов
    try:
        symbols_df = pd.read_sql_query(
            "SELECT DISTINCT symbol FROM candles_1m ORDER BY symbol", conn
        )
        symbols = symbols_df["symbol"].tolist()
        print(f"📊 Найдено символов: {len(symbols)}")
    except Exception as e:
        print(f"❌ Ошибка получения символов: {e}")
        conn.close()
        exit(1)

    # Создаем mapping периодов для EMA колонок
    ema_period_map = {f"ema{period}": period for period in EMA_PERIODS}
    ema_cols = list(ema_period_map.keys())

    total_gaps_found = 0
    total_gaps_fixed = 0

    # Обрабатываем каждый символ и таймфрейм
    for symbol in symbols:
        print(f"\n🔍 Обрабатываем символ: {symbol}")

        for timeframe in TIMEFRAMES_CONFIG.keys():
            table_name = f"candles_{timeframe}"
            tf_sec = TIMEFRAMES_CONFIG[timeframe]["interval_sec"]

            print(f"  📈 Таймфрейм: {timeframe}")

            try:
                # Загружаем данные свечей
                df = pd.read_sql_query(
                    f"SELECT * FROM {table_name} WHERE symbol = ? ORDER BY timestamp",
                    conn,
                    params=(symbol,),
                )

                if df.empty:
                    print(f"    ⚠️ Нет данных для {symbol} {timeframe}")
                    continue

                print(f"    📊 Загружено свечей: {len(df)}")

                # Ищем дыры в EMA индикаторах
                gaps = EzDIM.find_gaps_for_indicators(
                    df=df,
                    indicator_cols=ema_cols,
                    tf_sec=tf_sec,
                    period_map=ema_period_map,
                    symbol=symbol,
                    timeframe=timeframe,
                )

                if not gaps:
                    print(f"    ✅ Дыр в EMA не найдено")
                    continue

                print(f"    🔧 Найдено дыр: {len(gaps)}")
                total_gaps_found += len(gaps)

                # Исправляем каждую дыру
                for gap in gaps:
                    col = gap["col"]
                    start_ts = gap["start_ts"]
                    end_ts = gap["end_ts"]
                    period = gap["period"]

                    print(f"      🔧 Исправляем {col}: {start_ts} → {end_ts}")

                    try:
                        fixed_rows = calc_ema(
                            symbol=symbol,
                            timeframe=timeframe,
                            ema_periods=[period],
                            start_ts=start_ts,
                            end_ts=end_ts,
                            conn=conn,
                        )

                        if fixed_rows > 0:
                            print(f"      ✅ Исправлено строк: {fixed_rows}")
                            total_gaps_fixed += fixed_rows
                        else:
                            print(f"      ⚠️ Не удалось исправить {col}")

                    except Exception as e:
                        print(f"      ❌ Ошибка при исправлении {col}: {e}")

            except Exception as e:
                print(f"    ❌ Ошибка обработки {symbol} {timeframe}: {e}")
                continue

    # Закрываем соединение
    conn.close()

    # Выводим итоговую статистику
    print("\n" + "=" * 60)
    print("📊 ИТОГОВАЯ СТАТИСТИКА:")
    print(f"🔧 Всего найдено дыр: {total_gaps_found}")
    print(f"✅ Всего исправлено строк: {total_gaps_fixed}")

    if total_gaps_found == 0:
        print("🎉 Все EMA индикаторы в порядке!")
    else:
        print(f"🔧 Обработано {total_gaps_found} дыр")

    print("=" * 60)
