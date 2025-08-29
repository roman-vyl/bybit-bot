import pandas as pd
from typing import Tuple


def has_gaps(df: pd.DataFrame, timeframe_sec: int) -> bool:
    """
    Проверяет пропуски в timestamp.

    Args:
        df: DataFrame с колонкой timestamp
        timeframe_sec: период времени в секундах

    Returns:
        bool: True если есть пропуски, False если пропусков нет

    Raises:
        ValueError: если timestamp не является целочисленным
    """
    if df.empty:
        return False

    # Проверяем, что timestamp целочисленный
    if not pd.api.types.is_integer_dtype(df["timestamp"]):
        raise ValueError("timestamp должен быть целочисленным")

    # Сортируем по timestamp
    df_sorted = df.sort_values("timestamp").copy()

    # Вычисляем разности между соседними timestamp
    time_diffs = df_sorted["timestamp"].diff().dropna()

    # Проверяем, есть ли разности больше 1.1 * timeframe_sec
    threshold = 1.1 * timeframe_sec
    has_gaps = (time_diffs > threshold).any()

    return has_gaps


def validate_ohlc_data(df: pd.DataFrame) -> bool:
    """
    Проверяет корректность OHLC данных.

    Args:
        df: DataFrame с колонками open, high, low, close

    Returns:
        bool: True если данные корректны, False если есть проблемы
    """
    # Проверяем наличие необходимых колонок
    required_columns = ["open", "high", "low", "close"]
    if not all(col in df.columns for col in required_columns):
        return False

    # Проверяем отсутствие NaN значений
    ohlc_data = df[required_columns]
    if ohlc_data.isna().any().any():
        return False

    # Проверяем, что все значения строго больше 0
    if (ohlc_data <= 0).any().any():
        return False

    return True


def validate_continuous_window(df: pd.DataFrame, period: int) -> Tuple[bool, str]:
    """
    Проверяет непрерывность данных в скользящем окне для расчета EMA.

    Args:
        df: DataFrame с данными
        period: размер окна для проверки

    Returns:
        Tuple[bool, str]: (результат валидации, сообщение)
    """
    if df.empty:
        return False, "DataFrame пуст"

    if "close" not in df.columns:
        return False, "Отсутствует колонка close"

    # Оптимизация: проверяем только если есть NULL
    if not df["close"].isna().any():
        return True, "OK"

    # Проверяем непрерывность в окне
    continuous_check = df["close"].notna().rolling(window=period).sum()

    if (continuous_check < period).any():
        broken = (continuous_check < period).sum()
        total_windows = len(continuous_check.dropna())
        return (
            False,
            f"Обнаружено {broken}/{total_windows} окон с недостаточными данными",
        )

    return True, "OK"


def validate_for_indicator(
    df: pd.DataFrame, period: int, tf_sec: int
) -> Tuple[bool, str]:
    """
    Комплексная валидация данных для расчета индикаторов.

    Args:
        df: DataFrame с данными
        period: период индикатора
        tf_sec: период времени в секундах

    Returns:
        Tuple[bool, str]: (результат валидации, сообщение)
    """
    # Проверяем корректность tf_sec
    if tf_sec <= 0:
        return False, "Некорректный tf_sec (<= 0)"

    # Проверяем достаточность данных
    if len(df) < period:
        return False, f"Недостаточно данных: {len(df)} < {period}"

    # Проверяем OHLC данные
    if not validate_ohlc_data(df):
        return False, "Некорректные OHLC данные"

    # Проверяем пропуски в данных
    try:
        if has_gaps(df, tf_sec):
            return False, "Обнаружены пропуски в данных"
    except ValueError as e:
        return False, f"Ошибка проверки пропусков: {str(e)}"

    # Проверка на непрерывные окна close
    is_continuous, reason = validate_continuous_window(df, period)
    if not is_continuous:
        return False, f"Непрерывность close: {reason}"

    return True, "OK"


def quick_check(df: pd.DataFrame, period: int, tf_sec: int) -> bool:
    """
    Упрощенный интерфейс для быстрой проверки данных.

    Args:
        df: DataFrame с данными
        period: период индикатора
        tf_sec: период времени в секундах

    Returns:
        bool: True если данные валидны, False если нет
    """
    is_valid, _ = validate_for_indicator(df, period, tf_sec)
    return is_valid
