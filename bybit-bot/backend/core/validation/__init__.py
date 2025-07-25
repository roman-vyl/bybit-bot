"""
Модуль валидации данных

Предоставляет инструменты для проверки целостности и качества данных свечей.
"""

from .data_integrity import (
    validate_for_indicator,
    has_gaps,
    validate_ohlc_data,
    quick_check,
)

__all__ = [
    "validate_for_indicator",
    "has_gaps",
    "validate_ohlc_data",
    "quick_check",
]
