# plot_ema_streamlit.py
# ---------------------------------------------------------
# Streamlit-приложение для визуализации свечных графиков и EMA-индикаторов
# Таймфреймы — в виде горизонтальных кнопок (как в TradingView)
# EMA — включаются тогглами в левой колонке
# ---------------------------------------------------------

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

import streamlit as st
import pandas as pd
import sqlite3
import plotly.graph_objects as go

from config.timeframes_config import TIMEFRAMES_CONFIG

# Параметры
BASE_DIR = Path(__file__).resolve().parents[1]  # backend/
PROJECT_ROOT = BASE_DIR.parent  # bybit-bot/
DB_PATH = PROJECT_ROOT / "db" / "market_data.sqlite"
EMA_PATH = BASE_DIR / "config" / "ema_periods.txt"


# Загрузка EMA-периодов
def load_ema_periods():
    with open(EMA_PATH, "r") as f:
        return sorted(set(int(line.strip()) for line in f if line.strip().isdigit()))


# Загрузка данных из БД
def load_data(symbol, tf):
    table = f"candles_{tf}"
    conn = sqlite3.connect(DB_PATH)
    query = f"SELECT * FROM {table} WHERE symbol = ? ORDER BY timestamp DESC LIMIT 500"
    df = pd.read_sql(query, conn, params=(symbol,))
    conn.close()
    df = df.sort_values("timestamp")
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
    return df


# Визуализация
def plot_chart(df, ema_periods):
    fig = go.Figure()

    fig.add_trace(
        go.Candlestick(
            x=df["timestamp"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            name="Candles",
        )
    )

    for period in ema_periods:
        col = f"ema_{period}"
        if col in df.columns and not df[col].isna().all():
            fig.add_trace(
                go.Scatter(
                    x=df["timestamp"], y=df[col], mode="lines", name=f"EMA {period}"
                )
            )

    fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Price",
        xaxis_rangeslider_visible=False,
    )
    return fig


# --- UI ---

# Таймфреймы в стиле TradingView
st.markdown("**Таймфрейм:**")
if "selected_tf" not in st.session_state:
    st.session_state.selected_tf = "1h"

cols = st.columns(len(TIMEFRAMES_CONFIG))
for i, tf in enumerate(TIMEFRAMES_CONFIG.keys()):
    if tf == st.session_state.selected_tf:
        cols[i].markdown(
            f"<button style='background:#555;color:white;padding:6px 12px;border:none;border-radius:4px;cursor:default'>{tf}</button>",
            unsafe_allow_html=True,
        )
    else:
        if cols[i].button(tf):
            st.session_state.selected_tf = tf

selected_tf = st.session_state.selected_tf

symbol = st.text_input("Введите символ (например, BTCUSDT)", value="BTCUSDT")

ema_periods = load_ema_periods()

# Две колонки: слева тогглы EMA, справа график
left, right = st.columns([1, 5])

with left:
    st.markdown("**EMA линии:**")
    selected_emas = []
    for period in ema_periods:
        if st.toggle(f"EMA {period}", value=True, key=f"ema_toggle_{period}"):
            selected_emas.append(period)

with right:
    if symbol and selected_tf:
        df = load_data(symbol, selected_tf)
        if not df.empty:
            st.plotly_chart(plot_chart(df, selected_emas), use_container_width=True)
        else:
            st.warning("Нет данных по выбранному символу и таймфрейму.")
