"use client";

import React, { useEffect, useRef } from "react";
import {
    createChart,
    CandlestickSeries,
    LineSeries,
    type IChartApi,
    type CandlestickData,
    type Time,
    LineType, // <--- добавлен импорт
} from "lightweight-charts";
import { useChartData } from "@/lib/useChartData";
import { useEmaData, useMultiTimeframeEma } from "@/lib/useEmaData";
import { EmaDisplayConfig } from "@/types/chart";
import EmaControls from "./EmaControls";
import { TIMEFRAME_TO_SECONDS } from "@/lib/config";

const EMA_COLORS: Record<string, string> = {
    "20": "#ff6b6b",
    "50": "#4ecdc4",
    "100": "#45b7d1",
    "200": "#ffa726",
    "500": "#ab47bc",
};

type ChartWrapperProps = {
    timeframe: string;
    symbol: string;
    emaPeriods: string[];
    availableTimeframes: string[];
};

export default function ChartWrapper({ timeframe, symbol, emaPeriods, availableTimeframes }: ChartWrapperProps) {
    const { initialCandles, start, end, isLoading, error } = useChartData(symbol, timeframe);

    const chartRef = useRef<IChartApi | null>(null);
    const containerRef = useRef<HTMLDivElement | null>(null);
    // Храним актуальный видимый диапазон логических индексов
    const visibleRangeRef = React.useRef<{ from: number; to: number } | null>(null);

    // Новая инициализация состояния: только текущий ТФ, все EMA включены
    const [emaConfig, setEmaConfig] = React.useState<EmaDisplayConfig>(() => {
        const initial: EmaDisplayConfig = { enabled: true };
        initial[timeframe] = [...emaPeriods];
        return initial;
    });

    React.useEffect(() => {
        setEmaConfig(cfg => {
            const updated: EmaDisplayConfig = { enabled: true };
            // Включаем EMA только для текущего timeframe
            updated[timeframe] = [...emaPeriods];
            return updated;
        });
    }, [timeframe, emaPeriods]);

    // Собираем все выбранные ТФ и периоды
    const selectedTimeframes = Object.keys(emaConfig).filter(tf => tf !== "enabled" && Array.isArray(emaConfig[tf]) && (emaConfig[tf] as string[]).length > 0);
    const periodsByTf: Record<string, string[]> = {};
    selectedTimeframes.forEach(tf => {
        periodsByTf[tf] = Array.isArray(emaConfig[tf]) ? emaConfig[tf] as string[] : [];
    });

    // Получаем данные EMA для всех выбранных ТФ и периодов
    // (можно использовать useMultiTimeframeEma, но фильтруем по выбранным периодам)
    const { emaData: multiEmaData } = useMultiTimeframeEma(
        symbol,
        selectedTimeframes,
        emaPeriods,
        emaConfig.enabled
    );

    useEffect(() => {
        if (!containerRef.current || initialCandles.length === 0) return;

        const chart = createChart(containerRef.current, {
            layout: {
                background: { color: "#111" },
                textColor: "#ccc"
            },
            grid: { vertLines: { color: "#222" }, horzLines: { color: "#222" } },
            timeScale: { timeVisible: true, secondsVisible: false },
            rightPriceScale: { borderColor: "#555" },
            crosshair: { mode: 0 },
        });

        chartRef.current = chart;

        const candleSeries = chart.addSeries(CandlestickSeries);
        const candleData: CandlestickData<Time>[] = initialCandles.map(candle => ({
            time: candle.timestamp as Time,
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
        }));
        console.debug("[EMA] 🔥 Candles example:", candleData.slice(0, 3));
        candleSeries.setData(candleData);
        console.debug("[EMA] ✅ Candles set:", candleData.length, "candles");

        // Подписка на изменение видимого диапазона
        const updateVisibleRange = (range: { from: number; to: number } | null) => {
            if (range && typeof range.from === "number" && typeof range.to === "number") {
                visibleRangeRef.current = { from: Number(range.from), to: Number(range.to) };
            } else {
                visibleRangeRef.current = null;
            }
        };
        chart.timeScale().subscribeVisibleLogicalRangeChange(updateVisibleRange);
        // Инициализация текущим диапазоном
        updateVisibleRange(chart.timeScale().getVisibleLogicalRange());

        console.debug("[EMA] 📦 multiEmaData:", multiEmaData);
        console.debug("[EMA] ⚙️ emaConfig:", emaConfig);
        console.debug("[EMA] ⏱ selectedTimeframes:", selectedTimeframes);
        console.debug("[EMA] 📊 periodsByTf:", periodsByTf);
        let needFitContent = false;

        if (emaConfig.enabled && multiEmaData) {
            selectedTimeframes.forEach(tf => {
                const tfData = multiEmaData[tf] || {};
                const periods = periodsByTf[tf] || [];
                periods.forEach(period => {
                    const points = tfData[period];
                    console.debug(`[EMA] ⏱ Raw EMA timestamps ${tf}-${period}:`, points?.slice(0, 3).map(p => p.time));
                    console.debug(`[EMA] 🔍 Timeframe: ${tf}, Period: ${period}`);
                    console.debug(`[EMA] 🔢 Total raw points:`, points?.length ?? 0);
                    if (!points) return;
                    // 🛡 Защита от артефактов EMA
                    if (points.length < 5) return;

                    // (Фильтрация по visibleRange отключена по требованию)
                    console.debug(`[EMA]  Raw points (${points.length}) from ${tf}-${period}:`, points.map((p: any) => p.time));

                    // Проверка на большие гэпы
                    const timestamps = points.map((p: any) => Number(p.time));
                    const tfSec = TIMEFRAME_TO_SECONDS[tf];
                    const hasBigGap = timestamps.some((t, i) => i > 0 && (t - timestamps[i - 1]) > tfSec * 2);
                    if (hasBigGap) return;

                    // Уникальный priceScaleId для каждой EMA
                    // Для редких EMA (например, 1h) — ступенчатый тип линии и маркеры
                    const isSparse = tfSec >= 3600;

                    const line = chart.addSeries(LineSeries, {
                        color: EMA_COLORS[period] || "#ffa500",
                        lineWidth: 2,
                        priceLineVisible: false,
                        priceScaleId: 'right',
                        lineType: isSparse ? LineType.WithSteps : LineType.Simple,
                        crosshairMarkerVisible: isSparse,
                    });
                    const lineData = points
                        .filter(p => typeof p.value === "number" && isFinite(p.value))
                        .map(p => ({
                            time: p.time as Time,
                            value: p.value,
                        }));
                    console.debug(`[EMA] 🧩 Final lineData for ${tf}-${period}:`, lineData.slice(0, 3));
                    if (lineData.length === 0) return;
                    line.setData(lineData);
                    needFitContent = true;
                });
            });
        }

        setTimeout(() => {
            chart.timeScale().fitContent();
            console.debug("[EMA] 🧭 chart.timeScale().fitContent() called");
        }, 0);

        return () => {
            chart.timeScale().unsubscribeVisibleLogicalRangeChange(updateVisibleRange);
            chart.remove();
        };
    }, [initialCandles, multiEmaData, emaConfig, selectedTimeframes, periodsByTf]);

    if (isLoading) {
        return <div style={{ color: "#ccc" }}>Загрузка графика...</div>;
    }

    if (error) {
        return <div style={{ color: "red" }}>Ошибка: {error.message || "Неизвестная"}</div>;
    }

    return (
        <div>
            <EmaControls
                config={emaConfig}
                currentTimeframe={timeframe}
                availableTimeframes={availableTimeframes}
                onConfigChange={setEmaConfig}
                emaPeriods={emaPeriods}
            />
            <div
                ref={containerRef}
                style={{
                    position: "relative",
                    width: "100%",
                    height: "600px",
                }}
            />
        </div>
    );
}
