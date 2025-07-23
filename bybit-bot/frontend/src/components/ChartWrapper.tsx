"use client";

import React, { useEffect, useRef } from "react";
import {
    createChart,
    CandlestickSeries,
    LineSeries,
    type IChartApi,
    type CandlestickData,
    type Time,
} from "lightweight-charts";
import { useChartData } from "@/lib/useChartData";
import { useEmaData, useMultiTimeframeEma } from "@/lib/useEmaData";
import { EmaDisplayConfig } from "@/types/chart";
import EmaControls from "./EmaControls";

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

    // Новая инициализация состояния: только текущий ТФ, все EMA включены
    const [emaConfig, setEmaConfig] = React.useState<EmaDisplayConfig>(() => {
        const initial: EmaDisplayConfig = { enabled: true };
        initial[timeframe] = [...emaPeriods];
        return initial;
    });

    React.useEffect(() => {
        setEmaConfig(cfg => {
            const updated = { ...cfg };
            updated[timeframe] = [...emaPeriods]; // всегда включаем все EMA для нового ТФ
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
        candleSeries.setData(candleData);

        if (emaConfig.enabled && multiEmaData) {
            selectedTimeframes.forEach(tf => {
                const tfData = multiEmaData[tf] || {};
                const periods = periodsByTf[tf] || [];
                periods.forEach(period => {
                    const points = tfData[period];
                    if (!points) return;
                    const line = chart.addSeries(LineSeries, {
                        color: EMA_COLORS[period] || "#ffa500",
                        lineWidth: 2,
                    });
                    const lineData = points
                        .filter(p => typeof p.value === "number" && isFinite(p.value))
                        .map(p => ({
                            time: p.time as Time,
                            value: p.value,
                        }));
                    if (lineData.length > 0) {
                        line.setData(lineData);
                    }
                });
            });
        }

        return () => {
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
