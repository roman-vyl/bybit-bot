"use client";

import {
    createChart,
    CandlestickSeries,
    LineSeries,
    type IChartApi,
    type CandlestickData,
    type Time,
} from "lightweight-charts";
import { useEffect, useRef } from "react";
import type { EmaPoint } from "@/types/chart";

type CandleChartViewProps = {
    candles: CandlestickData<Time>[];
    emaSeries: Record<string, EmaPoint[]>;
    timeframe: string;
    symbol: string;
};

export default function CandleChartView({
    candles,
    emaSeries,
    timeframe,
}: CandleChartViewProps) {
    const chartRef = useRef<IChartApi | null>(null);
    const containerRef = useRef<HTMLDivElement | null>(null);

    useEffect(() => {
        if (!containerRef.current) return;

        const chart = createChart(containerRef.current, {
            layout: {
                background: { color: "#111" },
                textColor: "#ccc",
            },
            grid: {
                vertLines: { color: "#222" },
                horzLines: { color: "#222" },
            },
            timeScale: {
                timeVisible: true,
                secondsVisible: false,
            },
            rightPriceScale: {
                borderColor: "#555",
            },
            crosshair: {
                mode: 0,
            },
        });

        chartRef.current = chart;

        const candleSeries = chart.addSeries(CandlestickSeries);

        candleSeries.setData(candles);

        // Добавление всех EMA линий с фильтрацией -1
        Object.entries(emaSeries).forEach(([tf, points]) => {
            const line = chart.addSeries(LineSeries, {
                color: "#ffa500",
                lineWidth: tf === timeframe ? 2 : 1,
                lineStyle: tf === timeframe ? 0 : 2,
            });

            // Фильтруем значения -1 и преобразуем в формат LineSeriesData
            const lineData = points
                .filter((p) => p.value >= 0) // Фильтруем -1 и отрицательные значения
                .map((p) => ({
                    time: p.time as Time,
                    value: p.value,
                }));

            line.setData(lineData);
        });

        return () => chart.remove();
    }, [candles, emaSeries, timeframe]);

    return (
        <div
            ref={containerRef}
            style={{
                position: "relative",
                width: "100%",
                height: "600px",
            }}
        />
    );
}
