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
};

export default function ChartWrapper({ timeframe, symbol, emaPeriods }: ChartWrapperProps) {
    const { initialCandles, start, end, isLoading, error } = useChartData(symbol, timeframe);

    const chartRef = useRef<IChartApi | null>(null);
    const containerRef = useRef<HTMLDivElement | null>(null);

    const [emaConfig, setEmaConfig] = React.useState<EmaDisplayConfig>({
        enabled: true,
        periods: emaPeriods.slice(0, 2), // по умолчанию первые два
        timeframes: [timeframe],
    });

    React.useEffect(() => {
        setEmaConfig(cfg => ({
            ...cfg,
            timeframes: [timeframe],
        }));
    }, [timeframe]);

    const { emaData: currentEmaData } = useEmaData(
        symbol,
        timeframe,
        [timeframe],
        emaConfig.periods,
        emaConfig.enabled
    );

    const { emaData: multiEmaData } = useMultiTimeframeEma(
        symbol,
        emaConfig.timeframes,
        emaConfig.periods,
        emaConfig.enabled
    );

    useEffect(() => {
        if (!containerRef.current || initialCandles.length === 0) return;

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

        const candleData: CandlestickData<Time>[] = initialCandles.map(candle => ({
            time: candle.timestamp as Time,
            open: candle.open,
            high: candle.high,
            low: candle.low,
            close: candle.close,
        }));

        candleSeries.setData(candleData);

        if (emaConfig.enabled && currentEmaData[timeframe]) {
            Object.entries(currentEmaData[timeframe]).forEach(([period, points]) => {
                const line = chart.addSeries(LineSeries, {
                    color: EMA_COLORS[period] || "#ffa500",
                    lineWidth: 2,
                });

                const lineData = points.map(p => ({
                    time: p.time as Time,
                    value: p.value,
                }));

                line.setData(lineData);
            });
        }

        return () => {
            chart.remove();
        };
    }, [initialCandles, currentEmaData, timeframe]);

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
                availableTimeframes={[timeframe]}
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
