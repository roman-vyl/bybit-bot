"use client";

import React from "react";
import { EmaDisplayConfig } from "@/types/chart";

type EmaControlsProps = {
    config: EmaDisplayConfig;
    currentTimeframe: string;
    availableTimeframes: string[];
    onConfigChange: (config: EmaDisplayConfig) => void;
    emaPeriods: string[];
};

const EMA_DISPLAY_RULES: Record<string, string[]> = {
    "1m": ["1m", "5m"],
    "5m": ["5m", "15m", "30m"],
    "15m": ["15m", "30m", "1h"],
    "30m": ["30m", "1h", "4h"],
    "1h": ["1h", "4h", "1d"],
    "4h": ["4h", "6h", "12h", "1d"],
    "6h": ["6h", "12h", "1d"],
    "12h": ["12h", "1d", "1w"],
    "1d": ["1d", "1w"],
    "1w": ["1w"]
};

export default function EmaControls({
    config,
    currentTimeframe,
    availableTimeframes,
    onConfigChange,
    emaPeriods,
}: EmaControlsProps) {
    const suggestedTimeframes = EMA_DISPLAY_RULES[currentTimeframe] || [currentTimeframe];
    const otherTimeframes = availableTimeframes.filter(tf => !suggestedTimeframes.includes(tf));

    const togglePeriod = (period: string) => {
        const newPeriods = config.periods.includes(period)
            ? config.periods.filter(p => p !== period)
            : [...config.periods, period].sort((a, b) => parseInt(a) - parseInt(b));

        onConfigChange({
            ...config,
            periods: newPeriods
        });
    };

    const toggleTimeframe = (timeframe: string) => {
        const newTimeframes = config.timeframes.includes(timeframe)
            ? config.timeframes.filter(tf => tf !== timeframe)
            : [...config.timeframes, timeframe];

        onConfigChange({
            ...config,
            timeframes: newTimeframes
        });
    };

    const toggleEnabled = () => {
        onConfigChange({
            ...config,
            enabled: !config.enabled
        });
    };

    return (
        <div
            style={{
                padding: "16px",
                backgroundColor: "#1a1a1a",
                border: "1px solid #333",
                borderRadius: "8px",
                marginBottom: "16px",
                color: "#ccc"
            }}
        >
            <div
                style={{
                    display: "flex",
                    alignItems: "center",
                    marginBottom: "12px"
                }}
            >
                <label style={{ display: "flex", alignItems: "center", cursor: "pointer" }}>
                    <input
                        type="checkbox"
                        checked={config.enabled}
                        onChange={toggleEnabled}
                        style={{ marginRight: "8px" }}
                    />
                    <span style={{ fontWeight: "bold" }}>Показать EMA линии</span>
                </label>
            </div>

            {config.enabled && (
                <>
                    {/* Периоды EMA для текущего ТФ */}
                    <div style={{ marginBottom: "16px" }}>
                        <div style={{ marginBottom: "8px", fontSize: "14px", fontWeight: "bold" }}>
                            EMA периоды для {currentTimeframe}:
                        </div>
                        <div style={{ display: "flex", flexWrap: "wrap", gap: "8px" }}>
                            {emaPeriods.map(period => (
                                <label key={period} style={{ display: "flex", alignItems: "center", cursor: "pointer" }}>
                                    <input
                                        type="checkbox"
                                        checked={config.periods.includes(period)}
                                        onChange={() => togglePeriod(period)}
                                        style={{ marginRight: "4px" }}
                                    />
                                    <span style={{ fontSize: "13px" }}>EMA{period}</span>
                                </label>
                            ))}
                        </div>
                    </div>

                    {/* Рекомендуемые таймфреймы */}
                    <div style={{ marginBottom: "16px" }}>
                        <div style={{ marginBottom: "8px", fontSize: "14px", fontWeight: "bold" }}>
                            Рекомендуемые EMA с других ТФ:
                        </div>
                        <div style={{ display: "flex", flexWrap: "wrap", gap: "8px" }}>
                            {suggestedTimeframes.filter(tf => tf !== currentTimeframe).map(tf => (
                                <label key={tf} style={{ display: "flex", alignItems: "center", cursor: "pointer" }}>
                                    <input
                                        type="checkbox"
                                        checked={config.timeframes.includes(tf)}
                                        onChange={() => toggleTimeframe(tf)}
                                        style={{ marginRight: "4px" }}
                                    />
                                    <span
                                        style={{
                                            fontSize: "13px",
                                            color: config.timeframes.includes(tf) ? "#ffa500" : "#999"
                                        }}
                                    >
                                        {tf} EMA
                                    </span>
                                </label>
                            ))}
                        </div>
                    </div>

                    {/* Дополнительные таймфреймы */}
                    {otherTimeframes.length > 0 && (
                        <div>
                            <div style={{ marginBottom: "8px", fontSize: "14px", fontWeight: "bold" }}>
                                Дополнительные ТФ:
                            </div>
                            <div style={{ display: "flex", flexWrap: "wrap", gap: "8px" }}>
                                {otherTimeframes.map(tf => (
                                    <label key={tf} style={{ display: "flex", alignItems: "center", cursor: "pointer" }}>
                                        <input
                                            type="checkbox"
                                            checked={config.timeframes.includes(tf)}
                                            onChange={() => toggleTimeframe(tf)}
                                            style={{ marginRight: "4px" }}
                                        />
                                        <span
                                            style={{
                                                fontSize: "13px",
                                                color: config.timeframes.includes(tf) ? "#ffa500" : "#666",
                                                fontStyle: "italic"
                                            }}
                                        >
                                            {tf} EMA
                                        </span>
                                    </label>
                                ))}
                            </div>
                        </div>
                    )}
                </>
            )}
        </div>
    );
} 