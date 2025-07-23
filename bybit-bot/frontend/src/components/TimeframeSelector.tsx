"use client";

import { ChartConfig } from "@/lib/config";

type TimeframeSelectorProps = {
    config: ChartConfig;
    currentTimeframe: string;
    onTimeframeChange: (timeframe: string) => void;
};

export default function TimeframeSelector({
    config,
    currentTimeframe,
    onTimeframeChange,
}: TimeframeSelectorProps) {
    return (
        <div className="timeframe-selector">
            <h3 className="timeframe-selector-title">Таймфрейм:</h3>
            <div className="timeframe-buttons">
                {config.timeframes.map((timeframe) => (
                    <button
                        key={timeframe}
                        className={`timeframe-btn ${timeframe === currentTimeframe ? "active" : ""
                            }`}
                        onClick={() => onTimeframeChange(timeframe)}
                    >
                        {timeframe}
                    </button>
                ))}
            </div>
        </div>
    );
} 