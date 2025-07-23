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

export default function EmaControls({
    config,
    currentTimeframe,
    availableTimeframes,
    onConfigChange,
    emaPeriods,
}: EmaControlsProps) {
    // Для текущего ТФ
    const togglePeriod = (period: string) => {
        const current = Array.isArray(config[currentTimeframe]) ? config[currentTimeframe] as string[] : [];
        let updated: string[];
        if (current.includes(period)) {
            updated = current.filter((p: string) => p !== period);
        } else {
            updated = [...current, period];
        }
        onConfigChange({
            ...config,
            [currentTimeframe]: updated
        });
    };

    // Для чужих ТФ
    const toggleAllPeriodsForTf = (tf: string) => {
        const allPeriods = emaPeriods;
        const current = Array.isArray(config[tf]) ? config[tf] as string[] : [];
        const enabled = allPeriods.every(period => current.includes(period));
        onConfigChange({
            ...config,
            [tf]: enabled ? [] : [...allPeriods]
        });
    };
    const togglePeriodForTf = (tf: string, period: string) => {
        const current = Array.isArray(config[tf]) ? config[tf] as string[] : [];
        let updated: string[];
        if (current.includes(period)) {
            updated = current.filter((p: string) => p !== period);
        } else {
            updated = [...current, period];
        }
        onConfigChange({
            ...config,
            [tf]: updated
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
                padding: "8px 4px",
                backgroundColor: "#1a1a1a",
                border: "1px solid #333",
                borderRadius: "6px",
                marginBottom: "8px",
                color: "#ccc",
                display: "flex",
                flexDirection: "row",
                gap: "12px",
                justifyContent: "center",
                alignItems: "flex-start"
            }}
        >
            {/* Колонка для текущего ТФ */}
            <div style={{ minWidth: 80, maxWidth: 100 }}>
                <div style={{ marginBottom: "4px", fontSize: "12px", fontWeight: 500, textAlign: "center" }}>
                    {currentTimeframe}
                </div>
                <div style={{ display: "flex", flexDirection: "column", gap: "4px" }}>
                    {emaPeriods.map(period => {
                        const checked = Array.isArray(config[currentTimeframe]) && (config[currentTimeframe] as string[]).includes(period);
                        return (
                            <label key={period} style={{ display: "flex", alignItems: "center", cursor: "pointer", fontSize: "12px", margin: 0 }}>
                                <input
                                    type="checkbox"
                                    checked={checked}
                                    onChange={() => togglePeriod(period)}
                                    style={{ marginRight: "4px", width: 14, height: 14 }}
                                />
                                EMA{period}
                            </label>
                        );
                    })}
                </div>
            </div>
            {/* Колонки для чужих ТФ */}
            {availableTimeframes.filter(tf => tf !== currentTimeframe).map((tf: string) => {
                const allChecked = emaPeriods.every(period => Array.isArray(config[tf]) && (config[tf] as string[]).includes(period));
                return (
                    <div key={tf} style={{ minWidth: 80, maxWidth: 100 }}>
                        <div style={{ marginBottom: "4px", fontSize: "12px", fontWeight: 500, textAlign: "center" }}>
                            {tf}
                        </div>
                        <div style={{ display: "flex", flexDirection: "column", gap: "4px" }}>
                            <label style={{ display: "flex", alignItems: "center", cursor: "pointer", fontSize: "12px", margin: 0 }}>
                                <input
                                    type="checkbox"
                                    checked={allChecked}
                                    onChange={() => toggleAllPeriodsForTf(tf)}
                                    style={{ marginRight: "4px", width: 14, height: 14 }}
                                />
                                Все
                            </label>
                            {emaPeriods.map(period => {
                                const checked = Array.isArray(config[tf]) && (config[tf] as string[]).includes(period);
                                return (
                                    <label key={period} style={{ display: "flex", alignItems: "center", cursor: "pointer", fontSize: "12px", margin: 0 }}>
                                        <input
                                            type="checkbox"
                                            checked={checked}
                                            onChange={() => togglePeriodForTf(tf, period)}
                                            style={{ marginRight: "4px", width: 14, height: 14 }}
                                        />
                                        EMA{period}
                                    </label>
                                );
                            })}
                        </div>
                    </div>
                );
            })}
        </div>
    );
} 