"use client";

import { useState, useEffect } from "react";
import ChartWrapper from "@/components/ChartWrapper";
import TimeframeSelector from "@/components/TimeframeSelector";
import { ChartConfig, fetchConfig } from "@/lib/config";

export default function Home() {
  const [timeframe, setTimeframe] = useState("1h");
  const [config, setConfig] = useState<ChartConfig | null>(null);
  const [isLoadingConfig, setIsLoadingConfig] = useState(true);

  // Загрузка конфигурации при монтировании
  useEffect(() => {
    const loadConfig = async () => {
      try {
        setIsLoadingConfig(true);
        const chartConfig = await fetchConfig();
        setConfig(chartConfig);
        const defaultTf = chartConfig.timeframes.find(tf => tf === "1h") || chartConfig.timeframes[0];
        setTimeframe(defaultTf);

      } catch (error) {
        if (process.env.NODE_ENV === "development") {
          console.error("Ошибка загрузки конфигурации:", error);
        }
      } finally {
        setIsLoadingConfig(false);
      }
    };

    loadConfig();
  }, []);

  return (
    <div
      style={{
        width: "100%",
        minHeight: "100vh",
        padding: "20px",
        backgroundColor: "#111",
        color: "#ccc",
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        boxSizing: "border-box"
      }}
    >
      <div
        style={{
          maxWidth: "1200px",
          width: "100%",
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          gap: "20px"
        }}
      >
        <h1
          style={{
            fontSize: "24px",
            fontWeight: "bold",
            marginBottom: "10px",
            textAlign: "center"
          }}
        >
          Bybit Trading Chart
        </h1>

        {!isLoadingConfig && config && (
          <TimeframeSelector
            config={config}
            currentTimeframe={timeframe}
            onTimeframeChange={setTimeframe}
          />
        )}

        <div
          style={{
            width: "100%",
            border: "1px solid #333",
            borderRadius: "8px",
            overflow: "hidden"
          }}
        >
          {!isLoadingConfig && config && (
            <ChartWrapper symbol="BTCUSDT" timeframe={timeframe} emaPeriods={config.ema_periods} />
          )}

        </div>
      </div>
    </div>
  );
}
