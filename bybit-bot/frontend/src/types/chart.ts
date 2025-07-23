export type CandleApiData = {
    timestamp: number;
    open: number;
    high: number;
    low: number;
    close: number;
    volume?: number;
};

export type EmaPoint = {
    time: number;
    value: number;
};

// EMA данные по периодам для одного таймфрейма
export type EmaByPeriod = {
    [period: string]: EmaPoint[]; // например "20": [EmaPoint, ...]
};

// EMA данные по таймфреймам (каждый ТФ содержит периоды)
export type EmaByTimeframe = {
    [timeframe: string]: EmaByPeriod; // например "1h": {"20": [EmaPoint, ...]}
};

// Ответ API для /candles эндпоинта
export type CandlesApiResponse = {
    candles: CandleApiData[];
    ema: EmaByTimeframe;
};

// Ответ API для /indicators/ema эндпоинта  
export type EmaApiResponse = EmaByTimeframe;

// Конфигурация для отображения EMA
export type EmaDisplayConfig = {
    enabled: boolean;
    [timeframe: string]: string[] | boolean;
};

// Настройки цветов для EMA линий
export type EmaLineStyle = {
    color: string;
    lineWidth: number;
    lineStyle: number; // 0 = solid, 1 = dotted, 2 = dashed
    opacity?: number;
};

export type SignalPoint = {
    time: number;
    price: number;
    type: "buy" | "sell";
    // можно добавить другие поля позже
};
