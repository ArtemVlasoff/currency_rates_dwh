CREATE TABLE mart.rates_volatility (
    currency_code           LowCardinality(String),
    rate_date               Date,
    rate_per_unit           Decimal(10, 4),
    daily_return_pct        Decimal(10, 4),
    daily_return_abs        Decimal(10, 4),
    rolling_avg_30d         Decimal(10, 4),
    rolling_volatility_30d  Decimal(10, 4),
    z_score                 Float64,
    is_anomaly              UInt8,
    version                 DateTime
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (currency_code, rate_date);