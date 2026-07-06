CREATE TABLE mart.rates_weekly (
    currency_code   LowCardinality(String),
    week_start_date Date,
    week_end_date   Date,
    open_rate       Decimal(10, 4),
    close_rate      Decimal(10, 4),
    min_rate        Decimal(10, 4),
    max_rate        Decimal(10, 4),
    avg_rate        Decimal(10, 4),
    days_count      UInt8,
    version         DateTime
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (currency_code, week_start_date);