WITH rates_window AS (
    SELECT
        rate_date,
        char_code,
        rate_per_unit,
        COUNT(*) OVER (
            PARTITION BY char_code
            ORDER BY rate_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS window_days_count,
        lagInFrame(rate_per_unit, 1) OVER (
            PARTITION BY char_code 
            ORDER BY rate_date
        ) AS prev_rate_per_unit
    FROM postgresql(
        '{{ var.value.dwh_pg_host }}:{{ var.value.dwh_pg_port }}',
        '{{ var.value.dwh_pg_db }}',
        'rates',
        '{{ var.value.dwh_pg_user }}',
        '{{ var.value.dwh_pg_password }}'
    )
    WHERE rate_date BETWEEN toDate('{{ ds }}') - 29 AND toDate('{{ ds }}')
),
staging_calcs AS(
    SELECT
        char_code,
        rate_date,
        rate_per_unit,
        window_days_count,
        rate_per_unit / prev_rate_per_unit * 100 - 100  AS daily_return_pct,
        rate_per_unit - prev_rate_per_unit              AS daily_return_abs
    FROM rates_window
),
staging_aggs AS(
    SELECT
        char_code,
        rate_date,
        rate_per_unit,
        daily_return_pct,
        daily_return_abs,
        window_days_count,
        AVG(rate_per_unit) OVER (
            PARTITION BY char_code 
            ORDER BY rate_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )                                               AS rolling_avg_30d,
        stddevPop(daily_return_pct) OVER (
            PARTITION BY char_code 
            ORDER BY rate_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )                                               AS rolling_volatility_30d
    FROM staging_calcs
)
INSERT INTO mart.rates_volatility
SELECT
    char_code                               AS currency_code,
    rate_date,
    rate_per_unit,
    daily_return_pct,
    daily_return_abs,
    rolling_avg_30d,
    rolling_volatility_30d,
    (rate_per_unit - rolling_avg_30d) /
        nullIf(rolling_volatility_30d, 0)   AS z_score,
    abs(z_score) > 2                        AS is_anomaly,
    now()                                   AS version
FROM staging_aggs
WHERE window_days_count = 30









