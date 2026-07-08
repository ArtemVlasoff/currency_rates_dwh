{{ config(
    materialized='table',
    engine='ReplacingMergeTree(version)',
    order_by=['currency_code', 'week_start_date']
) }}

SELECT
    char_code                                       AS currency_code,
    toStartOfWeek(rate_date, 1)                     AS week_start_date,
    toStartOfWeek(rate_date, 1) + 6                 AS week_end_date,
    argMin(rate_per_unit, rate_date)                AS open_rate,
    argMax(rate_per_unit, rate_date)                AS close_rate,
    min(rate_per_unit)                              AS min_rate,
    max(rate_per_unit)                              AS max_rate,
    toDecimal64(round(avg(rate_per_unit), 4), 4)    AS avg_rate,
    count()                                         AS days_count,
    now()                                           AS version
FROM {{ postgres_source() }}
GROUP BY char_code, toStartOfWeek(rate_date, 1)