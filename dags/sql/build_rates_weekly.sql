INSERT INTO mart.rates_weekly
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
FROM postgresql(
    '{{ var.value.dwh_pg_host }}:{{ var.value.dwh_pg_port }}',
    '{{ var.value.dwh_pg_db }}',
    'rates',
    '{{ var.value.dwh_pg_user }}',
    '{{ var.value.dwh_pg_password }}'
)
WHERE rate_date >= '{{ data_interval_start.start_of("week").to_date_string() }}'
  AND rate_date <= '{{ data_interval_start.end_of("week").to_date_string() }}'
GROUP BY char_code, toStartOfWeek(rate_date, 1);