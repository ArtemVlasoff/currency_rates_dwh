{% macro postgres_source() %}
postgresql(
    '{{ env_var("DBT_POSTGRES_HOST") }}:{{ env_var("DBT_POSTGRES_PORT") }}',
    '{{ env_var("DBT_POSTGRES_DB") }}',
    'rates',
    '{{ env_var("DBT_POSTGRES_USER") }}',
    '{{ env_var("DBT_POSTGRES_PASSWORD") }}'
)
{% endmacro %}