from datetime import datetime

from airflow.sdk import dag
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator

@dag(
    dag_id='cbr_clickhouse_rates_volatility_pipeline',
    schedule='@daily',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    template_searchpath="/opt/airflow/dags/sql",
    tags=["clickhouse", "mart"],
)

def cbr_clickhouse_rates_volatility_pipeline():
    ClickHouseOperator(
        task_id='build_rates_volatility_mart',
        sql='build_rates_volatility.sql',
        clickhouse_conn_id='clickhouse'
    )

cbr_clickhouse_rates_volatility_pipeline()