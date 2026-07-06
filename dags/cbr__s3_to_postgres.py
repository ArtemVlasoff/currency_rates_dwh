from datetime import datetime
from airflow.providers.ssh.operators.ssh import SSHOperator

from airflow.sdk import dag, task

@dag(
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1
)

def cbr_transform_pipeline():
    SSHOperator(
        task_id='run_spark_transform',
        ssh_conn_id='ssh_wsl_host',
        cmd_timeout=300,
        command=(
            'PYSPARK_PYTHON=~/dwh/.venv/bin/python '
            '~/dwh/.venv/bin/spark-submit --packages '
            'org.apache.hadoop:hadoop-aws:3.4.2,org.postgresql:postgresql:42.7.4 '
            '~/dwh/scripts/transform_and_load_from_s3_to_dwh.py '
            '{{ dag_run.conf["rate_date"] }}'
        )
    )

cbr_transform_pipeline()