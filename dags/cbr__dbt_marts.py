from datetime import datetime
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import dag
from docker.types import Mount

@dag(
    schedule=None,
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1
)

def cbr_clickhouse_marts_pipeline():
    DockerOperator(
        task_id='build_dbt_marts',
        image='dwh-dbt',
        command=(
            "dbt "
            "run "
            "--select "
            "rates_weekly "
            "rates_volatility "
            "--vars "
            "'{\"rate_date\": \"{{ dag_run.conf['rate_date'] }}\"}'"
        ),
        network_mode='dwh_pipeline-net',
        mounts=[
            Mount(
                source='/home/artem/dwh/dbt/project',
                target='/dbt',
                type='bind'
            ),
            Mount(
                source='/home/artem/dwh/dbt/dbt_profile',
                target='/root/.dbt',
                type='bind'
            )
        ],
        environment={
            'DBT_POSTGRES_HOST': '{{ var.value.dwh_pg_host }}',
            'DBT_POSTGRES_PORT': '{{ var.value.dwh_pg_port }}',
            'DBT_POSTGRES_DB': '{{ var.value.dwh_pg_db }}',
            'DBT_POSTGRES_USER': '{{ var.value.dwh_pg_user }}',
            'DBT_POSTGRES_PASSWORD': '{{ var.value.dwh_pg_password }}',
        },
        auto_remove='success',
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False
    )

cbr_clickhouse_marts_pipeline()