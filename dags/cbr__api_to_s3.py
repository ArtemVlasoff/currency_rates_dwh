from datetime import datetime
import requests
import pyarrow as pa
import pyarrow.parquet as pq
import io
from botocore.exceptions import ClientError
import xmltodict

from airflow.sdk import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

CBR_BASE_URL = 'http://www.cbr.ru/scripts/XML_daily.asp'

@dag(
    schedule='@daily',
    start_date=datetime(2026, 6, 1),
    catchup=True,
    max_active_runs=1
)

def cbr_currencies_pipeline():

    @task
    def pull_from_api(**context) -> list[dict]:   
        
        url = f'{CBR_BASE_URL}?date_req={datetime.strptime(context["ds"], '%Y-%m-%d').strftime("%d/%m/%Y")}'

        response = requests.get(url)
        response.raise_for_status()

        raw_data = xmltodict.parse(response.content)['ValCurs']['Valute']

        return raw_data

    @task
    def push_to_s3(raw_data: list[dict], **context):
        
        if not raw_data:
            raise ValueError("Data is empty")

        table = pa.Table.from_pylist(raw_data)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        hook = S3Hook(aws_conn_id='minio_default')
        bucket = 'raw'
        s3_key = f'currencies/date={context["ds"]}/currencies.parquet'

        try:
            hook.create_bucket(bucket_name='raw')
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code not in ('BucketAlreadyOwnedByYou', 'BucketAlreadyExists'):
                raise

        hook.load_file_obj(
            file_obj=buffer,
            key=s3_key,
            bucket_name=bucket,
            replace=True
        )

    raw_data = pull_from_api()
    push_to_s3_task = push_to_s3(raw_data)

    trigger_transform = TriggerDagRunOperator(
        task_id='trigger__cbr_transform_pipeline',
        trigger_dag_id='cbr_transform_pipeline',
        conf={"rate_date": "{{ ds }}"},
        wait_for_completion=True
    )

    push_to_s3_task >> trigger_transform

cbr_currencies_pipeline()