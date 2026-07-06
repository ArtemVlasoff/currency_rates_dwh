from dotenv import load_dotenv
import os
import sys
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

load_dotenv()

DWH_DBNAME = os.environ.get('DWH_DBNAME')
DWH_USER = os.environ.get('DWH_USER')
DWH_PASSWORD = os.environ.get('DWH_PASSWORD')
DWH_HOST = os.environ.get('DWH_HOST')
DWH_PORT = os.environ.get('DWH_PORT')

CURRENCIES = ('USD', 'EUR', 'GBP', 'CNY', 'JPY', 'CHF', 'HKD', 'TRY')
rate_date  = sys.argv[1]

## EXTRACT & TRANSFORM

# Create Spark session
spark = ( 
    SparkSession.builder
    .appName("cbr_rates_transform_and_load")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2,org.postgresql:postgresql:42.7.4")
    .config("spark.hadoop.fs.s3a.endpoint", "http://172.23.32.93:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

df_raw  = spark.read.parquet(f"s3a://raw/currencies/date={rate_date }/currencies.parquet")

# Transform raw data to required format
df_result = (
    df_raw 
    .filter(F.col('CharCode').isin(*CURRENCIES))
    .withColumn('Value', F.regexp_replace(F.col('Value'), ',', '.').cast(DecimalType(10, 4)))
    .withColumn('Nominal', F.col('Nominal').cast('integer'))
    .withColumn('rate_per_unit', (F.col('Value') / F.col('Nominal')).cast(DecimalType(10, 4)))
    .withColumn('rate_date', F.lit(rate_date ).cast('date'))
    .withColumn('loaded_at', F.current_timestamp())
    .withColumnRenamed('CharCode', 'char_code')
    .withColumnRenamed('Value', 'value')
    .withColumnRenamed('Nominal', 'nominal')
    .select('rate_date', 'char_code', 'value', 'nominal', 'rate_per_unit', 'loaded_at')
)

## IDEMPOTENT CLEANUP
conn = None
try:    
    conn = psycopg2.connect(
        dbname=DWH_DBNAME,
        user=DWH_USER,
        password=DWH_PASSWORD,
        host=DWH_HOST,
        port=DWH_PORT
    )

    print('Successfully connected')

    cur = conn.cursor()
    cur.execute("DELETE FROM rates WHERE rate_date = %s", (rate_date ,))
    conn.commit()
    cur.close()

except Exception as e:
    print(f'Unable to connect to the database: {e}')
    raise

finally:
    if conn is not None:
        conn.close()

## LOAD
jdbc_url = f"jdbc:postgresql://{DWH_HOST}:{DWH_PORT}/{DWH_DBNAME}"

jdbc_properties = {
    "user": DWH_USER,
    "password": DWH_PASSWORD,
    "driver": "org.postgresql.Driver"
}

df_result.write.jdbc(
    url=jdbc_url,
    table="rates",
    mode="append",
    properties=jdbc_properties
)