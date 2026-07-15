"""
Загрузка RAW данных из S3 в PostgreSQL
"""
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.hooks.base import BaseHook
import pendulum
import duckdb
import logging

# Config
OWNER = 'kd'
DAG_ID = 'raw_from_s3_to_pg'

# Used tables in DAG
LAYER = 'raw'
SOURCE = 'earthquake'
SCHEMA = 'ods'
TARGET_TABLE = 'fct_earthquake'

# s3
ACCESS_KEY = Variable.get('access_key')
SECRET_KEY = Variable.get('secret_key')

# PostgreSQL
conn = BaseHook.get_connection("dwh_postgres")

host_pg = conn.host
port_pg = conn.port
database_pg = conn.schema
user_pg = conn.login
password_pg = conn.password

DEFAULT_ARGS = {
    'owner': OWNER,
    'catchup': True,
    'retries': 3,
    'retry_delay': pendulum.duration(hours=1)
}

def get_dates(**context) -> tuple[str, str]:
    
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date


def get_and_transfer_raw_data_to_ods_pg(**context):
    

    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    con = duckdb.connect()

    con.sql(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        CREATE SECRET dwh_postgres (
            TYPE postgres,
            HOST '{host_pg}',
            PORT {port_pg},
            DATABASE '{database_pg}',
            USER '{user_pg}',
            PASSWORD '{password_pg}'
        );

        ATTACH '' AS dwh_postgres_db (TYPE postgres, SECRET dwh_postgres);

        INSERT INTO dwh_postgres_db.{SCHEMA}.{TARGET_TABLE}
        (
            time,
            latitude,
            longitude,
            depth,
            mag,
            mag_type,
            nst,
            gap,
            dmin,
            rms,
            net,
            id,
            updated,
            place,
            type,
            horizontal_error,
            depth_error,
            mag_error,
            mag_nst,
            status,
            location_source,
            mag_source
        )
        SELECT
            time,
            latitude,
            longitude,
            depth,
            mag,
            magType AS mag_type,
            nst,
            gap,
            dmin,
            rms,
            net,
            id,
            updated,
            place,
            type,
            horizontalError AS horizontal_error,
            depthError AS depth_error,
            magError AS mag_error,
            magNst AS mag_nst,
            status,
            locationSource AS location_source,
            magSource AS mag_source
        FROM 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
        """,
    )

    con.close()
    logging.info(f"✅ Download for date success: {start_date}")

with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(2026, 6, 15),
    schedule_interval="0 12 * * *",
    tags=['ods', 's3', 'PostgreSQL'],
    catchup=True,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
) as dag:

    dag.doc_md = __doc__
    
    start = EmptyOperator(task_id='start')

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_api_to_s3",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,  # длительность работы сенсора
        poke_interval=30,  # частота проверки
    )

    get_and_transfer_raw_data_to_ods_pg = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=get_and_transfer_raw_data_to_ods_pg,
    )

    end = EmptyOperator(task_id='end')

    start >> sensor_on_raw_layer >> get_and_transfer_raw_data_to_ods_pg >> end