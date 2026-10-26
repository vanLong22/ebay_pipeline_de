from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import sys
import os 

# Thiết lập đường dẫn
DAG_DIR = os.path.dirname(os.path.abspath(__file__))
DBT_PROJECT_DIR = os.path.abspath(os.path.join(DAG_DIR, '../jaffle_shop'))
DBT_PROFILES_DIR = DBT_PROJECT_DIR

sys.path.append(os.path.abspath(os.path.join(DAG_DIR, '../include')))
from include.ebay_ingest import scrape_all_ebay_categories, upload_raw_to_s3

default_args = {
    'owner': 'ebay_data_engineer',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['longle18704@gmail.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_ebay_pipeline():
    raw_data = scrape_all_ebay_categories(max_items_per_category=200)
    upload_raw_to_s3(raw_data)

with DAG(
    dag_id='ebay_end_to_end_pipeline',
    default_args=default_args,
    description='Pipeline tự động: Cào eBay -> S3 -> Snowflake Bronze -> dbt Transformation & Test',
    schedule_interval='@daily',
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=['ebay', 'bronze', 'silver', 'gold', 'dbt'],
) as dag:

    # Task 1: Cào dữ liệu từ eBay và đẩy lên AWS S3
    ingest_bronze_task = PythonOperator(
        task_id='scrape_and_push_to_s3_bronze',
        python_callable=run_ebay_pipeline,
    )

    # Task 2: Gọi lệnh COPY INTO trên Snowflake để load dữ liệu từ S3 vào bảng raw_ebay_listings (Bronze)
    load_to_snowflake_bronze = SnowflakeOperator(
        task_id='load_s3_to_snowflake_bronze',
        snowflake_conn_id='snowflake_default',
        sql="""
            USE DATABASE EBAY_DB;
            USE SCHEMA BRONZE;

            COPY INTO raw_ebay_listings (raw_data, src_file_name)
            FROM (
            SELECT 
                $1, 
                METADATA$FILENAME 
            FROM @ebay_s3_stage/
            )
            FILES = ('year={{ execution_date.strftime("%Y") }}/month={{ execution_date.strftime("%m") }}/day={{ execution_date.strftime("%d") }}/')
            FILE_FORMAT = (FORMAT_NAME = ebay_raw_parquet_format) 
            ON_ERROR = 'CONTINUE';
        """,
    )


    # Task 3: Chạy dbt build để biến đổi dữ liệu (Staging -> Cleansed -> Marts Kimball)
    dbt_build_marts_task = BashOperator(
        task_id='dbt_build_marts',
        bash_command=f'dbt build --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --select stg_ebay_raw ebay_data_cleaned dq_check marts',
    )

    # Task 4: Chạy dbt test kiểm tra toàn vẹn tầng Marts
    dbt_test_marts_task = BashOperator(
        task_id='dbt_test_marts',
        bash_command=f'dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --select marts',
    )

    # Thiết lập luồng chạy tự động tuần tự (Lineage)
    ingest_bronze_task >> load_to_snowflake_bronze >> dbt_build_marts_task >> dbt_test_marts_task