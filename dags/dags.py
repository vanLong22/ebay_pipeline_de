from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
import sys
import os
import logging

# Thiết lập ghi log cho DAG
logger = logging.getLogger("AirflowDAG.EbayPipeline")

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

DAG_DIR = os.path.dirname(os.path.abspath(__file__))
DBT_PROJECT_DIR = os.path.abspath(os.path.join(DAG_DIR, '../jaffle_shop'))
DBT_PROFILES_DIR = DBT_PROJECT_DIR

sys.path.append(os.path.abspath(os.path.join(DAG_DIR, '../include')))
from include.ebay_ingest import scrape_all_ebay_categories, upload_parquet_partitioned_to_s3

default_args = {
    'owner': 'ebay_data_engineer',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['longle18704@gmail.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def run_ebay_pipeline(**kwargs):
    """Hàm bọc gọi tiến trình cào và đẩy S3 có bắt lỗi ngoại lệ chi tiết"""
    try:
        ds = kwargs['ds'] 
        execution_date_obj = datetime.strptime(ds, '%Y-%m-%d')
        logger.info(f"Bắt đầu chạy luồng cào dữ liệu eBay cho ngày phân vùng: {ds}")
        
        raw_data = scrape_all_ebay_categories()
        if not raw_data:
            logger.warning("Cảnh báo: Không thu thập được bản ghi nào từ API eBay trong kỳ chạy này.")
            
        upload_parquet_partitioned_to_s3(raw_data, target_date=execution_date_obj)
        logger.info("Hoàn thành xuất sắc nhiệm vụ cào dữ liệu và đồng bộ S3.")
    except Exception as e:
        logger.error(f"❌ Lỗi nghiêm trọng xảy ra trong Task Python cào dữ liệu: {e}")
        raise e


with DAG(
    dag_id='ebay_end_to_end_pipeline',
    default_args=default_args,
    description='Pipeline tự động: Cào eBay -> S3 -> Snowflake Bronze -> dbt Transformation & Test',
    schedule_interval='@daily',
    start_date=datetime(2026, 10, 1),
    catchup=False,
    tags=['ebay', 'bronze', 'silver', 'gold', 'dbt'],
) as dag:

    ingest_bronze_task = PythonOperator(
        task_id='scrape_and_push_to_s3_bronze',
        python_callable=run_ebay_pipeline,
        provide_context=True,
    )

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
            FILES = ('/year={{ ds.split("-")[0] }}/month={{ ds.split("-")[1] }}/day={{ ds.split("-")[2] }}/')
            FILE_FORMAT = (FORMAT_NAME = ebay_raw_parquet_format) 
            ON_ERROR = 'CONTINUE';
        """,
    )

    dbt_build_pipeline = BashOperator(
        task_id='dbt_build_entire_pipeline',
        bash_command=(
            f'dbt deps --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} && '
            f'dbt build --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR} --select +dq_check ebay_data_cleaned ebay_data_json_expanded marts'
        ),
    )

    ingest_bronze_task >> load_to_snowflake_bronze >> dbt_build_pipeline