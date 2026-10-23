from datetime import datetime, timedelta
from airflow import DAG 
from airflow.operators.python import PythonOperator
import sys
import os 

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../include')))
# import hàm cào dữ liệu và đẩy lên s3
from include.ebay_ingest import scrape_all_ebay_categories, upload_raw_to_s3

# cấu hình DAG
default_args = {
    'owner': 'ebay_data_engineer',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['longle18704@gmail.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=6),

}

# hàm trung gian để cào dữ liệu và đẩy lên s3
def run_ebay_pipeline():
    # cào dữ liệu từ eBay
    raw_data = scrape_all_ebay_categories(max_items_per_category=200)

    # đẩy dữ liệu lên S3 bronze layer
    upload_raw_to_s3(raw_data)


# khởi tạo DAG
with DAG(
    dag_id = 'ebay_bronze_ingestion_pipeline',
    default_args=default_args,
    description='DAG để cào dữ liệu từ eBay và đẩy lên S3 bronze layer',
    schedule_interval='@daily',  # chạy lúc 00:00 hàng ngày
    start_date=datetime(2026, 10, 1),
    catchup=False,   # bỏ qua toàn bộ các lịch chạy cũ trong quá khứ đi, chỉ chạy phiênngày hiện tại 
    tags=['ebay', 'bronze', 'ingestion'],
) as dag:

    ingest_task = PythonOperator(
        task_id='scrape_and_push_to_s3_bronze',
        python_callable=run_ebay_pipeline,
    )
    ingest_task