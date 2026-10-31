import os
import json
import time
from datetime import datetime
import base64
import requests
import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import logging
from dotenv import load_dotenv

# Load biến môi trường từ file .env ở thư mục gốc
load_dotenv()

# --- CẤU HÌNH HỆ THỐNG LOGGING ---
LOG_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '../logs'))
os.makedirs(LOG_DIR, exist_ok=True)
log_file_path = os.path.join(LOG_DIR, 'ebay_ingest_execution.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8'),
        logging.StreamHandler() # Vẫn hiển thị log ra terminal/airflow UI
    ]
)
logger = logging.getLogger("eBayIngestPipeline")

CLIENT_ID = os.getenv("EBAY_CLIENT_ID")
CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-2")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
BASE_PREFIX = os.getenv("BUCKET_BASE_PREFIX", "bronze/ebay_api")

TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
TAXONOMY_URL = "https://api.ebay.com/commerce/taxonomy/v1/category_tree/0"

def get_access_token():
    """Lấy OAuth Token từ eBay API kèm ghi log chi tiết lỗi"""
    credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": f"Basic {encoded_credentials}",
    }
    data = {
        "grant_type": "client_credentials",
        "scope": "https://api.ebay.com/oauth/api_scope",
    }
    try:
        res = requests.post(TOKEN_URL, headers=headers, data=data, timeout=10)
        if res.status_code == 200:
            logger.info("Lấy OAuth Token thành công.")
            return res.json().get("access_token")
        logger.error(f"Lỗi lấy Token từ eBay API - Status: {res.status_code}, Response: {res.text}")
    except Exception as e:
        logger.critical(f"Ngoại lệ khi kết nối đến eBay Token API: {e}")
    return None


def get_all_top_categories(token):
    """Lấy danh sách tất cả các danh mục chính từ eBay Taxonomy API"""
    headers = {"Authorization": f"Bearer {token}"}
    try:
        res = requests.get(TAXONOMY_URL, headers=headers, timeout=10)
        if res.status_code == 200:
            nodes = (
                res.json()
                .get("rootCategoryNode", {})
                .get("childCategoryTreeNodes", [])
            )
            categories = [
                {
                    "id": node["category"]["categoryId"],
                    "name": node["category"]["categoryName"],
                }
                for node in nodes
            ]
            logger.info(f"Đã lấy thành công {len(categories)} danh mục gốc từ Taxonomy API.")
            return categories
        logger.error(f"Lỗi lấy cây danh mục - Status: {res.status_code}, Response: {res.text}")
    except Exception as e:
        logger.critical(f"Ngoại lệ khi gọi Taxonomy API: {e}")
    return []


def scrape_all_ebay_categories():
    """
    Duyệt qua các danh mục chính, cào dữ liệu sản phẩm từ eBay Browse API
    kết hợp ghi log toàn bộ tiến trình và cơ chế xử lý lỗi ngoại lệ.
    """
    token = get_access_token()
    if not token:
        logger.error("Không thể khởi tạo token, hủy tiến trình cào dữ liệu.")
        return []

    categories = get_all_top_categories(token)
    logger.info(f"=== BẮT ĐẦU CÀO DỮ LIỆU CHO {len(categories)} DANH MỤC ===")

    all_raw_items = []

    for cat in categories:
        cat_id = cat["id"]
        cat_name = cat["name"]
        logger.info(f"Đang quét danh mục: {cat_name} (ID: {cat_id})")

        offset = 0
        limit = 100
        max_safe_offset = 5000  
        retry_count = 0
        max_retries = 3         

        while offset < max_safe_offset:
            headers = {"Authorization": f"Bearer {token}"}
            params = {
                "category_ids": cat_id,
                "limit": limit,
                "offset": offset,
            }

            try:
                res = requests.get(SEARCH_URL, headers=headers, params=params, timeout=15)
            except Exception as e:
                logger.warning(f"Lỗi kết nối mạng khi cào danh mục {cat_id} tại offset {offset}: {e}. Thử lại...")
                time.sleep(2)
                continue

            if res.status_code == 401:
                retry_count += 1
                if retry_count > max_retries:
                    logger.error(f"❌ Lỗi 401 Unauthorized liên tiếp {max_retries} lần ở danh mục {cat_id}. Bỏ qua.")
                    break
                logger.warning(f"Token hết hạn (Lần thử {retry_count}/{max_retries}), đang xin token mới...")
                token = get_access_token()
                if not token:
                    break
                continue

            if res.status_code == 429:
                retry_count += 1
                if retry_count > max_retries:
                    logger.error(f"❌ Vượt quá giới hạn Rate Limit (429) {max_retries} lần. Bỏ qua danh mục.")
                    break
                logger.warning(f"Chạm giới hạn Rate Limit API! Tạm nghỉ 60 giây (Lần {retry_count})...")
                time.sleep(60)
                continue

            if res.status_code != 200:
                logger.error(f"Bỏ qua danh mục {cat_id} do lỗi HTTP {res.status_code}: {res.text}")
                break

            retry_count = 0
            data = res.json()
            items = data.get("itemSummaries", [])

            if not items:
                logger.info(f"Đã duyệt hết sản phẩm hoặc không còn dữ liệu trong danh mục: {cat_name}")
                break

            for item in items:
                item["scraped_category_id"] = cat_id
                item["scraped_category_name"] = cat_name
                item["scraped_at"] = datetime.now().isoformat()
                all_raw_items.append(item)

            offset += limit
            time.sleep(0.3)

    logger.info(f"Hoàn tất quá trình cào. Tổng số lượng bản ghi thu về: {len(all_raw_items)}")
    return all_raw_items


def ensure_s3_bucket_exists(s3_client, bucket_name, region):
    """Kiểm tra S3 Bucket và ghi log trạng thái."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"S3 Bucket '{bucket_name}' đã tồn tại và sẵn sàng.")
    except Exception:
        logger.warning(f"S3 Bucket '{bucket_name}' chưa tồn tại. Đang tiến hành tạo mới...")
        try:
            if region == "us-east-1":
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            logger.info(f"Đã tạo thành công S3 Bucket: '{bucket_name}'")
        except Exception as e:
            logger.critical(f"❌ Lỗi nghiêm trọng khi tạo S3 Bucket: {e}")
            raise e


def upload_parquet_partitioned_to_s3(data_list, target_date=None):
    """Chuyển đổi DataFrame sang Parquet, phân chia thư mục theo partition ngày và ghi log chi tiết."""
    if not data_list:
        logger.warning("Danh sách dữ liệu trống, không có dữ liệu để đẩy lên S3.")
        return

    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

        ensure_s3_bucket_exists(s3_client, BUCKET_NAME, AWS_REGION)

        df = pd.DataFrame(data_list)

        if target_date is None:
            target_date = datetime.now()
            
        year = target_date.strftime("%Y")
        month = target_date.strftime("%m")
        day = target_date.strftime("%d")
        timestamp_str = datetime.now().strftime("%H%M%S")

        s3_prefix = f"{BASE_PREFIX}/year={year}/month={month}/day={day}"
        file_name = f"ebay_raw_data_{timestamp_str}.parquet"
        s3_object_key = f"{s3_prefix}/{file_name}"

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy', index=False)
        parquet_buffer.seek(0)

        logger.info(f"Đang tải file Parquet lên S3 tại s3://{BUCKET_NAME}/{s3_object_key}...")
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_object_key,
            Body=parquet_buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        logger.info(f"✅ Đã đẩy thành công {len(data_list)} bản ghi lên S3 (Partition: year={year}/month={month}/day={day})!")
    except Exception as e:
        logger.error(f"❌ Lỗi ngoại lệ khi đẩy file Parquet lên S3: {e}")
        raise e


if __name__ == "__main__":
    raw_data = scrape_all_ebay_categories()
    upload_parquet_partitioned_to_s3(raw_data)