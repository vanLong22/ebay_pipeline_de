# import os
# import json
# import time
# from datetime import datetime
# import base64
# import requests
# import boto3
# from dotenv import load_dotenv

# # Load biến môi trường từ file .env ở thư mục gốc
# load_dotenv()

# CLIENT_ID = os.getenv("EBAY_CLIENT_ID")
# CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")
# AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
# AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
# AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-2")
# BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
# SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
# TAXONOMY_URL = "https://api.ebay.com/commerce/taxonomy/v1/category_tree/0"


# def get_access_token():
#     """Lấy OAuth Token từ eBay API"""
#     credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"
#     encoded_credentials = base64.b64encode(credentials.encode()).decode()
#     headers = {
#         "Content-Type": "application/x-www-form-urlencoded",
#         "Authorization": f"Basic {encoded_credentials}",
#     }
#     data = {
#         "grant_type": "client_credentials",
#         "scope": "https://api.ebay.com/oauth/api_scope",
#     }
#     res = requests.post(TOKEN_URL, headers=headers, data=data)
#     if res.status_code == 200:
#         return res.json().get("access_token")
#     print("Lỗi lấy Token:", res.text)
#     return None


# def get_all_top_categories(token):
#     """Lấy danh sách tất cả các danh mục chính từ eBay Taxonomy API"""
#     headers = {"Authorization": f"Bearer {token}"}
#     res = requests.get(TAXONOMY_URL, headers=headers)
#     if res.status_code == 200:
#         nodes = (
#             res.json()
#             .get("rootCategoryNode", {})
#             .get("childCategoryTreeNodes", [])
#         )
#         return [
#             {
#                 "id": node["category"]["categoryId"],
#                 "name": node["category"]["categoryName"],
#             }
#             for node in nodes
#         ]
#     print("Lỗi lấy cây danh mục:", res.text)
#     return []


# def scrape_all_ebay_categories():
#     """
#     Duyệt qua các danh mục chính, cào dữ liệu sản phẩm từ eBay Browse API
#     không giới hạn cứng số lượng, kết hợp cơ chế an toàn chống lặp vô hạn (Retry & Offset < 5000).
#     """
#     token = get_access_token()
#     if not token:
#         return []

#     categories = get_all_top_categories(token)
#     print(f"=== TÌM THẤY {len(categories)} DANH MỤC CHÍNH ĐỂ CÀO ===")

#     all_raw_items = []

#     for cat in categories:
#         cat_id = cat["id"]
#         cat_name = cat["name"]
#         print(f"\n>>> Đang quét danh mục: {cat_name} (ID: {cat_id})")

#         offset = 0
#         limit = 100
#         max_safe_offset = 5000  # Ngưỡng an toàn tối đa theo yêu cầu
#         retry_count = 0
#         max_retries = 3        # Giới hạn số lần thử lại liên tiếp khi gặp lỗi hệ thống/rate limit

#         while offset < max_safe_offset:
#             headers = {"Authorization": f"Bearer {token}"}
#             params = {
#                 "category_ids": cat_id,
#                 "limit": limit,
#                 "offset": offset,
#             }

#             res = requests.get(SEARCH_URL, headers=headers, params=params)

#             # 1. Xử lý Token hết hạn (401) có kèm cơ chế đếm số lần retry
#             if res.status_code == 401:
#                 retry_count += 1
#                 if retry_count > max_retries:
#                     print(f"❌ Lỗi 401 liên tiếp {max_retries} lần. Bỏ qua danh mục này để tránh kẹt vòng lặp.")
#                     break
#                 print(f"Token hết hạn (Lần thử {retry_count}/{max_retries}), đang đổi token mới...")
#                 token = get_access_token()
#                 if not token:
#                     break
#                 continue

#             # 2. Xử lý Rate Limit (429) có kèm cơ chế đếm số lần retry
#             if res.status_code == 429:
#                 retry_count += 1
#                 if retry_count > max_retries:
#                     print(f"❌ Chạm giới hạn Rate Limit (429) {max_retries} lần liên tiếp. Bỏ qua danh mục.")
#                     break
#                 print(f"Chạm giới hạn API! Tạm nghỉ 60 giây (Lần thử {retry_count}/{max_retries})...")
#                 time.sleep(60)
#                 continue

#             if res.status_code != 200:
#                 print(f"Bỏ qua danh mục do lỗi {res.status_code}: {res.text}")
#                 break

#             # Nếu request thành công, reset lại biến đếm retry
#             retry_count = 0

#             data = res.json()
#             items = data.get("itemSummaries", [])

#             # 3. Điều kiện ngắt vòng lặp (break) nếu API trả về danh sách rỗng hoặc hết trang
#             if not items:
#                 print("Đã duyệt hết sản phẩm hoặc không còn dữ liệu trong danh mục này.")
#                 break

#             # Gắn thêm metadata danh mục vào từng item thô
#             for item in items:
#                 item["scraped_category_id"] = cat_id
#                 item["scraped_category_name"] = cat_name
#                 all_raw_items.append(item)

#             offset += limit
#             print(f"   + Tổng tích lũy toàn hệ thống: {len(all_raw_items)} sản phẩm...")
#             time.sleep(0.3)

#     return all_raw_items


# def upload_raw_to_s3(data_list):
#     """
#     Đẩy toàn bộ dữ liệu thô (Raw JSON) trực tiếp lên S3 tầng Bronze: bronze/ebay_api/
#     """
#     if not data_list:
#         print("Không có dữ liệu để đẩy lên S3.")
#         return

#     s3_client = boto3.client(
#         's3',
#         aws_access_key_id=AWS_ACCESS_KEY_ID,
#         aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
#         region_name=AWS_REGION
#     )

#     current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
#     file_name = f"ebay_multicategory_raw_{current_date}.json"
#     s3_object_key = f"bronze/ebay_api/{file_name}"

#     # Đóng gói dữ liệu dạng JSON chuẩn
#     json_data = json.dumps(data_list, indent=4)

#     try:
#         print(f"\nĐang tải file lên S3 tại s3://{BUCKET_NAME}/{s3_object_key}...")
#         s3_client.put_object(
#             Bucket=BUCKET_NAME,
#             Key=s3_object_key,
#             Body=json_data,
#             ContentType='application/json'
#         )
#         print(f"✅ Đã đẩy thành công {len(data_list)} bản ghi thô lên S3 Bronze!")
#     except Exception as e:
#         print(f"❌ Lỗi khi tải lên S3: {e}")


# if __name__ == "__main__":
#     # Cào toàn bộ dữ liệu theo danh mục (không giới hạn cứng, có chặn ngưỡng offset < 5000 an toàn)
#     raw_data = scrape_all_ebay_categories()
    
#     # Đẩy thẳng kết quả lên S3 Bronze
#     upload_raw_to_s3(raw_data)








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
from dotenv import load_dotenv

# Load biến môi trường từ file .env ở thư mục gốc
load_dotenv()

CLIENT_ID = os.getenv("EBAY_CLIENT_ID")
CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-2")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
BASE_PREFIX = os.getenv("BUCKET_BASE_PREFIX")

TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"
TAXONOMY_URL = "https://api.ebay.com/commerce/taxonomy/v1/category_tree/0"

def get_access_token():
    """Lấy OAuth Token từ eBay API"""
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
    res = requests.post(TOKEN_URL, headers=headers, data=data)
    if res.status_code == 200:
        return res.json().get("access_token")
    print("Lỗi lấy Token:", res.text)
    return None


def get_all_top_categories(token):
    """Lấy danh sách tất cả các danh mục chính từ eBay Taxonomy API"""
    headers = {"Authorization": f"Bearer {token}"}
    res = requests.get(TAXONOMY_URL, headers=headers)
    if res.status_code == 200:
        nodes = (
            res.json()
            .get("rootCategoryNode", {})
            .get("childCategoryTreeNodes", [])
        )
        return [
            {
                "id": node["category"]["categoryId"],
                "name": node["category"]["categoryName"],
            }
            for node in nodes
        ]
    print("Lỗi lấy cây danh mục:", res.text)
    return []


def scrape_all_ebay_categories():
    """
    Duyệt qua các danh mục chính, cào dữ liệu sản phẩm từ eBay Browse API
    không giới hạn cứng số lượng, kết hợp cơ chế an toàn chống lặp vô hạn.
    """
    token = get_access_token()
    if not token:
        return []

    categories = get_all_top_categories(token)
    print(f"=== TÌM THẤY {len(categories)} DANH MỤC CHÍNH ĐỂ CÀO ===")

    all_raw_items = []

    for cat in categories:
        cat_id = cat["id"]
        cat_name = cat["name"]
        print(f"\n>>> Đang quét danh mục: {cat_name} (ID: {cat_id})")

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

            res = requests.get(SEARCH_URL, headers=headers, params=params)

            if res.status_code == 401:
                retry_count += 1
                if retry_count > max_retries:
                    print(f"❌ Lỗi 401 liên tiếp {max_retries} lần. Bỏ qua danh mục.")
                    break
                print(f"Token hết hạn, đang đổi token mới...")
                token = get_access_token()
                if not token:
                    break
                continue

            if res.status_code == 429:
                retry_count += 1
                if retry_count > max_retries:
                    print(f"❌ Chạm giới hạn Rate Limit (429). Bỏ qua danh mục.")
                    break
                print(f"Chạm giới hạn API! Tạm nghỉ 60 giây...")
                time.sleep(60)
                continue

            if res.status_code != 200:
                print(f"Bỏ qua danh mục do lỗi {res.status_code}: {res.text}")
                break

            retry_count = 0
            data = res.json()
            items = data.get("itemSummaries", [])

            if not items:
                print("Đã duyệt hết sản phẩm trong danh mục này.")
                break

            for item in items:
                item["scraped_category_id"] = cat_id
                item["scraped_category_name"] = cat_name
                # Thêm mốc thời gian cào dữ liệu dạng string để phục vụ partition
                item["scraped_at"] = datetime.now().isoformat()
                all_raw_items.append(item)

            offset += limit
            print(f"   + Tổng tích lũy toàn hệ thống: {len(all_raw_items)} sản phẩm...")
            time.sleep(0.3)

    return all_raw_items


def ensure_s3_bucket_exists(s3_client, bucket_name, region):
    """Kiểm tra xem S3 Bucket đã tồn tại chưa, nếu chưa thì tiến hành tạo mới."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"✅ S3 Bucket '{bucket_name}' đã tồn tại.")
    except Exception:
        print(f"⚠️ S3 Bucket '{bucket_name}' chưa tồn tại. Đang tiến hành tạo mới...")
        try:
            if region == "us-east-1":
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            print(f"✅ Đã tạo thành công S3 Bucket: '{bucket_name}'")
        except Exception as e:
            print(f"❌ Lỗi khi tạo S3 Bucket: {e}")
            raise e


# Tìm đến hàm này trong file ebay_ingest.py của bạn và cập nhật lại:
def upload_parquet_partitioned_to_s3(data_list, target_date=None):
    """
    Chuyển đổi dữ liệu JSON sang Parquet, phân chia thư mục theo YYYY/MM/DD
    Dựa trên ngày được truyền vào (target_date) thay vì datetime.now() cố định.
    """
    if not data_list:
        print("Không có dữ liệu để đẩy lên S3.")
        return

    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    # 1. Đảm bảo S3 Bucket tồn tại
    ensure_s3_bucket_exists(s3_client, BUCKET_NAME, AWS_REGION)

    # 2. Chuyển đổi sang Pandas DataFrame
    df = pd.DataFrame(data_list)

    # CRITICAL FIX: Kiểm tra nếu có target_date từ Airflow truyền vào thì dùng, không thì mới lấy hiện tại
    if target_date is None:
        target_date = datetime.now()
        
    year = target_date.strftime("%Y")
    month = target_date.strftime("%m")
    day = target_date.strftime("%d")
    timestamp_str = datetime.now().strftime("%H%M%S") # Giữ timestamp giờ chạy thật để tránh đè file

    # Tạo đường dẫn S3 chuẩn cấu trúc partition
    s3_prefix = f"{BASE_PREFIX}/year={year}/month={month}/day={day}"
    file_name = f"ebay_raw_data_{timestamp_str}.parquet"
    s3_object_key = f"{s3_prefix}/{file_name}"

    # 3. Chuyển đổi DataFrame sang định dạng Parquet qua bộ nhớ đệm
    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy', index=False)
    parquet_buffer.seek(0)

    try:
        print(f"\nĐang tải file Parquet lên S3 tại s3://{BUCKET_NAME}/{s3_object_key}...")
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_object_key,
            Body=parquet_buffer.getvalue(),
            ContentType='application/octet-stream'
        )
        print(f"✅ Đã đẩy thành công {len(data_list)} bản ghi dạng Parquet lên S3 (Partition: year={year}/month={month}/day={day})!")
    except Exception as e:
        print(f"❌ Lỗi khi tải file Parquet lên S3: {e}")



if __name__ == "__main__":
    # Bước 1: Cào toàn bộ dữ liệu từ API eBay
    raw_data = scrape_all_ebay_categories()
    
    # Bước 2: Chuyển đổi sang Parquet và đẩy lên S3 theo phân vùng ngày tháng
    upload_parquet_partitioned_to_s3(raw_data)