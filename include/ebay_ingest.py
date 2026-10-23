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
# AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-1")
# BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# TOKEN_URL = "https://api.ebay.com/identity/v1/oauth2/token"
# SEARCH_URL = "https://api.ebay.com/buy/browse/v1/item_summary/search"


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


# def fetch_ebay_data_by_keyword(keyword="laptop", max_items=500):
#     """
#     Cào dữ liệu từ eBay theo từ khóa chỉ định với giới hạn an toàn,
#     tránh bị quá tải hoặc dính Rate Limit (429).
#     """
#     token = get_access_token()
#     if not token:
#         return []

#     all_items = []
#     limit = 100
#     offset = 0

#     print(f"=== BẮT ĐẦU CÀO DỮ LIỆU EBAY CHO TỪ KHÓA: '{keyword}' ===")

#     while offset < max_items:
#         headers = {"Authorization": f"Bearer {token}"}
#         params = {
#             "q": keyword,
#             "limit": limit,
#             "offset": offset,
#         }

#         res = requests.get(SEARCH_URL, headers=headers, params=params)

#         # Xử lý Token hết hạn (401)
#         if res.status_code == 401:
#             print("Token hết hạn, đang cấp token mới...")
#             token = get_access_token()
#             continue

#         # Xử lý Rate Limit (429)
#         if res.status_code == 429:
#             print("Chạm giới hạn API! Tạm nghỉ 60 giây...")
#             time.sleep(60)
#             continue

#         if res.status_code != 200:
#             print(f"Lỗi API {res.status_code}: {res.text}")
#             break

#         data = res.json()
#         items = data.get("itemSummaries", [])

#         if not items:
#             print("Đã duyệt hết sản phẩm từ API.")
#             break

#         all_items.extend(items)
#         offset += limit
#         print(f"   + Đã lấy tích lũy: {len(all_items)} sản phẩm...")
#         time.sleep(0.5)

#     return all_items


# def upload_raw_to_s3(data_list, keyword="laptop"):
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
#     file_name = f"ebay_{keyword}_{current_date}.json"
#     s3_object_key = f"bronze/ebay_api/{file_name}"

#     # Đóng gói dữ liệu dạng JSON chuẩn
#     json_data = json.dumps(data_list, indent=4)

#     try:
#         print(f"Đang tải file lên S3 tại s3://{BUCKET_NAME}/{s3_object_key}...")
#         s3_client.put_object(
#             Bucket=BUCKET_NAME,
#             Key=s3_object_key,
#             Body=json_data,
#             ContentType='application/json'
#         )
#         print(f"✅ Đã đẩy thành công {len(data_list)} bản ghi lên S3 Bronze!")
#     except Exception as e:
#         print(f"❌ Lỗi khi tải lên S3: {e}")


# if __name__ == "__main__":
#     # Cào mẫu từ khóa sản phẩm cho dự án FMCG / E-commerce của bạn
#     keyword_query = "sports equipment" 
#     raw_data = fetch_ebay_data_by_keyword(keyword=keyword_query, max_items=300)
    
#     # Lưu thẳng lên S3
#     upload_raw_to_s3(raw_data, keyword=keyword_query)


import os
import json
import time
from datetime import datetime
import base64
import requests
import boto3
from dotenv import load_dotenv

# Load biến môi trường từ file .env ở thư mục gốc
load_dotenv()

CLIENT_ID = os.getenv("EBAY_CLIENT_ID")
CLIENT_SECRET = os.getenv("EBAY_CLIENT_SECRET")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-2")
BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

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
    không giới hạn cứng số lượng, kết hợp cơ chế an toàn chống lặp vô hạn (Retry & Offset < 5000).
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
        max_safe_offset = 5000  # Ngưỡng an toàn tối đa theo yêu cầu
        retry_count = 0
        max_retries = 3        # Giới hạn số lần thử lại liên tiếp khi gặp lỗi hệ thống/rate limit

        while offset < max_safe_offset:
            headers = {"Authorization": f"Bearer {token}"}
            params = {
                "category_ids": cat_id,
                "limit": limit,
                "offset": offset,
            }

            res = requests.get(SEARCH_URL, headers=headers, params=params)

            # 1. Xử lý Token hết hạn (401) có kèm cơ chế đếm số lần retry
            if res.status_code == 401:
                retry_count += 1
                if retry_count > max_retries:
                    print(f"❌ Lỗi 401 liên tiếp {max_retries} lần. Bỏ qua danh mục này để tránh kẹt vòng lặp.")
                    break
                print(f"Token hết hạn (Lần thử {retry_count}/{max_retries}), đang đổi token mới...")
                token = get_access_token()
                if not token:
                    break
                continue

            # 2. Xử lý Rate Limit (429) có kèm cơ chế đếm số lần retry
            if res.status_code == 429:
                retry_count += 1
                if retry_count > max_retries:
                    print(f"❌ Chạm giới hạn Rate Limit (429) {max_retries} lần liên tiếp. Bỏ qua danh mục.")
                    break
                print(f"Chạm giới hạn API! Tạm nghỉ 60 giây (Lần thử {retry_count}/{max_retries})...")
                time.sleep(60)
                continue

            if res.status_code != 200:
                print(f"Bỏ qua danh mục do lỗi {res.status_code}: {res.text}")
                break

            # Nếu request thành công, reset lại biến đếm retry
            retry_count = 0

            data = res.json()
            items = data.get("itemSummaries", [])

            # 3. Điều kiện ngắt vòng lặp (break) nếu API trả về danh sách rỗng hoặc hết trang
            if not items:
                print("Đã duyệt hết sản phẩm hoặc không còn dữ liệu trong danh mục này.")
                break

            # Gắn thêm metadata danh mục vào từng item thô
            for item in items:
                item["scraped_category_id"] = cat_id
                item["scraped_category_name"] = cat_name
                all_raw_items.append(item)

            offset += limit
            print(f"   + Tổng tích lũy toàn hệ thống: {len(all_raw_items)} sản phẩm...")
            time.sleep(0.3)

    return all_raw_items


def upload_raw_to_s3(data_list):
    """
    Đẩy toàn bộ dữ liệu thô (Raw JSON) trực tiếp lên S3 tầng Bronze: bronze/ebay_api/
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

    current_date = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"ebay_multicategory_raw_{current_date}.json"
    s3_object_key = f"bronze/ebay_api/{file_name}"

    # Đóng gói dữ liệu dạng JSON chuẩn
    json_data = json.dumps(data_list, indent=4)

    try:
        print(f"\nĐang tải file lên S3 tại s3://{BUCKET_NAME}/{s3_object_key}...")
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_object_key,
            Body=json_data,
            ContentType='application/json'
        )
        print(f"✅ Đã đẩy thành công {len(data_list)} bản ghi thô lên S3 Bronze!")
    except Exception as e:
        print(f"❌ Lỗi khi tải lên S3: {e}")


if __name__ == "__main__":
    # Cào toàn bộ dữ liệu theo danh mục (không giới hạn cứng, có chặn ngưỡng offset < 5000 an toàn)
    raw_data = scrape_all_ebay_categories()
    
    # Đẩy thẳng kết quả lên S3 Bronze
    upload_raw_to_s3(raw_data)