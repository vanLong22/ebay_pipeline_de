Bạn viết code DAG trên máy tính ➔ Bạn dùng lệnh docker-compose up để Docker bật Airflow lên và chạy thử nghiệm (Local) ➔ Khi thấy code chạy chuẩn, bạn copy toàn bộ thư mục dự án đó (gồm file DAG và file docker-compose.yaml) lên máy chủ Cloud ➔ Trên Cloud, bạn lại gõ lệnh docker-compose up ➔ Hệ thống tự động kích hoạt y hệt như dưới máy bạn và chạy 24/7.




-- -- Dùng TRANSIENT TABLE để tiết kiệm tối đa chi phí lưu trữ và Fail-safe
-- CREATE OR REPLACE TRANSIENT TABLE raw_ebay_listings (
--     raw_data VARIANT,                                       -- Cột chứa nội dung JSON thô
--     src_file_name VARCHAR,                                    -- Tên file nguồn trên S3 để đối chiếu khi lỗi
--     load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()  -- Thời gian nạp vào kho
-- );
dùng cách này tạo table thì sau 1 thời gian nó sẽ tự động xóa khỏi schema bronze của snowflake phải ko 


========================================

tức là nếu tôi cào dữ liệu mới hôm nay, nó sẽ so sánh unique_key là item_id với dữ iệu đã cào được vào hôm trước , nếu khác thì thêm và cập nhật ở bảng dữ liệu nằm trong silver và gold schema của snowflake đúng ko. 

1. Các bản ghi mới được lưu ở đâu?Tất cả dữ liệu (bao gồm cả dữ liệu cũ từ hôm qua, dữ liệu cũ được cập nhật lại, và các bản ghi mang item_id mới tinh của hôm nay) đều được lưu chung vào cùng một bảng Silver duy nhất (ví dụ: bảng ebay_data_cleaned) trên Snowflake.Bạn không phải quản lý file thủ công hay sợ bị mất dữ liệu cũ, vì Snowflake là một Data Warehouse dạng cột, nó tự động quản lý các khối dữ liệu (micro-partitions) bên dưới.2. Bản chất của chiến lược merge (Upsert) hoạt động như thế nào?Không chỉ đơn thuần là "thêm mới item_id khác với hôm qua", chiến lược merge thông minh hơn thế rất nhiều dựa trên unique_key = 'item_id':Trường hợp 1: Nếu item_id hoàn toàn mới (Chưa có trong bảng Silver)$\rightarrow$ Hệ thống sẽ thực hiện hành động INSERT (chèn) dòng dữ liệu mới tinh đó vào bảng.Trường hợp 2: Nếu item_id ĐÃ TỒN TẠI trong bảng Silver (vì sản phẩm này hôm qua đã cào rồi, và hôm nay script Python cào lại)$\rightarrow$ Hệ thống sẽ không chèn trùng lặp, mà thực hiện hành động UPDATE (cập nhật) lại các thông tin mới nhất của sản phẩm đó (Ví dụ: Hôm qua giá là 10$, hôm nay người bán hạ giá còn 8$, hoặc số lượng bid đã tăng lên...). Các cột thay đổi sẽ được cập nhật đè lên giá trị cũ của đúng item_id đó.

========================================
cách code cào dữ liệu hoạt động

Dựa vào cấu trúc code trong file `ebay_ingest.py` của bạn, câu trả lời ngắn gọn là: **Ở tầng Ingestion (S3) bạn dùng dạng Append (Incremental theo file/partition), nhưng khi đưa vào Snowflake (từ tầng Silver trở đi) bạn NÊN dùng Incremental Load thay vì Full Load.**

Cụ thể cách vận hành cho từng tầng đối với dữ liệu cào này như sau:

---

### 1. Ở tầng S3 (Code Python hiện tại của bạn)

* **Bản chất code:** Mỗi lần script Python chạy, nó sẽ cào danh sách sản phẩm hiện tại từ eBay API và sinh ra một file `.parquet` mới độc lập, lưu vào đúng thư mục phân vùng ngày hôm đó:
`s3://.../year=YYYY/month=MM/day=DD/ebay_items_HHMMSS.parquet`
* **Đây là cơ chế gì?** Đây chính là dạng **Append-only (Incremental theo lô/file)**. Bạn không ghi đè hay xóa file cũ trên S3 mà cứ mỗi ngày lại bỏ thêm các file mới vào kho lưu trữ (Bronze Layer).

---

### 2. Ở tầng Snowflake (Đưa vào dbt)

Khi bạn dùng dbt để biến đổi dữ liệu từ S3 vào Snowflake, chiến lược xử lý được chia rõ ràng:

* **Tầng Bronze (Bảng thô trỏ vào S3):**
* Thường được cấu hình dưới dạng **External Table** trỏ thẳng vào các thư mục phân vùng trên S3. Khi S3 có thêm file của ngày mới, External Table tự động nhìn thấy mà không cần nạp lại từ đầu.


* **Tầng Silver (`ebay_data_cleaned.sql` - Làm sạch):**
* **TUYỆT ĐỐI KHÔNG DÙNG FULL LOAD** khi dữ liệu lớn dần.
* **Giải pháp:** Bạn phải chuyển model dbt này sang dạng **Incremental** với chiến lược **`merge` (Upsert)** dựa trên khóa chính là `item_id`.
* **Lý do:** Vì script Python của bạn mỗi ngày cào lại các sản phẩm đang active (dẫn đến hôm nay có thể cào lại những sản phẩm trùng với hôm qua), việc dùng dbt Incremental (Merge) sẽ giúp hệ thống tự động:
* Nếu `item_id` đã tồn tại và giá/trạng thái thay đổi $\rightarrow$ **Cập nhật (Update)** thông tin mới.
* Nếu `item_id` là sản phẩm mới xuất hiện $\rightarrow$ **Chèn thêm (Insert)**.
* Giúp giữ nguyên lịch sử, chạy nhanh trong vài giây và tiết kiệm chi phí tính toán tuyệt đối trên Snowflake!


========================================
