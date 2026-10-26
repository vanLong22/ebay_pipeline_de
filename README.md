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
Trong thiết kế Kho dữ liệu (Data Warehouse) và Data Marts (tầng Gold), việc xây dựng **Dimension Table (Bảng chiều - Dim)** và **Fact Table (Bảng sự kiện - Fact)** tuân theo **Mô hình chiều (Dimensional Modeling)** của Ralph Kimball.

Dưới đây là các quy tắc chuẩn mực và câu trả lời cho việc có bắt buộc phải tuân thủ chúng hay không:

---

### I. Các quy tắc khi tạo Dimension Table (Bảng Dim)

Bảng Dim trả lời cho câu hỏi: *Ai, cái gì, ở đâu, khi nào, loại gì?* (Chứa thông tin ngữ cảnh và mô tả).

1. **Phải có Khóa chính (Surrogate Key / Primary Key):**
* Mỗi bảng Dim nên có một khóa chính độc lập (thường dùng dạng chuỗi số nguyên tự tăng hoặc dùng hàm `HASH()` như cách bạn đang làm với `HASH(item_id)` hoặc `HASH(seller_username)`). Tránh phụ thuộc hoàn toàn vào mã nguồn cũ vì mã nguồn có thể bị thay đổi từ hệ thống gốc.


2. **Chứa các thuộc tính mô tả (Descriptive Attributes):**
* Các cột trong Dim chủ yếu là kiểu chữ (`STRING`) mang tính chất phân loại, lọc dữ liệu hoặc hiển thị (ví dụ: tên danh mục, tên người bán, quốc gia, trạng thái sản phẩm). **Tuyệt đối không chứa các con số để cộng trừ nhân chia (measures).**


3. **Cấu trúc phẳng (Denormalized):**
* Khác với thiết kế cơ sở dữ liệu OLTP chuẩn hóa cao (3rd Normal Form), bảng Dim trong Data Warehouse thường chấp nhận lặp lại dữ liệu (flat table) để giảm thiểu việc phải `JOIN` nhiều bảng với nhau khi người dùng truy vấn.


4. **Quản lý thay đổi (Slow Changing Dimensions - SCD):**
* Cần có chiến lược xử lý khi thông tin trong Dim thay đổi theo thời gian (ví dụ: người bán đổi tên, sản phẩm đổi danh mục) để lịch sử dữ liệu không bị sai lệch.



---

### II. Các quy tắc khi tạo Fact Table (Bảng Fact)

Bảng Fact trả lời cho câu hỏi: *Bao nhiêu?* (Chứa các số liệu đo lường, giao dịch hoặc sự kiện).

1. **Xác định rõ Hạt giống (Grain - Mức độ chi tiết):**
* Đây là **quy tắc quan trọng nhất**. Bạn phải định nghĩa rõ: *1 dòng trong bảng Fact đại diện cho cái gì?*
* *Ví dụ:* Trong file `fact_listing.sql` của bạn, grain được định nghĩa rất chuẩn: *"One ITEM_ID per snapshot date"* (1 dòng tương ứng với 1 sản phẩm tại 1 ngày chụp dữ liệu).


2. **Chứa các Khóa ngoại (Foreign Keys):**
* Bảng Fact phải chứa các khóa ngoại (như `listing_key`, `seller_key`, `date_key`, `category_key`...) để liên kết ngược lại với các bảng Dim tương ứng.


3. **Chứa các Chỉ số đo lường (Measures / Metrics):**
* Các cột dữ liệu phải là các con số phục vụ cho việc tính toán thống kê (ví dụ: `price_value`, `bid_count`, `discount_percentage`, `shipping_cost`). Các con số này thường có tính chất có thể cộng dồn (additive) hoặc bán cộng dồn (semi-additive).


4. **Kích thước bảng rất lớn (High Volume):**
* Bảng Fact thường có số lượng dòng cực lớn và tăng trưởng theo từng ngày (time-series). Do đó, nó cần được tối ưu phân vùng (partition) hoặc đánh chỉ mục (clustering) để truy vấn không bị chậm.


=================================================

Để hiểu rõ sự khác biệt kinh điển này, chúng ta hãy đặt hai mô hình OLTP (Hệ thống chạy App/Web - chuẩn hóa cao) và Data Warehouse (Kho dữ liệu báo cáo - làm phẳng bảng) lên cùng một bàn cân với một ví dụ thực tế từ chính cấu trúc bảng dim_category (Danh mục sản phẩm) trong dự án eBay của bạn.

1. Cách thiết kế của OLTP (Chuẩn hóa cao - 3rd Normal Form)

Trong các hệ thống như MySQL, PostgreSQL chạy ứng dụng Web, mục tiêu tối cao là không được phí phạm dung lượng ổ đĩa và tuyệt đối không để dữ liệu bị lặp lại (để khi sửa đổi tên danh mục thì chỉ cần sửa đúng 1 chỗ).
Hệ thống OLTP sẽ chia thông tin danh mục thành các bảng nhỏ rời rạc và nối với nhau bằng các ID:
• Bảng Category_L1 (Cấp 1):
l1_id	l1_name
C_01	Baby

• Bảng Category_L2 (Cấp 2 - trung gian): Trỏ khóa ngoại về L1
l2_id	l2_name	l1_id
C_02_A	Baby Clothing	C_01

• Bảng Category_Leaf (Cấp thấp nhất): Trỏ khóa ngoại về L2
leaf_id	leaf_name	l2_id
C_Leaf_99	Baby Shoes	C_02_A


⚠️ Vấn đề khi làm báo cáo với OLTP:

Nếu sếp muốn chạy một câu lệnh xem tổng doanh thu của toàn bộ nhóm ngành Baby (L1), Snowflake hoặc hệ thống tính toán của bạn sẽ phải thực hiện một chuỗi phép JOIN cực kỳ kinh hoàng:
Bảng Fact ➔ JOIN với Category_Leaf ➔ JOIN tiếp với Category_L2 ➔ JOIN tiếp với Category_L1 rồi mới WHERE l1_name = 'Baby'.
Việc phải thực hiện 3-4 phép JOIN bắc cầu trên hàng triệu dòng dữ liệu chỉ để lấy cái tên danh mục sẽ làm hệ thống báo cáo bị nghẽn mạch và chạy siêu chậm.

2. Cách thiết kế của Data Warehouse (Chấp nhận lặp dữ liệu - Flat Table)

Trong Kho dữ liệu (Data Warehouse), dung lượng ổ đĩa ngày nay rất rẻ, cái đắt đỏ nhất là tài nguyên tính toán (CPU/RAM) mỗi lần xử lý phép JOIN. Do đó, người ta áp dụng quy tắc "Làm phẳng bảng" (Denormalization).
Tất cả các cấp danh mục từ cha đến con sẽ được gộp chung vào ĐÚNG 1 BẢNG DUY NHẤT là dim_category giống hệt như bản mô tả thiết kế của bạn:
Bảng dim_category (Flat Table hoàn toàn):

category_key	category_leaf_id	category_leaf_name	category_l2_name	category_l1_id	category_l1_name
1	C_Leaf_99	Baby Shoes	Baby Clothing	C_01	Baby
2	C_Leaf_100	Baby Socks	Baby Clothing	C_01	Baby

=================================================
neus dữ liệu trong snowflake đã có các dim và fact table rồi thì làm khi chạy các dbt như này có làm lỗi dữ liệu ở gold schema snowflake ko