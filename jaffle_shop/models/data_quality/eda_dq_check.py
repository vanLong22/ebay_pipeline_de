import snowflake.snowpark.functions as F

def model(dbt, session):
    # 1. Cấu hình dbt: Lưu kết quả phân tích ngoại lai này thành một bảng (Table) thực thể trên Snowflake
    dbt.config(
        materialized="table",
        schema="data_quality" # Sẽ tạo bảng trong schema data_quality (tùy cấu hình dbt_project.yml)
    )
    
    # 2. ĐỌC DỮ LIỆU TỪ MODEL TRƯỚC ĐÓ (Tư duy dbt lineage)
    # Thay vì dùng session.table(), dbt bắt buộc dùng dbt.ref() để hiểu luồng đi của dữ liệu
    # Giả định dữ liệu đã được bóc tách phẳng tại model 'stg_ebay_listings' (tầng Silver)
    df_snow = dbt.ref("stg_ebay_raw")
    
    # Tính toán tổng số dòng (để phục vụ tính tỷ lệ % sau này)
    total_rows = df_snow.count()
    if total_rows == 0:
        total_rows = 1 # Tránh lỗi chia cho 0 nếu bảng trống
        
    # 3. PHÁT HIỆN NGOẠI LAI (Outliers) CHO price_value BẰNG IQR
    # Tính toán phân vị Q1 (25%) và Q3 (75%) trực tiếp trên cụm tính toán của Snowflake
    stats = df_snow.select(
        F.percentile_cont(0.25).within_group("price_value").alias("Q1"),
        F.percentile_cont(0.75).within_group("price_value").alias("Q3")
    ).collect()[0]

    q1 = stats["Q1"] if stats["Q1"] is not None else 0
    q3 = stats["Q3"] if stats["Q3"] is not None else 0
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    # 4. TẠO RA BẢNG KẾT QUẢ ĐỂ XUẤT RA BI / AIRFLOW GIÁM SÁT
    # dbt Python model BẮT BUỘC phải trả về một DataFrame. 
    # Do đó, thay vì chỉ 'print' ra màn hình log, ta sẽ biến các chỉ số này thành 1 dòng dữ liệu lưu xuống kho.
    
    final_dq_report = df_snow.filter(
        (F.col("price_value") < lower_bound) | (F.col("price_value") > upper_bound)
    ).select(
        F.lit(q1).alias("Q1_VALUE"),
        F.lit(q3).alias("Q3_VALUE"),
        F.lit(lower_bound).alias("LOWER_BOUND"),
        F.lit(upper_bound).alias("UPPER_BOUND"),
        F.count("item_id").alias("OUTLIERS_COUNT"),
        F.round((F.count("item_id") / total_rows) * 100, 2).alias("OUTLIERS_PERCENTAGE"),
        F.current_timestamp().alias("CHECKED_AT")
    )

    # 5. BẮT BUỘC: Trả về DataFrame để dbt tự động ghi thành bảng 'eda_dq_check' trên Snowflake
    return final_dq_report
