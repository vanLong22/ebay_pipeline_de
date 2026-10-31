FROM quay.io/astronomer/astro-runtime:12.6.0

USER root
# 1. Tối ưu hóa lệnh cập nhật hệ thống bảo mật để không bị lỗi 404
RUN apt-get update --allow-releaseinfo-change && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ============================================================================
# Cấu hình biến môi trường dbt dưới quyền hệ thống trước
# ============================================================================
ENV DBT_PROJECT_DIR=/usr/local/airflow/jaffle_shop
ENV DBT_PROFILES_DIR=/usr/local/airflow/jaffle_shop
ENV PYTHONDONTWRITEBYTECODE=1 

# 2. Tạo thư mục jaffle_shop bằng quyền root để tránh lỗi phân quyền hệ thống
RUN mkdir -p /usr/local/airflow/jaffle_shop && chown -R astro:0 /usr/local/airflow/jaffle_shop

# 3. Chuyển sang user chuẩn của Astro Runtime (User mặc định là astro chứ không phải airflow)
USER astro

# Copy toàn bộ mã nguồn của bạn vào hệ thống với quyền sở hữu chính xác
COPY --chown=astro:0 dags/ /usr/local/airflow/dags/
COPY --chown=astro:0 include/ /usr/local/airflow/include/
COPY --chown=astro:0 jaffle_shop/ /usr/local/airflow/jaffle_shop/

# ============================================================================
# TỰ ĐỘNG TẠO MÔI TRƯỜNG ẢO PYTHON VÀ CÀI ĐẶT DBT-SNOWFLAKE SIÊU TỐC
# ============================================================================
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt