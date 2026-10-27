# ============================================================================
# Bước 1: Chọn Image nền (Base Image) chứa sẵn hệ điều hành Linux và Apache Airflow
# ============================================================================
FROM apache/airflow:2.7.2-python3.10

USER root

# Cài đặt các thư viện hệ thống cần thiết của Linux (nếu có)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch về user airflow để đảm bảo tính an toàn bảo mật khi cài thư viện Python
USER airflow

# ============================================================================
# Bước 2: Sao chép toàn bộ mã nguồn của dự án từ máy thật vào trong Docker Image
# ============================================================================
# Tạo thư mục chứa dự án dbt bên trong môi trường Docker
RUN mkdir -p ${AIRFLOW_HOME}/jaffle_shop

# Copy code DAGs, các hàm cào dữ liệu (include) và dự án dbt vào container
COPY --chown=airflow:root dags/ ${AIRFLOW_HOME}/dags/
COPY --chown=airflow:root include/ ${AIRFLOW_HOME}/include/
COPY --chown=airflow:root jaffle_shop/ ${AIRFLOW_HOME}/jaffle_shop/

# ============================================================================
# Bước 3: Định nghĩa các Biến môi trường (Environment Variables) cấu hình hệ thống
# ============================================================================
ENV DBT_PROJECT_DIR=${AIRFLOW_HOME}/jaffle_shop
ENV DBT_PROFILES_DIR=${AIRFLOW_HOME}/jaffle_shop
# Ngăn Python sinh ra các file rác .pyc làm phình container
ENV PYTHONDONTWRITEBYTECODE=1 

# ============================================================================
# Bước 4: Cài đặt toàn bộ các thư viện Python (Dependencies) phục vụ pipeline
# ============================================================================
# Lưu ý: dbt-snowflake đã bao gồm sẵn thư viện kết nối dbt-core và thư viện snowflake
RUN pip install --no-cache-dir --user \
    apache-airflow-providers-snowflake \
    boto3 \
    pandas \
    pyarrow \
    requests \
    dbt-snowflake

# (Tùy chọn) Kiểm tra xem dbt đã cài đặt thành công và nhận diện được chưa
RUN dbt --version
