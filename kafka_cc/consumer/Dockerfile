# File: kafka_cc/consumer/Dockerfile

FROM python:3.12-slim

WORKDIR /app

# Cài đặt thư viện hdfs (cần gcc và các gói build)
# Hoặc cài snakebite-py3 nếu dùng RPC
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libkrb5-dev \
 && rm -rf /var/lib/apt/lists/* # Lệnh dọn dẹp cache của apt-get

COPY ../../requirements_consumer.txt .

RUN pip install --no-cache-dir -r requirements_consumer.txt

COPY ./kafka_cc/consumer /app/kafka_cc/consumer
# Đảm bảo __init__.py cũng được copy nếu có cấu trúc package phức tạp hơn

CMD ["python", "-u", "kafka_cc/consumer/kafka_to_hdfs_consumer.py"]