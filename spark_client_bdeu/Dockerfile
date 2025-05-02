# File: spark_client_bdeu/Dockerfile

# Dùng logic từ base/Dockerfile của big-data-europe
FROM alpine:3.10

# Có thể copy các LABEL, ENV từ base/Dockerfile gốc nếu muốn
ENV SPARK_VERSION=3.2.1
ENV HADOOP_VERSION=3.2
ENV BASE_URL=https://archive.apache.org/dist/spark/
ENV PYTHONHASHSEED 1

# Cài đặt các gói cần thiết và Spark (lấy từ base/Dockerfile gốc)
# Thêm 'git' nếu bạn cần clone repo bên trong container
RUN apk add --no-cache curl bash openjdk8-jre python3 py-pip nss libc6-compat coreutils procps git \
      && ln -s /lib64/ld-linux-x86-64.so.2 /lib/ld-linux-x86-64.so.2 \
      && wget ${BASE_URL}/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
      && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
      && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Thiết lập biến môi trường Spark
ENV SPARK_HOME=/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
# Đặt PYSPARK_PYTHON để trỏ đúng python3 trên Alpine
ENV PYSPARK_PYTHON=/usr/bin/python3
ENV PYSPARK_DRIVER_PYTHON=/usr/bin/python3

WORKDIR /app

# Container này chỉ dùng làm client, không cần chạy service gì mặc định
# Có thể giữ nó chạy bằng CMD ["tail", "-f", "/dev/null"] nếu muốn dùng exec dễ hơn
CMD ["bash"] # Hoặc để CMD mặc định là bash