# File: scheduler/Dockerfile

# Chọn image Python nền
FROM python:3.12-slim

# === THÊM PHẦN CÀI ĐẶT CHROME ===
# Cài các gói cần thiết để thêm PPA và cài Chrome
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    gnupg \
    # Các gói phụ thuộc khác có thể cần cho Chrome
    libxss1 \
    libappindicator1 \
    fonts-liberation \
 && rm -rf /var/lib/apt/lists/*

# Tải và cài đặt Google Chrome (phiên bản ổn định)
# Luôn kiểm tra link tải mới nhất nếu cần
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
 && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
 && apt-get update \
 && apt-get install -y google-chrome-stable --no-install-recommends \
 && rm -rf /var/lib/apt/lists/*
# === KẾT THÚC PHẦN CÀI ĐẶT CHROME ===

# Đặt thư mục làm việc bên trong container
WORKDIR /app

# Sao chép file requirements trước để tận dụng Docker cache layer
COPY ../requirements.txt .

# Cài đặt các thư viện Python (bao gồm selenium, webdriver-manager)
RUN pip install --no-cache-dir -r requirements.txt

# Sao chép code ứng dụng
COPY ./scheduler /app/scheduler
COPY ./crawler /app/crawler
COPY ./kafka_cc /app/kafka_cc
# Đảm bảo __init__.py cũng được copy

# Lệnh để chạy khi container khởi động
CMD ["python", "-u", "scheduler/scheduler_runner.py"]