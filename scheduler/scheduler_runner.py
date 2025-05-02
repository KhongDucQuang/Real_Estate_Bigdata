# File: scheduler/scheduler_runner.py

# --- Code sửa sys.path (nếu cần, giữ lại từ bước trước) ---
import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# --- Kết thúc code sửa sys.path ---

import schedule
import time
import signal # Để xử lý tín hiệu dừng (Ctrl+C)
import logging
from concurrent.futures import ThreadPoolExecutor

# Import producer và crawler
# Lưu ý: đổi lại tên thư mục nếu bạn dùng tên khác 'kafka_cc'
from kafka_cc.producer.kafka_producer import send_to_kafka, close_kafka_producer
from crawler.alonhadat import crawl_alonhadat
# from crawler.123nhadatviet import crawl_123nhadatviet # Ví dụ import crawler khác

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("scheduler_runner")

# Mapping nguồn crawler -> (hàm crawl, tên topic)
sources = [
    ("alonhadat", crawl_alonhadat, "alonhadat"), # (Tên nguồn, Hàm crawl, Tên topic)
    # ("123nhadat", crawl_123nhadat, "123nhadat"), # Thêm nguồn khác ở đây
]

def job_for_source(source_name, crawl_func, topic_name):
    log.info(f"🚀 Bắt đầu cào dữ liệu từ: {source_name}")
    try:
        data = crawl_func() # Gọi hàm crawl tương ứng
        if data is not None: # Kiểm tra xem có dữ liệu trả về không
            log.info(f"Cào xong {source_name}, thu được {len(data)} bản ghi.")
            if data: # Chỉ gửi nếu list không rỗng
                send_to_kafka(data, topic_name=topic_name)
        else:
            log.warning(f"Hàm crawl của {source_name} không trả về dữ liệu.")
    except Exception as e:
        log.error(f"Lỗi trong quá trình chạy job cho {source_name}: {e}", exc_info=True)

def crawl_all_sources():
    log.info("--- Bắt đầu chu kỳ crawl tất cả các nguồn ---")
    # Số worker bằng số nguồn để chạy song song tối đa
    with ThreadPoolExecutor(max_workers=len(sources)) as executor:
        for name, func, topic in sources:
            # Submit từng job vào pool
            executor.submit(job_for_source, name, func, topic)
    log.info("--- Kết thúc chu kỳ crawl ---")

# --- Xử lý tín hiệu dừng (Ctrl+C) ---
shutdown_signal_received = False
def handle_shutdown_signal(signum, frame):
    global shutdown_signal_received
    if not shutdown_signal_received:
        log.warning(f"Nhận được tín hiệu dừng (Signal {signum}). Đang chờ job hiện tại hoàn thành và dừng scheduler...")
        shutdown_signal_received = True
        # Không cancel schedule ngay, để job đang chạy có thể hoàn thành
        # schedule.clear() # Nếu muốn dừng ngay lập tức
    else:
        log.warning("Nhận tín hiệu dừng lần thứ hai. Buộc dừng.")
        exit(1)

signal.signal(signal.SIGINT, handle_shutdown_signal)  # Ctrl+C
signal.signal(signal.SIGTERM, handle_shutdown_signal) # Tín hiệu dừng từ Docker

# --- Lên lịch và chạy ---
crawl_interval_minutes = 1 # Đặt khoảng thời gian crawl
log.info(f"Scheduler bắt đầu. Sẽ chạy crawl mỗi {crawl_interval_minutes} phút.")
schedule.every(crawl_interval_minutes).minutes.do(crawl_all_sources)

# Chạy lần đầu tiên ngay lập tức (tùy chọn)
# crawl_all_sources()

# Vòng lặp chính của scheduler
while not shutdown_signal_received:
    schedule.run_pending()
    time.sleep(1) # Giảm tải CPU

# --- Dọn dẹp trước khi thoát ---
log.info("Scheduler đang dừng. Đóng các kết nối...")
close_kafka_producer()
log.info("Scheduler đã dừng.")