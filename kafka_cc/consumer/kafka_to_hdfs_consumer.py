# File: kafka_cc/consumer/kafka_to_hdfs_consumer.py
import json
import logging
import signal
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from hdfs import InsecureClient
from hdfs.util import HdfsError
from datetime import datetime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("kafka_hdfs_consumer")

# --- Cấu hình ---
KAFKA_BROKERS = ['kafka:9093'] # Kafka nội bộ
KAFKA_TOPIC = 'alonhadat'      # Topic chứa dữ liệu nguồn alonhadat
KAFKA_GROUP_ID = 'hdfs-consumers-alonhadat' # Consumer group ID
HDFS_NAMENODE_URL = 'http://namenode:9870' # URL WebHDFS NameNode
HDFS_USER = 'root' # Hoặc user bạn muốn dùng trên HDFS (thường là root với image bde)
# Đường dẫn lưu file trên HDFS, ví dụ: /user/root/realestate_data/raw/alonhadat/YYYY/MM/DD/data.jsonl
HDFS_BASE_PATH = '/user/root/realestate_data/raw/alonhadat'
# Ghi vào HDFS sau mỗi N bản ghi hoặc X giây
BATCH_SIZE = 100
BATCH_INTERVAL_SECONDS = 60

hdfs_client = None
consumer = None

def get_hdfs_client():
    global hdfs_client
    if hdfs_client is None:
        log.info(f"Đang kết nối tới HDFS NameNode: {HDFS_NAMENODE_URL}")
        try:
            # Nếu HDFS không yêu cầu user cụ thể, bỏ qua tham số user
            # Cần kiểm tra quyền ghi trên HDFS cho user này
            hdfs_client = InsecureClient(HDFS_NAMENODE_URL, user=HDFS_USER)
            # Kiểm tra kết nối bằng cách liệt kê thư mục gốc (hoặc thư mục base path)
            hdfs_client.makedirs(HDFS_BASE_PATH) # Tạo thư mục nếu chưa có
            log.info(f"✅ Kết nối HDFS thành công tới {HDFS_NAMENODE_URL} với user {HDFS_USER}")
        except HdfsError as he:
            log.error(f"Lỗi HDFS khi kết nối hoặc tạo thư mục: {he}")
            hdfs_client = None
        except Exception as e:
            log.error(f"Lỗi không xác định khi kết nối HDFS: {e}")
            hdfs_client = None
    return hdfs_client

def get_kafka_consumer():
    global consumer
    if consumer is None:
        log.info(f"Đang kết nối Kafka Consumer tới brokers: {KAFKA_BROKERS}, topic: {KAFKA_TOPIC}, group: {KAFKA_GROUP_ID}")
        retry_count = 0
        max_retries = 5
        while consumer is None and retry_count < max_retries:
            try:
                consumer = KafkaConsumer(
                    KAFKA_TOPIC,
                    bootstrap_servers=KAFKA_BROKERS,
                    group_id=KAFKA_GROUP_ID,
                    auto_offset_reset='earliest', # Bắt đầu đọc từ đầu nếu là group mới
                    consumer_timeout_ms=1000, # Timeout để vòng lặp không bị block mãi mãi
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')), # Tự động parse JSON
                    # enable_auto_commit=True # Hoặc False nếu muốn commit thủ công
                )
                log.info("✅ Kết nối Kafka Consumer thành công!")
            except NoBrokersAvailable:
                retry_count += 1
                log.warning(f"Consumer không thể kết nối tới Kafka broker. Thử lại lần {retry_count}/{max_retries} sau 5 giây...")
                time.sleep(5)
            except Exception as e:
                log.error(f"Lỗi không xác định khi khởi tạo Kafka Consumer: {e}")
                break # Không thử lại với lỗi không xác định
        if consumer is None:
            log.error("❌ Không thể khởi tạo Kafka Consumer sau nhiều lần thử.")
    return consumer

def write_batch_to_hdfs(batch_data):
    if not batch_data:
        return

    hdfs_cli = get_hdfs_client()
    if not hdfs_cli:
        log.error("HDFS client không sẵn sàng, không thể ghi dữ liệu.")
        # Có thể thêm cơ chế lưu tạm vào đâu đó hoặc bỏ qua batch này
        return

    # Tạo đường dẫn HDFS dựa trên ngày hiện tại
    now = datetime.now()
    hdfs_path_with_date = f"{HDFS_BASE_PATH}/{now.strftime('%Y/%m/%d')}"
    # Tên file có thể bao gồm timestamp để tránh ghi đè nếu chạy lại
    hdfs_file_path = f"{hdfs_path_with_date}/data_{now.strftime('%H%M%S%f')}.jsonl"

    log.info(f"Chuẩn bị ghi {len(batch_data)} bản ghi vào HDFS: {hdfs_file_path}")

    # Chuyển đổi list các dict thành định dạng jsonl (mỗi dòng một JSON object)
    # Hoặc bạn có thể ghi thành file JSON lớn, CSV, Parquet,... tùy nhu cầu Batch Layer
    jsonl_data = "\n".join(json.dumps(record, ensure_ascii=False) for record in batch_data)

    try:
        # Đảm bảo thư mục tồn tại
        hdfs_cli.makedirs(hdfs_path_with_date)
        # Ghi dữ liệu (dạng bytes utf-8)
        with hdfs_cli.write(hdfs_file_path, encoding='utf-8', overwrite=False) as writer: # overwrite=False để không ghi đè file cũ nếu trùng tên
             writer.write(jsonl_data)
        log.info(f"✅ Ghi thành công {len(batch_data)} bản ghi vào HDFS.")
    except HdfsError as he:
         log.error(f"Lỗi HDFS khi ghi file {hdfs_file_path}: {he}")
         # Xử lý lỗi, ví dụ thử lại, ghi vào dead letter queue,...
    except Exception as e:
         log.error(f"Lỗi không xác định khi ghi vào HDFS {hdfs_file_path}: {e}")


def consume_and_write():
    kafka_cons = get_kafka_consumer()
    if not kafka_cons:
        log.error("Kafka consumer không khởi tạo được. Thoát.")
        return

    message_batch = []
    last_write_time = time.time()

    log.info("Bắt đầu vòng lặp đọc message từ Kafka...")
    while True: # Vòng lặp vô hạn để đọc message
        try:
            for message in kafka_cons:
                # message.value đã được parse thành dict nhờ value_deserializer
                log.debug(f"Nhận message: Offset={message.offset}, Key={message.key}, Value={message.value}")
                message_batch.append(message.value)

                # Kiểm tra điều kiện ghi batch
                current_time = time.time()
                if len(message_batch) >= BATCH_SIZE or (current_time - last_write_time) >= BATCH_INTERVAL_SECONDS:
                    write_batch_to_hdfs(message_batch)
                    message_batch = [] # Reset batch
                    last_write_time = current_time
                    # Commit offset nếu bạn dùng commit thủ công
                    # kafka_cons.commit()

            # Kiểm tra ghi batch lần cuối nếu vòng lặp kết thúc hoặc consumer timeout
            current_time = time.time()
            if message_batch and (current_time - last_write_time) >= BATCH_INTERVAL_SECONDS:
                 write_batch_to_hdfs(message_batch)
                 message_batch = []
                 last_write_time = current_time
                 # Commit offset nếu cần

        except Exception as e:
            log.error(f"Lỗi trong vòng lặp consumer: {e}", exc_info=True)
            # Có thể cần kết nối lại consumer hoặc HDFS client ở đây
            time.sleep(10) # Đợi trước khi thử lại

# --- Xử lý shutdown ---
def handle_shutdown(signum, frame):
    log.warning(f"Nhận được tín hiệu dừng (Signal {signum}). Đang dừng consumer...")
    if consumer:
        consumer.close()
        log.info("Đã đóng Kafka Consumer.")
    # Không cần đóng hdfs_client tường minh với thư viện này
    exit(0)

signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)

# --- Bắt đầu chạy ---
if __name__ == "__main__":
    consume_and_write()