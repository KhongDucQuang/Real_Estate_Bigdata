# File: kafka_cc/producer/kafka_producer.py
import json
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import time

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("kafka_producer")

KAFKA_BROKERS = ['kafka:9093'] # Sử dụng service name và cổng INTERNAL
producer = None

def get_kafka_producer():
    global producer
    if producer is None:
        log.info(f"Đang kết nối tới Kafka brokers: {KAFKA_BROKERS}")
        retry_count = 0
        max_retries = 5
        while producer is None and retry_count < max_retries:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BROKERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8'),
                    # Thêm các tham số khác nếu cần, ví dụ: client_id, retries, acks
                    client_id='realestate-producer'
                )
                log.info("✅ Kết nối Kafka Producer thành công!")
            except NoBrokersAvailable:
                retry_count += 1
                log.warning(f"Không thể kết nối tới Kafka broker. Thử lại lần {retry_count}/{max_retries} sau 5 giây...")
                time.sleep(5)
            except Exception as e:
                log.error(f"Lỗi không xác định khi khởi tạo Kafka Producer: {e}")
                # Không thử lại với lỗi không xác định
                break
        if producer is None:
             log.error("❌ Không thể khởi tạo Kafka Producer sau nhiều lần thử.")

    return producer

def send_to_kafka(data_list, topic_name):
    prod = get_kafka_producer()
    if not prod:
        log.error(f"Kafka Producer không sẵn sàng. Không thể gửi {len(data_list)} bản ghi vào topic '{topic_name}'.")
        return False # Trả về False nếu gửi thất bại

    if not data_list:
        log.info("Không có dữ liệu để gửi vào Kafka.")
        return True # Không có gì để gửi cũng coi là thành công

    log.info(f"Chuẩn bị gửi {len(data_list)} bản ghi vào topic: {topic_name}")
    success_count = 0
    try:
        for item in data_list:
            try:
                # Dùng trường 'link' hoặc một ID duy nhất khác làm key nếu có
                key = item.get("link", item.get("title", f"unknown_key_{time.time()}"))
                if not isinstance(key, str):
                    key = str(key)

                future = prod.send(topic_name, key=key, value=item)
                # Có thể thêm xử lý callback để kiểm tra lỗi gửi từng message (nâng cao)
                # future.add_callback(on_send_success).add_errback(on_send_error)
                success_count += 1
            except Exception as e:
                log.error(f"Lỗi khi gửi bản ghi vào Kafka: {item}. Lỗi: {e}")

        # Đảm bảo tất cả message trong buffer được gửi đi
        prod.flush()
        log.info(f"✅ Đã gửi xong {success_count}/{len(data_list)} bản ghi vào topic: {topic_name}")
        return True
    except Exception as e:
        log.error(f"Lỗi nghiêm trọng khi gửi dữ liệu tới Kafka: {e}")
        return False

def close_kafka_producer():
    global producer
    if producer:
        log.info("Đang đóng Kafka Producer...")
        producer.close()
        producer = None
        log.info("Đã đóng Kafka Producer.")

# Có thể thêm hàm callback (nâng cao)
# def on_send_success(record_metadata):
#     log.debug(f"Gửi thành công: topic={record_metadata.topic} partition={record_metadata.partition} offset={record_metadata.offset}")
# def on_send_error(excp):
#     log.error('Lỗi khi gửi message', exc_info=excp)