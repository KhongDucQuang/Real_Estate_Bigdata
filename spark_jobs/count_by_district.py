# File: spark_jobs/count_by_district.py (Đã sửa, không dùng f-string)
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

if __name__ == "__main__":
    # 1. Tạo SparkSession
    spark = SparkSession.builder \
        .appName("District Count Batch Job") \
        .getOrCreate()

    # 2. Định nghĩa đường dẫn Input/Output trên HDFS
    # !!! Thay đổi đường dẫn input cho phù hợp với dữ liệu của bạn !!!
    input_path = "hdfs://namenode:9000/user/root/realestate_data/raw/alonhadat/2025/04/24/*"
    output_path = "hdfs://namenode:9000/user/root/realestate_data/batch_views_spark/district_counts"

    # Sử dụng .format() thay vì f-string
    print("Đọc dữ liệu từ: {}".format(input_path))
    print("Ghi kết quả tới: {}".format(output_path))

    try:
        # 3. Đọc dữ liệu JSON Lines từ HDFS
        raw_df = spark.read.json(input_path)

        # 4. Xử lý dữ liệu
        print("Bắt đầu xử lý dữ liệu...")
        district_counts = raw_df.select(col("quan_huyen")) \
                                .filter(col("quan_huyen").isNotNull() & (col("quan_huyen") != "")) \
                                .groupBy("quan_huyen") \
                                .agg(count("*").alias("so_luong"))

        print("Xử lý hoàn tất. Số lượng quận/huyện tìm thấy:", district_counts.count())
        # district_counts.show(5)

        # 5. Ghi kết quả ra HDFS
        print("Đang ghi kết quả vào {}...".format(output_path)) # Sử dụng .format()
        district_counts.write.mode("overwrite").parquet(output_path)
        # Hoặc ghi CSV:
        # district_counts.write.mode("overwrite").csv(output_path, header=True)

        print("✅ Ghi kết quả thành công!")

    except Exception as e:
        # Sử dụng .format()
        print("❌ Đã xảy ra lỗi trong quá trình xử lý Spark: {}".format(e))
        # import traceback
        # traceback.print_exc()

    finally:
        # 6. Dừng SparkSession
        spark.stop()
        print("SparkSession đã dừng.")