from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, date_format, expr, element_at, to_timestamp, col, split, to_date
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, ArrayType, TimestampType, DoubleType, DateType
import subprocess
import logging
import threading
import json


def jobCoinData(spark):
    json_schema = ArrayType(StructType([
        StructField("Name", StringType(), True),
        StructField("Symbol", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Volume", DoubleType(), True),
        StructField("Market Cap", DoubleType(), True)
    ]))

    # Định nghĩa tham số Kafka
    kafka_params = {
        "kafka.bootstrap.servers": "kafka:9092",
        "subscribe": "coin",
        "startingOffsets": "latest",
        "failOnDataLoss": "false"
    }

    # Đọc dữ liệu từ Kafka
    kafka_df = spark.readStream.format("kafka").options(**kafka_params).load()

    print("Data vn30")
    print(kafka_df)

    # Chuyển đổi cột 'value' từ dạng binary sang chuỗi JSON
    kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

    stock_df = kafka_df.select(from_json(col("value"), json_schema).alias("data"))
    stock_df.printSchema()

    # Sử dụng hàm explode để biến đổi mảng thành các hàng
    stock_df = stock_df.select(explode(col("data")).alias("stock_data")).select("stock_data.*")

    # Định nghĩa đường dẫn xuất HDFS
    output_path = "hdfs://namenode:8020/user/root/kafka_data"

    # Chỉ định vị trí checkpoint
    checkpoint_location_hdfs = "hdfs://namenode:8020/user/root/checkpoints_hdfs"
  
    # Ghi dữ liệu vào HDFS dưới dạng file parquet
    hdfs_query = stock_df.writeStream \
        .outputMode("append") \
        .format("json") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_location_hdfs) \
        .start()
    
    hdfs_query.awaitTermination()

    # run file hadoop_to_spark.py
    # subprocess.run(["python3", "hadoop_to_elastic.py"])

def readStreamTime(spark):
    json_schema = ArrayType(StructType([
        StructField("Name", StringType(), True),
        StructField("Symbol", StringType(), True),
        StructField("Date", StringType(), True),
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Volume", DoubleType(), True),
        StructField("Market Cap", DoubleType(), True)
    ]))

    # Định nghĩa tham số Kafka
    kafka_params = {
        "kafka.bootstrap.servers": "kafka:9092",
        "subscribe": "realtime",
        "startingOffsets": "latest"
    }

    # Đọc dữ liệu từ Kafka
    kafka_df = spark.readStream.format("kafka").options(**kafka_params).load()
  
    # Chuyển đổi cột 'value' từ dạng binary sang chuỗi JSON
    kafka_df = kafka_df.selectExpr("CAST(value AS STRING)")

    # Chuyển đổi cột 'value' từ chuỗi JSON sang dữ liệu JSON
    data_df = kafka_df.select(from_json(col("value"), json_schema).alias("jsonData"))

    # data_df = data_df.withColumn("jsonData.time", to_timestamp(element_at("jsonData.time", 1), "HH:mm:ss"))  # Điều chỉnh định dạng của chuỗi 'time' tương ứng

    # unique_data_df = data_df.select("jsonData.*").dropDuplicates(['total_minutes'])
    query = data_df.writeStream.outputMode("append").format("console").start()

    
    try:
        data_df.writeStream .format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "https://btl-bigdata-nghia.es.us-central1.gcp.cloud.es.io") \
            .option("es.port", "9243") \
            .option("es.resource", "realtime") \
            .option("es.net.http.auth.user", "elastic") \
            .option("es.net.http.auth.pass", "4mTGdc4MvU7rJ2YJ5DcLllF0") \
            .option("es.nodes.wan.only", "true") \
            .option("checkpointLocation", "../checkpoint") \
            .outputMode("append") \
            .option("failOnDataLoss", "false") \
            .start()
        logging.info("Dữ liệu đã được gửi thành công lên Elasticsearch!")
    except Exception as e:
        logging.error("Đã xảy ra lỗi khi gửi dữ liệu lên Elasticsearch: %s", str(e))

    query.awaitTermination()


# def show_data(spark):
#     json_schema = StructType([
#         StructField("Date", StringType(), True),
#         StructField("Open", StringType(), True),
#         StructField("High", StringType(), True),
#         StructField("Low", StringType(), True),
#         StructField("Close", StringType(), True),
#         StructField("Adj Close", StringType(), True),
#         StructField("Volume", StringType(), True),
#         StructField("company", StringType(), True)
#     ])
#     input_path = "hdfs://namenode:8020/dataInput/output.json"
#     df = spark.read.option('multiline', True).schema(json_schema).json(input_path)
#     df = df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd")) \
#         .withColumn("Open", col("Open").cast("double")) \
#         .withColumn("High", col("High").cast("double")) \
#         .withColumn("Low", col("Low").cast("double")) \
#         .withColumn("Close", col("Close").cast("double")) \
#         .withColumn("Adj Close", col("Adj Close").cast("double")) \
#         .withColumn("Volume", col("Volume").cast("long"))
    
#     df.printSchema()
#     df.show(35, truncate=False)
    
#     company_names = df.select("company").distinct().rdd.flatMap(lambda x: x).collect()
    
#     for company_name in company_names:
#         filtered_df = df.filter(col("company") == company_name)
#         filtered_df.show(truncate=False)
        
#         if not filtered_df.rdd.isEmpty():
#             hdfs_output_path = f"hdfs://namenode:8020/user/root/{company_name}_processed"
            
            
#             filtered_df.write.json(hdfs_output_path)

# def show_one_stock(spark):
#     json_schema = StructType([
#         StructField("Date", StringType(), True),
#         StructField("Open", StringType(), True),
#         StructField("High", StringType(), True),
#         StructField("Low", StringType(), True),
#         StructField("Close", StringType(), True),
#         StructField("Adj Close", StringType(), True),
#         StructField("Volume", StringType(), True),
#         StructField("company", StringType(), True)
#     ])
    
#     json_data = [
#         {
#             "Date": "2022-12-19",
#             "Open": "244.860001",
#             "High": "245.210007",
#             "Low": "238.710007",
#             "Close": "240.449997",
#             "Adj Close": "238.3367",
#             "Volume": "29696400.0",  # Chuyển đổi giá trị thành kiểu double
#             "company": "MSFT"
#         },
#         {
#             "Date": "2022-12-20",
#             "Open": "239.399994",
#             "High": "242.910004",
#             "Low": "238.419998",
#             "Close": "241.800003",
#             "Adj Close": "239.674835",
#             "Volume": "25150800.0",  # Chuyển đổi giá trị thành kiểu double
#             "company": "MSFT"
#         }
#     ]
    
#     df = spark.createDataFrame(json_data, schema=json_schema)
#     df = df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd")) \
#         .withColumn("Open", col("Open").cast("double")) \
#         .withColumn("High", col("High").cast("double")) \
#         .withColumn("Low", col("Low").cast("double")) \
#         .withColumn("Close", col("Close").cast("double")) \
#         .withColumn("Adj Close", col("Adj Close").cast("double")) \
#         .withColumn("Volume", col("Volume").cast("long"))
#     df.show()
#     try:
#         # .option("es.nodes", "https://btl-bigdata-nghia.kb.us-central1.gcp.cloud.es.io") \
#         # .option("es.port", "9243") \
#         df.write.format("org.elasticsearch.spark.sql") \
#             .option("es.nodes", "https://btl-bigdata-nghia.es.us-central1.gcp.cloud.es.io") \
#             .option("es.port", "9243") \
#             .option("es.resource", "search-nghia_nghia") \
#             .option("es.net.http.auth.user", "elastic") \
#             .option("es.net.http.auth.pass", "4mTGdc4MvU7rJ2YJ5DcLllF0") \
#             .option("es.nodes.wan.only", "true") \
#             .mode("overwrite") \
#             .save()
#         logging.info("Dữ liệu đã được gửi thành công lên Elasticsearch!")
#     except Exception as e:
#         logging.error("Đã xảy ra lỗi khi gửi dữ liệu lên Elasticsearch: %s", str(e))

def calculate_and_add_percentage_change(spark):
    json_schema = StructType([
        StructField("Date", StringType(), True),
        StructField("Open", StringType(), True),
        StructField("High", StringType(), True),
        StructField("Low", StringType(), True),
        StructField("Close", StringType(), True),
        StructField("Adj Close", StringType(), True),
        StructField("Volume", StringType(), True),
        StructField("company", StringType(), True)
    ])
    input_path = "hdfs://namenode:8020/dataInput/output.json"

    # Đọc dữ liệu từ tệp JSON
    df = spark.read.option('multiline', True).json(input_path)
    df = df.withColumn("Date", to_date(col("Date"), "yyyy-MM-dd")) \
        .withColumn("Open", col("Open").cast("double")) \
        .withColumn("High", col("High").cast("double")) \
        .withColumn("Low", col("Low").cast("double")) \
        .withColumn("Close", col("Close").cast("double")) \
        .withColumn("Adj Close", col("Adj Close").cast("double")) \
        .withColumn("Volume", col("Volume").cast("long"))

    # Lọc các dòng với giá trị "Date" là "2023-12-15"
    filtered_df = df.filter(col("Date") == "2023-12-15")

    # Thêm cột mới "PercentageChange"
    filtered_df = filtered_df.withColumn("PercentageChange", ((col("Close") - col("Open")) / col("Open")) * 100)

    # Hiển thị kết quả
    filtered_df.show(truncate=False)

    # Lưu kết quả vào một biến mới có trường company
    result_with_company = filtered_df.select("company", "PercentageChange")

    # Hiển thị kết quả cuối cùng
    result_with_company.show(truncate=False)
    try:
        # .option("es.nodes", "https://btl-bigdata-nghia.kb.us-central1.gcp.cloud.es.io") \
        # .option("es.port", "9243") \
        result_with_company.write.format("org.elasticsearch.spark.sql") \
            .option("es.nodes", "https://btl-bigdata-nghia.es.us-central1.gcp.cloud.es.io") \
            .option("es.port", "9243") \
            .option("es.resource", "hello_my_friend") \
            .option("es.net.http.auth.user", "elastic") \
            .option("es.net.http.auth.pass", "4mTGdc4MvU7rJ2YJ5DcLllF0") \
            .option("es.nodes.wan.only", "true") \
            .mode("overwrite") \
            .save()
        logging.info("Dữ liệu đã được gửi thành công lên Elasticsearch!")
    except Exception as e:
        logging.error("Đã xảy ra lỗi khi gửi dữ liệu lên Elasticsearch: %s", str(e))


if __name__ == "__main__":
    # Khởi tạo SparkSession
    spark = SparkSession.builder.appName("HelloMyFriend").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    t1 = threading.Thread(target=jobCoinData, args=(spark,))
    t2 = threading.Thread(target=calculate_and_add_percentage_change, args=(spark,))
    t1.start()
    t2.start()

    t1.join()
    t2.join()

    print("Done!")

