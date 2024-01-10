from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round
import logging

# def get_first_open(dataframe):
#     first_open = dataframe.groupBy('ticker', 'companyType') \
#         .agg({'time': 'min', 'open': 'first'}) \
#         .withColumnRenamed("min(time)", "first_time") \
#         .withColumnRenamed("first(open)", "first_open_value") \
#         .select('ticker', 'companyType', 'first_time', 'first_open_value')  

#     return first_open

# def get_end_open(dataframe):
#     end_open = dataframe.groupBy('ticker', 'companyType') \
#         .agg({'time': 'max', 'open': 'last'}) \
#         .withColumnRenamed("max(time)", "end_time") \
#         .withColumnRenamed("last(open)", "end_open_value") \
#         .select('ticker', 'companyType', 'end_time', 'end_open_value')

#     return end_open

# def calculate_growth(dataframe):
#     growth = dataframe.withColumn("growth",
#                                      round(((col("end_open_value") - col("first_open_value")) / col("first_open_value")) * 100, 2))

#     return growth

# def calculate_metrics_per_ticker(dataframe):
#     first_open = get_first_open(dataframe)
#     end_open = get_end_open(dataframe)

#     aggregated_data = dataframe.groupBy('ticker', 'companyType').agg(
#         {'volume': 'sum', 'high': 'max', 'low': 'min'}
#     ).withColumnRenamed("sum(volume)", "total_volume").withColumnRenamed("max(high)", "max_high").withColumnRenamed("min(low)", "min_low")

#     aggregated_data = aggregated_data.join(first_open, on=['ticker', 'companyType'], how='inner')
#     aggregated_data = aggregated_data.join(end_open, on=['ticker', 'companyType'], how='inner')

#     aggregated_data = aggregated_data.withColumn("price_range_percent", round(((col("max_high") - col("min_low")) / col("first_open_value")) * 100, 2))

#     growth = calculate_growth(aggregated_data)
#     aggregated_data = aggregated_data.join(growth.select('ticker', 'companyType', 'growth'), on=['ticker', 'companyType'], how='inner')
#     return aggregated_data

if __name__ == "__main__":
    spark = SparkSession.builder.appName("ReadFromHadoop").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    input_path = "hdfs://namenode:8020/user/root/kafka_data/part-00000-0a85b43e-1ec9-4ca9-8c87-954be32df352-c000.json"
    hadoop_data = spark.read.json(input_path)
    hadoop_data.printSchema()
    hadoop_data.show(5,truncate=False)
    # hadoop_data.filter(hadoop_data['ticker'] == 'HPG').show()

    # calulate_metrics = calculate_metrics_per_ticker(hadoop_data)
    # calulate_metrics.show(35,truncate=False)

    # try:
    #     calulate_metrics.write.format("org.elasticsearch.spark.sql") \
    #         .option("es.nodes", "https://big-data.es.asia-southeast1.gcp.elastic-cloud.com") \
    #         .option("es.port", "9243") \
    #         .option("es.resource", "vn_30") \
    #         .option("es.net.http.auth.user", "elastic") \
    #         .option("es.net.http.auth.pass", "Fqlvu8CGw9jIGdxSsSSR4R1z") \
    #         .option("es.nodes.wan.only", "true") \
    #         .mode("overwrite") \
    #         .save()
    #     logging.info("Dữ liệu đã được gửi thành công lên Elasticsearch!")
    # except Exception as e:
    #     logging.error("Đã xảy ra lỗi khi gửi dữ liệu lên Elasticsearch: %s", str(e))

    spark.stop()
