from pyspark.sql import SparkSession
import os

#os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.spark:spark-token-provider-kafka-0-10_2.12:3.5.3,org.apache.kafka:kafka-clients:3.5.2 pyspark-shell"

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("KafkaStructuredStreaming").master("local[*]").config("spark.hadoop.fs.defaultFS", "file:///") .config("spark.hadoop.io.file.buffer.size", "65536") \
    .getOrCreate()
# 配置 Kafka
kafka_brokers = "172.31.206.85:9092"
kafka_topic = "test_topic"

# + Kaf
# ka 读取流数据
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.31.206.85:9092") \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "earliest")\
    .load()

# 转换 Kafka 消息为字符串
messages = kafka_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

# 输出到控制台
query = messages.writeStream \
    .outputMode("complete") \
    .option("checkpointLocation", "D:\\project\\checkpoint") \
    .format("console") \
    .start()
query.awaitTermination()
