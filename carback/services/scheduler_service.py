import threading
from extensions import spark
from pyspark.sql import functions as F
import json
from services.car_service import CarService
from util.RedisUtil import RedisUtil
from util.KafkaUtil import KafkaUtil


class SchedulerService:
    def consume_itcast_order_stream(self):
        # {"c_name": "423", "oil_consume": 2.8}
        print("消费线程运行")
        topics = "itcast_order"
        kafka_stream = KafkaUtil.consumeBySpark(spark, topics)
        messages = kafka_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
        # 解析 JSON 数据
        events = messages.select(
            F.from_json(F.col("value"), "struct<c_name:string,oil_consume:double>").alias("json_data"))
        # 将数据按 c_name 分组，计算油耗的平均值
        oil_avg = events.select("json_data.c_name", "json_data.oil_consume").groupBy("c_name").agg(
            F.avg("oil_consume").alias("avg_oil_consume"))
        # 使用 foreachBatch 将 DataFrame 转换为 RDD
        query = oil_avg.writeStream.foreachBatch(RedisUtil.save_to_redis) \
            .outputMode("complete") \
            .start()
        print("流式查询")
        query.awaitTermination()

    def consume_itcast_order(self):
        task_thread = threading.Thread(target=self.consume_itcast_order_stream, daemon=True)
        task_thread.start()
        print("Thread started, Flask app will now run.")

    # 从 MySQL 读取数据并发送到 Kafka
    def produce_itcast_order(self):
        global count
        topics = "itcast_order"
        # 计数器
        count = 0
        try:
            result = CarService().get_all_cars()
            if result:
                # 将数据转换为 JSON 格式并发送到 Kafka
                messages = json.dumps(result)
                for message in messages:
                    KafkaUtil.createKafkaProducer().send(topics, value=message)
                    print("数据发送到 Kafka: {message}")
                    count += 1  # 更新计数器
                print("发送数据条数为：" + str(count))
            else:
                print("未找到符合条件的数据。")
        except Exception as e:
            print("发生错误: {e}" + e)
