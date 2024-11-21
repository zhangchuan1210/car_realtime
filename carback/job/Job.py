from extensions import spark
from util.RedisUtil import RedisUtil
import json
from util.KafkaUtil import KafkaUtil
from services.car_service import CarService

global count

# 从 MySQL 读取数据并发送到 Kafka
def produce_itcast_order():
    topics = ["itcast_order"]
    # 计数器
    count = 0
    try:
        result = CarService.get_all_cars()
        if result:
            # 将数据转换为 JSON 格式并发送到 Kafka
            messages = json.dumps(result)
            for message in messages:
                KafkaUtil.createKafkaProducer().send(topics, value=message)
                print("数据发送到 Kafka: {message}")
                count += 1  # 更新计数器
            print("发送数据条数为："+str(count))
        else:
            print("未找到符合条件的数据。")
    except Exception as e:
        print("发生错误: {e}")

def consume_itcast_order_stream():
    topics = ["itcast_order"]
    kafka_stream = KafkaUtil.consumeBySpark(spark,topics)
    events = kafka_stream.map(lambda x: json.loads(x[1]))
    # Perform computation for average oil consumption by car name
    oil_avg = events.map(lambda x: (x["c_name"], x["oil_consume"])) \
        .groupByKey() \
        .map(lambda x:(x[0],sum(x[1])/len(x[1])))
    query = oil_avg.writeStream \
        .foreachRDD(lambda rdd: rdd.foreachPartition(RedisUtil.save_to_redis))\
        .outputMode("append") \
        .format("console") \
        .start()
    query.awaitTermination()