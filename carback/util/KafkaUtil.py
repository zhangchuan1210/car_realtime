from kafka import KafkaProducer, KafkaConsumer
from config import Config


class KafkaUtil(object):
    @staticmethod
    def createKafkaConsumer(topic_name):
        consumer = KafkaConsumer(topic_name, bootstrap_servers=Config.KAFKA_BROKER_URL)
        return consumer

    @staticmethod
    def createKafkaProducer():
        producer = KafkaProducer(bootstrap_servers=Config.KAFKA_BROKER_URL)
        return producer

    def produce(topic_name, message):
        try:
            KafkaUtil.createKafkaProducer().send(topic_name, value=message)
        except Exception as e:
            print("发生错误: {e}")

    @staticmethod
    def consumeBySpark(spark, topic_name):
        kafka_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers",
                                                               Config.KAFKA_BROKER_URL).option("startingOffsets", "earliest").option("subscribe",topic_name).load()
        return kafka_stream
