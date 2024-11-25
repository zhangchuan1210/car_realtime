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

    @staticmethod
    def consumeBySpark(spark, topic_name):
        kafka_stream = spark.readStream.format("kafka").option("kafka.bootstrap.servers",
                                                               Config.KAFKA_BROKER_URL).option("failOnDataLoss", "false") \
                                                               .option("startingOffsets", "earliest").option("subscribe",topic_name).load()
        return kafka_stream
