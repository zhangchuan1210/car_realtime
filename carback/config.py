# config.py

class Config:
    SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://root:zc142500@localhost/car'  # Hive 连接字符串
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    REDIS_HOST = 'localhost'
    REDIS_PORT = 6379
    KAFKA_BROKER_URL = '127.0.0.1:9092'
    KAFKA_TOPIC = 'company_updates'
