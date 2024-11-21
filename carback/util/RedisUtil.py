import redis
from config import Config

class RedisUtil(object):
    def __init__(self):
        self.redis_client=redis.StrictRedis(Config.REDIS_HOST,Config.REDIS_PORT)

    @staticmethod
    def save_to_redis(self,partition):
        try:
            for key, value in partition:
                self.redis_client.set(key, value)
        except Exception as e:
            print(f"Error saving to Redis: {e}")