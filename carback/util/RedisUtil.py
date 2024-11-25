import redis
from config import Config
class RedisUtil(object):
    redis_client=redis.StrictRedis(Config.REDIS_HOST,Config.REDIS_PORT)
    @staticmethod
    def save_to_redis(batch_df,batch_id):
        try:
            df=batch_df.toPandas()
            for _, row in df.iterrows():
                # 转换为字符串格式（可用JSON或其他格式）
                RedisUtil.redis_client.rpush("haoyouliang", row.to_json())
                print(row.to_json())
        except Exception as e:
            print(f"Error saving to Redis: {e}")