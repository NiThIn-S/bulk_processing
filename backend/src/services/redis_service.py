import os
import redis.asyncio as redis

import config
from config.logger import log


rp = config.REDIS_KEY_PREFIX
expire_time = 60*60*24*1 #(1 day)

class RedisService:
    def __init__(self):
        self.r = None
        self.pipe = None
        self.pool = None

    async def connect_redis(self):
        """Establish connection to redis."""
        self.pool = redis.ConnectionPool(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            password=config.REDIS_PASSWORD,
            socket_connect_timeout=10,
        )
        self.r = redis.Redis(connection_pool=self.pool)
        self.pipe = self.r.pipeline()

    async def check_online(self):
        await self.r.get("dummy_test_key")

    async def check_status(self):
        try:
            await self.check_online()
        except Exception as e:
            log.error(f"*****Connection to Redis failed.*****, err: {repr(e)}")
            raise RuntimeError("Connection to Redis is not established.")

    async def disconnect_redis(self):
        """Disconnect from redis."""
        if self.pipe:
            await self.pipe.execute()
        await self.pool.disconnect()
        log.info("*****Disconnected from Redis.*****")


redis_service = RedisService()
