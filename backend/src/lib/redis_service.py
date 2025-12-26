import os
import json
import redis.asyncio as redis
from typing import Optional, Dict, List
from uuid import UUID

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
            username=config.REDIS_USERNAME,
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

    async def store_csv(self, batch_id: UUID, csv_content: bytes, ttl: int = 86400):
        """Store CSV content in Redis with TTL."""
        key = f"{rp}csv:{batch_id}"
        await self.r.set(key, csv_content, ex=ttl)
        log.info(f"Stored CSV for batch: {batch_id}")

    async def get_csv(self, batch_id: UUID) -> Optional[bytes]:
        """Get CSV content from Redis."""
        key = f"{rp}csv:{batch_id}"
        result = await self.r.get(key)
        return result

    async def store_rows(self, batch_id: UUID, rows: List[Dict], ttl: int = 86400):
        """Store deduplicated rows in Redis as JSON."""
        key = f"{rp}rows:{batch_id}"
        await self.r.set(key, json.dumps(rows), ex=ttl)
        log.info(f"Stored {len(rows)} rows for batch: {batch_id}")

    async def get_rows(self, batch_id: UUID) -> Optional[List[Dict]]:
        """Get deduplicated rows from Redis."""
        key = f"{rp}rows:{batch_id}"
        result = await self.r.get(key)
        if result:
            return json.loads(result)
        return None

    async def store_status(self, batch_id: UUID, status: Dict, ttl: int = 86400):
        """Store status in Redis as JSON."""
        key = f"{rp}status:{batch_id}"
        await self.r.set(key, json.dumps(status), ex=ttl)
        log.debug(f"Stored status for batch: {batch_id}")

    async def get_status(self, batch_id: UUID) -> Optional[Dict]:
        """Get status from Redis."""
        key = f"{rp}status:{batch_id}"
        result = await self.r.get(key)
        if result:
            return json.loads(result)
        return None

    async def update_status_field(self, batch_id: UUID, field: str, value):
        """Update a specific field in the status JSON."""
        status = await self.get_status(batch_id)
        if status:
            status[field] = value
            await self.store_status(batch_id, status)
        else:
            log.warning(f"Status not found for batch: {batch_id}, cannot update field: {field}")

    async def set_retry_lock(self, batch_id: UUID, ttl: int = 3600) -> bool:
        """
        Set retry lock in Redis. Returns True if set, False if already exists.
        Uses SETNX (set if not exists) for atomic operation.
        """
        key = f"{rp}retry:{batch_id}"
        result = await self.r.set(key, "locked", ex=ttl, nx=True)
        if result:
            log.info(f"Set retry lock for batch: {batch_id}")
        else:
            log.warning(f"Retry lock already exists for batch: {batch_id}")
        return result is not None

    async def check_retry_lock(self, batch_id: UUID) -> bool:
        """Check if retry lock exists."""
        key = f"{rp}retry:{batch_id}"
        result = await self.r.exists(key)
        return result > 0

    async def delete_retry_lock(self, batch_id: UUID):
        """Delete retry lock."""
        key = f"{rp}retry:{batch_id}"
        await self.r.delete(key)
        log.info(f"Deleted retry lock for batch: {batch_id}")


redis_service = RedisService()
