import redis.asyncio as redis
import hashlib
import json
from typing import List, Dict

class RedisCache:
    def __init__(self, host="redis", port=6379, db=0):
        self.client = redis.Redis(host=host, port=port, db=db)

    def _make_hash_key(self, text: str) -> str:
        return hashlib.sha256(text.encode()).hexdigest()

    def make_batch_keys(self, texts: List[str]) -> List[str]:
        return [self._make_hash_key(text) for text in texts]

    async def get_many(self, keys: List[str]) -> List[Dict | None]:
        cached_raw = await self.client.mget(keys)
        return [json.loads(item) if item else None for item in cached_raw]

    async def set_many(self, key_value_pairs: Dict[str, Dict], expire_seconds: int = 3600):
        pipeline = self.client.pipeline()
        for key, value in key_value_pairs.items():
            pipeline.set(key, json.dumps(value), ex=expire_seconds)
        await pipeline.execute()

    async def get(self, key: str) -> Dict | None:
        data = await self.client.get(key)
        return json.loads(data) if data else None

    async def set(self, key: str, value: Dict, expire_seconds: int = 3600):
        await self.client.set(key, json.dumps(value), ex=expire_seconds)