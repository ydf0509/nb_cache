# -*- coding: utf-8 -*-
"""Dual cache backend: memory (L1) + Redis (L2).

Reads check memory first, then Redis on miss.
Writes go to both memory and Redis.
"""
from nb_cache.backends.base import BaseBackend
from nb_cache.backends.memory import MemoryBackend
from nb_cache.backends.redis import RedisBackend
from nb_cache.ttl import ttl_to_seconds


class DualBackend(BaseBackend):
    """Two-layer cache: fast in-memory L1 + Redis L2.

    Args:
        memory_size: Max entries for the memory layer.
        local_ttl: Default TTL for local memory cache (seconds).
            If set, local cache uses this TTL regardless of the outer TTL,
            which is useful for keeping local cache short-lived.
        url/host/port/db/password: Redis connection parameters.
        prefix: Redis key prefix.
    """

    def __init__(self, memory_size=1000, local_ttl=None,
                 url=None, host="localhost", port=6379, db=0,
                 password=None, prefix="", **kwargs):
        super(DualBackend, self).__init__(**kwargs)
        self._local_ttl = local_ttl
        self._memory = MemoryBackend(size=memory_size)
        self._redis = RedisBackend(
            url=url, host=host, port=port, db=db,
            password=password, prefix=prefix, **kwargs,
        )

    def _local_ttl_val(self, ttl):
        if self._local_ttl is not None:
            return self._local_ttl
        return ttl

    # --- Lifecycle ---

    async def init(self):
        await self._memory.init()
        await self._redis.init()
        await super(DualBackend, self).init()

    def init_sync(self):
        self._memory.init_sync()
        self._redis.init_sync()
        super(DualBackend, self).init_sync()

    async def close(self):
        await self._memory.close()
        await self._redis.close()
        await super(DualBackend, self).close()

    def close_sync(self):
        self._memory.close_sync()
        self._redis.close_sync()
        super(DualBackend, self).close_sync()

    async def ping(self):
        return await self._redis.ping()

    def ping_sync(self):
        return self._redis.ping_sync()

    # --- Async interface ---

    async def get(self, key):
        val = await self._memory.get(key)
        if val is not None:
            return val
        val = await self._redis.get(key)
        if val is not None:
            local_ttl = self._local_ttl_val(None)
            await self._memory.set(key, val, ttl=local_ttl)
        return val

    async def set(self, key, value, ttl=None):
        await self._redis.set(key, value, ttl=ttl)
        await self._memory.set(key, value, ttl=self._local_ttl_val(ttl))

    async def delete(self, key):
        await self._memory.delete(key)
        return await self._redis.delete(key)

    async def exists(self, key):
        if await self._memory.exists(key):
            return True
        return await self._redis.exists(key)

    async def expire(self, key, ttl):
        await self._memory.expire(key, self._local_ttl_val(ttl))
        return await self._redis.expire(key, ttl)

    async def get_expire(self, key):
        return await self._redis.get_expire(key)

    async def clear(self):
        await self._memory.clear()
        await self._redis.clear()

    async def incr(self, key, amount=1):
        result = await self._redis.incr(key, amount)
        self._memory.set_sync(key, result, ttl=self._local_ttl_val(None))
        return result

    async def get_many(self, *keys):
        results = []
        missed_keys = []
        missed_indices = []
        for i, k in enumerate(keys):
            val = await self._memory.get(k)
            results.append(val)
            if val is None:
                missed_keys.append(k)
                missed_indices.append(i)
        if missed_keys:
            redis_vals = await self._redis.get_many(*missed_keys)
            for idx, val in zip(missed_indices, redis_vals):
                results[idx] = val
                if val is not None:
                    k = keys[idx]
                    await self._memory.set(k, val, ttl=self._local_ttl_val(None))
        return results

    async def set_many(self, pairs, ttl=None):
        await self._redis.set_many(pairs, ttl=ttl)
        await self._memory.set_many(pairs, ttl=self._local_ttl_val(ttl))

    async def delete_many(self, *keys):
        await self._memory.delete_many(*keys)
        await self._redis.delete_many(*keys)

    async def delete_match(self, pattern):
        self._memory.delete_match_sync(pattern)
        await self._redis.delete_match(pattern)

    async def scan(self, pattern):
        return await self._redis.scan(pattern)

    async def get_match(self, pattern):
        return await self._redis.get_match(pattern)

    async def get_keys_count(self):
        return await self._redis.get_keys_count()

    async def get_size(self):
        return await self._redis.get_size()

    async def set_lock(self, key, ttl):
        return await self._redis.set_lock(key, ttl)

    async def unlock(self, key):
        return await self._redis.unlock(key)

    async def is_locked(self, key):
        return await self._redis.is_locked(key)

    # --- Sync interface ---

    def get_sync(self, key):
        val = self._memory.get_sync(key)
        if val is not None:
            return val
        val = self._redis.get_sync(key)
        if val is not None:
            local_ttl = self._local_ttl_val(None)
            self._memory.set_sync(key, val, ttl=local_ttl)
        return val

    def set_sync(self, key, value, ttl=None):
        self._redis.set_sync(key, value, ttl=ttl)
        self._memory.set_sync(key, value, ttl=self._local_ttl_val(ttl))

    def delete_sync(self, key):
        self._memory.delete_sync(key)
        return self._redis.delete_sync(key)

    def exists_sync(self, key):
        if self._memory.exists_sync(key):
            return True
        return self._redis.exists_sync(key)

    def expire_sync(self, key, ttl):
        self._memory.expire_sync(key, self._local_ttl_val(ttl))
        return self._redis.expire_sync(key, ttl)

    def get_expire_sync(self, key):
        return self._redis.get_expire_sync(key)

    def clear_sync(self):
        self._memory.clear_sync()
        self._redis.clear_sync()

    def incr_sync(self, key, amount=1):
        result = self._redis.incr_sync(key, amount)
        self._memory.set_sync(key, result, ttl=self._local_ttl_val(None))
        return result

    def get_many_sync(self, *keys):
        results = []
        missed_keys = []
        missed_indices = []
        for i, k in enumerate(keys):
            val = self._memory.get_sync(k)
            results.append(val)
            if val is None:
                missed_keys.append(k)
                missed_indices.append(i)
        if missed_keys:
            redis_vals = self._redis.get_many_sync(*missed_keys)
            for idx, val in zip(missed_indices, redis_vals):
                results[idx] = val
                if val is not None:
                    k = keys[idx]
                    self._memory.set_sync(k, val, ttl=self._local_ttl_val(None))
        return results

    def set_many_sync(self, pairs, ttl=None):
        self._redis.set_many_sync(pairs, ttl=ttl)
        self._memory.set_many_sync(pairs, ttl=self._local_ttl_val(ttl))

    def delete_many_sync(self, *keys):
        self._memory.delete_many_sync(*keys)
        self._redis.delete_many_sync(*keys)

    def delete_match_sync(self, pattern):
        self._memory.delete_match_sync(pattern)
        self._redis.delete_match_sync(pattern)

    def scan_sync(self, pattern):
        return self._redis.scan_sync(pattern)

    def get_match_sync(self, pattern):
        return self._redis.get_match_sync(pattern)

    def get_keys_count_sync(self):
        return self._redis.get_keys_count_sync()

    def get_size_sync(self):
        return self._redis.get_size_sync()

    def set_lock_sync(self, key, ttl):
        return self._redis.set_lock_sync(key, ttl)

    def unlock_sync(self, key):
        return self._redis.unlock_sync(key)

    def is_locked_sync(self, key):
        return self._redis.is_locked_sync(key)
