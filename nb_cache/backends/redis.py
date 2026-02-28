# -*- coding: utf-8 -*-
"""Redis cache backend with sync and async support."""
import time

from nb_cache.backends.base import BaseBackend
from nb_cache.ttl import ttl_to_seconds


class RedisBackend(BaseBackend):
    """Redis backend supporting both sync and async operations.

    Args:
        url: Redis URL (e.g. "redis://localhost:6379/0")
        host: Redis host
        port: Redis port
        db: Redis database number
        password: Redis password
        socket_timeout: Socket timeout in seconds
        max_connections: Maximum connections in pool
        prefix: Key prefix for namespacing
        **kwargs: Extra arguments passed to redis client
    """

    def __init__(self, url=None, host="localhost", port=6379, db=0,
                 password=None, socket_timeout=None, max_connections=None,
                 prefix="", **kwargs):
        super(RedisBackend, self).__init__(**kwargs)
        self._url = url
        self._host = host
        self._port = port
        self._db = db
        self._password = password
        self._socket_timeout = socket_timeout
        self._max_connections = max_connections
        self._prefix = prefix
        self._async_client = None
        self._sync_client = None
        self._extra_kwargs = kwargs

    def _make_key(self, key):
        if self._prefix:
            return "{}:{}".format(self._prefix, key)
        return key

    # --- Lifecycle ---

    async def init(self):
        try:
            import redis.asyncio as aioredis
        except ImportError:
            import aioredis
        connect_kwargs = {}
        if self._socket_timeout:
            connect_kwargs['socket_timeout'] = self._socket_timeout
        if self._max_connections:
            connect_kwargs['max_connections'] = self._max_connections

        if self._url:
            self._async_client = aioredis.from_url(self._url, decode_responses=False, **connect_kwargs)
        else:
            self._async_client = aioredis.Redis(
                host=self._host, port=self._port, db=self._db,
                password=self._password, decode_responses=False,
                **connect_kwargs,
            )
        await super(RedisBackend, self).init()

    def init_sync(self):
        import redis as sync_redis
        connect_kwargs = {}
        if self._socket_timeout:
            connect_kwargs['socket_timeout'] = self._socket_timeout
        if self._max_connections:
            connect_kwargs['max_connections'] = self._max_connections

        if self._url:
            self._sync_client = sync_redis.from_url(self._url, decode_responses=False, **connect_kwargs)
        else:
            self._sync_client = sync_redis.Redis(
                host=self._host, port=self._port, db=self._db,
                password=self._password, decode_responses=False,
                **connect_kwargs,
            )

        if self._async_client is None:
            try:
                import redis.asyncio as aioredis
            except ImportError:
                aioredis = None
            if aioredis is not None:
                if self._url:
                    self._async_client = aioredis.from_url(self._url, decode_responses=False, **connect_kwargs)
                else:
                    self._async_client = aioredis.Redis(
                        host=self._host, port=self._port, db=self._db,
                        password=self._password, decode_responses=False,
                        **connect_kwargs,
                    )

        super(RedisBackend, self).init_sync()

    async def close(self):
        if self._async_client:
            await self._async_client.close()
            self._async_client = None
        await super(RedisBackend, self).close()

    def close_sync(self):
        if self._sync_client:
            self._sync_client.close()
            self._sync_client = None
        super(RedisBackend, self).close_sync()

    async def ping(self):
        if self._async_client:
            return await self._async_client.ping()
        return False

    def ping_sync(self):
        if self._sync_client:
            return self._sync_client.ping()
        return False

    # --- Async interface ---

    async def get(self, key):
        return await self._async_client.get(self._make_key(key))

    async def set(self, key, value, ttl=None):
        ttl_sec = ttl_to_seconds(ttl)
        rkey = self._make_key(key)
        if ttl_sec and ttl_sec > 0:
            await self._async_client.setex(rkey, int(ttl_sec), value)
        else:
            await self._async_client.set(rkey, value)

    async def delete(self, key):
        return await self._async_client.delete(self._make_key(key))

    async def exists(self, key):
        return bool(await self._async_client.exists(self._make_key(key)))

    async def expire(self, key, ttl):
        ttl_sec = ttl_to_seconds(ttl)
        if ttl_sec and ttl_sec > 0:
            return await self._async_client.expire(self._make_key(key), int(ttl_sec))
        return False

    async def get_expire(self, key):
        val = await self._async_client.ttl(self._make_key(key))
        if val == -2:
            return None
        return val

    async def clear(self):
        if self._prefix:
            await self.delete_match("*")
        else:
            await self._async_client.flushdb()

    async def incr(self, key, amount=1):
        return await self._async_client.incrby(self._make_key(key), amount)

    async def get_many(self, *keys):
        if not keys:
            return []
        rkeys = [self._make_key(k) for k in keys]
        return await self._async_client.mget(*rkeys)

    async def set_many(self, pairs, ttl=None):
        ttl_sec = ttl_to_seconds(ttl)
        pipe = self._async_client.pipeline()
        for key, value in pairs.items():
            rkey = self._make_key(key)
            if ttl_sec and ttl_sec > 0:
                pipe.setex(rkey, int(ttl_sec), value)
            else:
                pipe.set(rkey, value)
        await pipe.execute()

    async def delete_many(self, *keys):
        if keys:
            rkeys = [self._make_key(k) for k in keys]
            await self._async_client.delete(*rkeys)

    async def delete_match(self, pattern):
        rpattern = self._make_key(pattern)
        cursor = 0
        while True:
            cursor, keys = await self._async_client.scan(cursor, match=rpattern, count=100)
            if keys:
                await self._async_client.delete(*keys)
            if cursor == 0:
                break

    async def scan(self, pattern):
        rpattern = self._make_key(pattern)
        result = []
        cursor = 0
        prefix_len = len(self._prefix) + 1 if self._prefix else 0
        while True:
            cursor, keys = await self._async_client.scan(cursor, match=rpattern, count=100)
            for k in keys:
                kstr = k.decode('utf-8') if isinstance(k, bytes) else k
                if prefix_len:
                    kstr = kstr[prefix_len:]
                result.append(kstr)
            if cursor == 0:
                break
        return result

    async def get_match(self, pattern):
        keys = await self.scan(pattern)
        result = {}
        for k in keys:
            val = await self.get(k)
            if val is not None:
                result[k] = val
        return result

    async def get_keys_count(self):
        return await self._async_client.dbsize()

    async def get_size(self):
        return await self.get_keys_count()

    # --- Lock (async) ---

    async def set_lock(self, key, ttl):
        ttl_sec = ttl_to_seconds(ttl)
        rkey = self._make_key("__lock__:{}".format(key))
        result = await self._async_client.set(rkey, b"1", nx=True, ex=int(ttl_sec or 60))
        return result is not None and result is not False

    async def unlock(self, key):
        rkey = self._make_key("__lock__:{}".format(key))
        await self._async_client.delete(rkey)
        return True

    async def is_locked(self, key):
        rkey = self._make_key("__lock__:{}".format(key))
        return bool(await self._async_client.exists(rkey))

    # --- Sync interface ---

    def get_sync(self, key):
        return self._sync_client.get(self._make_key(key))

    def set_sync(self, key, value, ttl=None):
        ttl_sec = ttl_to_seconds(ttl)
        rkey = self._make_key(key)
        if ttl_sec and ttl_sec > 0:
            self._sync_client.setex(rkey, int(ttl_sec), value)
        else:
            self._sync_client.set(rkey, value)

    def delete_sync(self, key):
        return self._sync_client.delete(self._make_key(key))

    def exists_sync(self, key):
        return bool(self._sync_client.exists(self._make_key(key)))

    def expire_sync(self, key, ttl):
        ttl_sec = ttl_to_seconds(ttl)
        if ttl_sec and ttl_sec > 0:
            return self._sync_client.expire(self._make_key(key), int(ttl_sec))
        return False

    def get_expire_sync(self, key):
        val = self._sync_client.ttl(self._make_key(key))
        if val == -2:
            return None
        return val

    def clear_sync(self):
        if self._prefix:
            self.delete_match_sync("*")
        else:
            self._sync_client.flushdb()

    def incr_sync(self, key, amount=1):
        return self._sync_client.incrby(self._make_key(key), amount)

    def get_many_sync(self, *keys):
        if not keys:
            return []
        rkeys = [self._make_key(k) for k in keys]
        return self._sync_client.mget(*rkeys)

    def set_many_sync(self, pairs, ttl=None):
        ttl_sec = ttl_to_seconds(ttl)
        pipe = self._sync_client.pipeline()
        for key, value in pairs.items():
            rkey = self._make_key(key)
            if ttl_sec and ttl_sec > 0:
                pipe.setex(rkey, int(ttl_sec), value)
            else:
                pipe.set(rkey, value)
        pipe.execute()

    def delete_many_sync(self, *keys):
        if keys:
            rkeys = [self._make_key(k) for k in keys]
            self._sync_client.delete(*rkeys)

    def delete_match_sync(self, pattern):
        rpattern = self._make_key(pattern)
        cursor = 0
        while True:
            cursor, keys = self._sync_client.scan(cursor, match=rpattern, count=100)
            if keys:
                self._sync_client.delete(*keys)
            if cursor == 0:
                break

    def scan_sync(self, pattern):
        rpattern = self._make_key(pattern)
        result = []
        cursor = 0
        prefix_len = len(self._prefix) + 1 if self._prefix else 0
        while True:
            cursor, keys = self._sync_client.scan(cursor, match=rpattern, count=100)
            for k in keys:
                kstr = k.decode('utf-8') if isinstance(k, bytes) else k
                if prefix_len:
                    kstr = kstr[prefix_len:]
                result.append(kstr)
            if cursor == 0:
                break
        return result

    def get_match_sync(self, pattern):
        keys = self.scan_sync(pattern)
        result = {}
        for k in keys:
            val = self.get_sync(k)
            if val is not None:
                result[k] = val
        return result

    def get_keys_count_sync(self):
        return self._sync_client.dbsize()

    def get_size_sync(self):
        return self.get_keys_count_sync()

    # --- Lock (sync) ---

    def set_lock_sync(self, key, ttl):
        ttl_sec = ttl_to_seconds(ttl)
        rkey = self._make_key("__lock__:{}".format(key))
        result = self._sync_client.set(rkey, b"1", nx=True, ex=int(ttl_sec or 60))
        return result is not None and result is not False

    def unlock_sync(self, key):
        rkey = self._make_key("__lock__:{}".format(key))
        self._sync_client.delete(rkey)
        return True

    def is_locked_sync(self, key):
        rkey = self._make_key("__lock__:{}".format(key))
        return bool(self._sync_client.exists(rkey))
