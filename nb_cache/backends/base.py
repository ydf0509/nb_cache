# -*- coding: utf-8 -*-
"""Abstract base class for cache backends.

Every backend must provide both sync and async interfaces.
"""
import asyncio


class BaseBackend(object):
    """Abstract cache backend with sync and async interfaces."""

    def __init__(self, **kwargs):
        self._is_init = False

    @property
    def is_init(self):
        return self._is_init

    # --- Lifecycle ---

    async def init(self):
        self._is_init = True

    def init_sync(self):
        self._is_init = True

    async def close(self):
        self._is_init = False

    def close_sync(self):
        self._is_init = False

    async def ping(self):
        return True

    def ping_sync(self):
        return True

    # --- Core GET/SET/DELETE (async) ---

    async def get(self, key):
        raise NotImplementedError

    async def set(self, key, value, ttl=None):
        raise NotImplementedError

    async def delete(self, key):
        raise NotImplementedError

    async def exists(self, key):
        raise NotImplementedError

    async def expire(self, key, ttl):
        raise NotImplementedError

    async def get_expire(self, key):
        """Return remaining TTL in seconds, or -1 if no expiry, or None if key missing."""
        raise NotImplementedError

    async def clear(self):
        raise NotImplementedError

    async def incr(self, key, amount=1):
        raise NotImplementedError

    # --- Batch operations (async) ---

    async def get_many(self, *keys):
        results = []
        for k in keys:
            results.append(await self.get(k))
        return results

    async def set_many(self, pairs, ttl=None):
        for key, value in pairs.items():
            await self.set(key, value, ttl=ttl)

    async def delete_many(self, *keys):
        for k in keys:
            await self.delete(k)

    async def delete_match(self, pattern):
        raise NotImplementedError

    # --- Scan / Match (async) ---

    async def scan(self, pattern):
        raise NotImplementedError

    async def get_match(self, pattern):
        raise NotImplementedError

    async def get_keys_count(self):
        raise NotImplementedError

    async def get_size(self):
        raise NotImplementedError

    # --- Lock (async) ---

    async def set_lock(self, key, ttl):
        raise NotImplementedError

    async def unlock(self, key):
        raise NotImplementedError

    async def is_locked(self, key):
        raise NotImplementedError

    # --- Set operations (async) ---

    async def set_add(self, key, *values):
        raise NotImplementedError

    async def set_remove(self, key, *values):
        raise NotImplementedError

    async def set_pop(self, key, count=1):
        raise NotImplementedError

    # --- Core GET/SET/DELETE (sync) ---

    def get_sync(self, key):
        raise NotImplementedError

    def set_sync(self, key, value, ttl=None):
        raise NotImplementedError

    def delete_sync(self, key):
        raise NotImplementedError

    def exists_sync(self, key):
        raise NotImplementedError

    def expire_sync(self, key, ttl):
        raise NotImplementedError

    def get_expire_sync(self, key):
        raise NotImplementedError

    def clear_sync(self):
        raise NotImplementedError

    def incr_sync(self, key, amount=1):
        raise NotImplementedError

    # --- Batch (sync) ---

    def get_many_sync(self, *keys):
        return [self.get_sync(k) for k in keys]

    def set_many_sync(self, pairs, ttl=None):
        for key, value in pairs.items():
            self.set_sync(key, value, ttl=ttl)

    def delete_many_sync(self, *keys):
        for k in keys:
            self.delete_sync(k)

    def delete_match_sync(self, pattern):
        raise NotImplementedError

    # --- Scan (sync) ---

    def scan_sync(self, pattern):
        raise NotImplementedError

    def get_match_sync(self, pattern):
        raise NotImplementedError

    def get_keys_count_sync(self):
        raise NotImplementedError

    def get_size_sync(self):
        raise NotImplementedError

    # --- Lock (sync) ---

    def set_lock_sync(self, key, ttl):
        raise NotImplementedError

    def unlock_sync(self, key):
        raise NotImplementedError

    def is_locked_sync(self, key):
        raise NotImplementedError
