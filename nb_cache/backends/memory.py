# -*- coding: utf-8 -*-
"""In-memory LRU cache backend with sync and async support."""
import asyncio
import fnmatch
import re
import threading
import time
from collections import OrderedDict

from nb_cache.backends.base import BaseBackend
from nb_cache.ttl import ttl_to_seconds


class MemoryBackend(BaseBackend):
    """Thread-safe in-memory LRU cache backend.

    Args:
        size: Maximum number of entries. 0 means unlimited.
        check_interval: How often (seconds) to run passive expiry cleanup.
    """

    def __init__(self, size=0, check_interval=60, **kwargs):
        super(MemoryBackend, self).__init__(**kwargs)
        self._size = size
        self._check_interval = check_interval
        self._store = OrderedDict()   # key -> value
        self._expiry = {}             # key -> expire_timestamp
        self._locks = {}              # key -> expire_timestamp (for locks)
        self._sets = {}               # key -> set (for set operations)
        self._lock = threading.RLock()
        self._last_cleanup = time.time()

    # --- Lifecycle ---

    async def init(self):
        await super(MemoryBackend, self).init()

    def init_sync(self):
        super(MemoryBackend, self).init_sync()

    async def close(self):
        self.clear_sync()
        await super(MemoryBackend, self).close()

    def close_sync(self):
        self.clear_sync()
        super(MemoryBackend, self).close_sync()

    async def ping(self):
        return True

    def ping_sync(self):
        return True

    # --- Internal helpers ---

    def _maybe_cleanup(self):
        now = time.time()
        if now - self._last_cleanup > self._check_interval:
            self._last_cleanup = now
            self._do_cleanup()

    def _do_cleanup(self):
        now = time.time()
        expired_keys = [k for k, exp in self._expiry.items() if exp <= now]
        for k in expired_keys:
            self._remove_key(k)

    def _remove_key(self, key):
        self._store.pop(key, None)
        self._expiry.pop(key, None)

    def _is_expired(self, key):
        exp = self._expiry.get(key)
        if exp is not None and exp <= time.time():
            self._remove_key(key)
            return True
        return False

    def _evict_if_needed(self):
        if self._size > 0:
            while len(self._store) >= self._size:
                self._store.popitem(last=False)

    def _pattern_to_regex(self, pattern):
        regex = fnmatch.translate(pattern)
        return re.compile(regex)

    # --- Sync interface ---

    def get_sync(self, key):
        with self._lock:
            self._maybe_cleanup()
            if key not in self._store or self._is_expired(key):
                return None
            self._store.move_to_end(key)
            return self._store[key]

    def set_sync(self, key, value, ttl=None):
        with self._lock:
            self._maybe_cleanup()
            ttl_sec = ttl_to_seconds(ttl)
            self._evict_if_needed()
            self._store[key] = value
            self._store.move_to_end(key)
            if ttl_sec and ttl_sec > 0:
                self._expiry[key] = time.time() + ttl_sec
            else:
                self._expiry.pop(key, None)

    def delete_sync(self, key):
        with self._lock:
            self._remove_key(key)
            return True

    def exists_sync(self, key):
        with self._lock:
            if key not in self._store or self._is_expired(key):
                return False
            return True

    def expire_sync(self, key, ttl):
        with self._lock:
            if key not in self._store or self._is_expired(key):
                return False
            ttl_sec = ttl_to_seconds(ttl)
            if ttl_sec and ttl_sec > 0:
                self._expiry[key] = time.time() + ttl_sec
            else:
                self._expiry.pop(key, None)
            return True

    def get_expire_sync(self, key):
        with self._lock:
            if key not in self._store or self._is_expired(key):
                return None
            exp = self._expiry.get(key)
            if exp is None:
                return -1
            remaining = exp - time.time()
            return max(0.0, remaining)

    def clear_sync(self):
        with self._lock:
            self._store.clear()
            self._expiry.clear()
            self._locks.clear()
            self._sets.clear()

    def incr_sync(self, key, amount=1):
        with self._lock:
            if key not in self._store or self._is_expired(key):
                self._store[key] = amount
                return amount
            val = self._store[key]
            new_val = val + amount
            self._store[key] = new_val
            return new_val

    def get_many_sync(self, *keys):
        return [self.get_sync(k) for k in keys]

    def set_many_sync(self, pairs, ttl=None):
        for key, value in pairs.items():
            self.set_sync(key, value, ttl=ttl)

    def delete_many_sync(self, *keys):
        for k in keys:
            self.delete_sync(k)

    def delete_match_sync(self, pattern):
        with self._lock:
            regex = self._pattern_to_regex(pattern)
            to_delete = [k for k in self._store if regex.match(k)]
            for k in to_delete:
                self._remove_key(k)

    def scan_sync(self, pattern):
        with self._lock:
            self._maybe_cleanup()
            regex = self._pattern_to_regex(pattern)
            return [k for k in list(self._store.keys()) if regex.match(k) and not self._is_expired(k)]

    def get_match_sync(self, pattern):
        with self._lock:
            self._maybe_cleanup()
            regex = self._pattern_to_regex(pattern)
            result = {}
            for k in list(self._store.keys()):
                if regex.match(k) and not self._is_expired(k):
                    result[k] = self._store[k]
            return result

    def get_keys_count_sync(self):
        with self._lock:
            self._maybe_cleanup()
            return len(self._store)

    def get_size_sync(self):
        return self.get_keys_count_sync()

    # --- Lock (sync) ---

    def set_lock_sync(self, key, ttl):
        with self._lock:
            lock_key = "__lock__:{}".format(key)
            now = time.time()
            exp = self._locks.get(lock_key)
            if exp is not None and exp > now:
                return False
            ttl_sec = ttl_to_seconds(ttl)
            self._locks[lock_key] = now + (ttl_sec or 60)
            return True

    def unlock_sync(self, key):
        with self._lock:
            lock_key = "__lock__:{}".format(key)
            self._locks.pop(lock_key, None)
            return True

    def is_locked_sync(self, key):
        with self._lock:
            lock_key = "__lock__:{}".format(key)
            exp = self._locks.get(lock_key)
            if exp is None:
                return False
            if exp <= time.time():
                self._locks.pop(lock_key, None)
                return False
            return True

    # --- Async interface (delegates to sync with lock) ---

    async def get(self, key):
        return self.get_sync(key)

    async def set(self, key, value, ttl=None):
        self.set_sync(key, value, ttl=ttl)

    async def delete(self, key):
        return self.delete_sync(key)

    async def exists(self, key):
        return self.exists_sync(key)

    async def expire(self, key, ttl):
        return self.expire_sync(key, ttl)

    async def get_expire(self, key):
        return self.get_expire_sync(key)

    async def clear(self):
        self.clear_sync()

    async def incr(self, key, amount=1):
        return self.incr_sync(key, amount)

    async def get_many(self, *keys):
        return self.get_many_sync(*keys)

    async def set_many(self, pairs, ttl=None):
        self.set_many_sync(pairs, ttl=ttl)

    async def delete_many(self, *keys):
        self.delete_many_sync(*keys)

    async def delete_match(self, pattern):
        self.delete_match_sync(pattern)

    async def scan(self, pattern):
        return self.scan_sync(pattern)

    async def get_match(self, pattern):
        return self.get_match_sync(pattern)

    async def get_keys_count(self):
        return self.get_keys_count_sync()

    async def get_size(self):
        return self.get_size_sync()

    async def set_lock(self, key, ttl):
        return self.set_lock_sync(key, ttl)

    async def unlock(self, key):
        return self.unlock_sync(key)

    async def is_locked(self, key):
        return self.is_locked_sync(key)
