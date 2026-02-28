# -*- coding: utf-8 -*-
"""Middleware system for cache operations.

Middlewares wrap backend operations (get, set, delete, etc.)
and can modify keys, values, or behavior.
"""


class Middleware(object):
    """Base middleware class. Subclass and override methods."""

    def on_get(self, key, backend):
        """Called before get. Return modified key or None to skip."""
        return key

    def on_get_result(self, key, result, backend):
        """Called after get. Return modified result."""
        return result

    def on_set(self, key, value, ttl, backend):
        """Called before set. Return (key, value, ttl) or None to skip."""
        return key, value, ttl

    def on_delete(self, key, backend):
        """Called before delete. Return key or None to skip."""
        return key


class PrefixMiddleware(Middleware):
    """Adds a prefix to all cache keys."""

    def __init__(self, prefix):
        self._prefix = prefix

    def _add_prefix(self, key):
        return "{}:{}".format(self._prefix, key)

    def on_get(self, key, backend):
        return self._add_prefix(key)

    def on_set(self, key, value, ttl, backend):
        return self._add_prefix(key), value, ttl

    def on_delete(self, key, backend):
        return self._add_prefix(key)


class MemoryLimitMiddleware(Middleware):
    """Limits total memory usage of the cache.

    Only works with memory backends that support get_keys_count_sync.
    """

    def __init__(self, max_keys=10000):
        self._max_keys = max_keys

    def on_set(self, key, value, ttl, backend):
        try:
            count = backend.get_keys_count_sync()
            if count >= self._max_keys:
                return None
        except (NotImplementedError, AttributeError):
            pass
        return key, value, ttl
