# -*- coding: utf-8 -*-
"""Helper functions and utilities."""
from nb_cache.middleware import PrefixMiddleware, MemoryLimitMiddleware


def add_prefix(prefix):
    """Create a PrefixMiddleware that adds prefix to all keys."""
    return PrefixMiddleware(prefix)


def memory_limit(max_keys=10000):
    """Create a MemoryLimitMiddleware."""
    return MemoryLimitMiddleware(max_keys=max_keys)


def invalidate_further():
    """Context manager / marker for cascading invalidation.

    When used, invalidation triggered inside this context will
    also invalidate dependent caches.
    """
    import contextlib

    @contextlib.contextmanager
    def _ctx():
        yield

    return _ctx()


def noself(func):
    """Decorator that strips 'self' from cache key generation.

    Useful for methods where caching should be shared across instances.
    """
    func._noself = True
    return func
