# -*- coding: utf-8 -*-
"""Failover cache decorator.

On exception, returns the cached value if available.
"""
import functools

from nb_cache._compat import is_coroutine_function
from nb_cache.condition import get_cache_condition
from nb_cache.key import get_cache_key, get_cache_key_template
from nb_cache.serialize import default_serializer, _SENTINEL
from nb_cache.ttl import ttl_to_seconds


def failover(ttl, key=None, exceptions=None, condition=None, prefix="fail",
             tags=(), backend=None, serializer=None):
    """Cache with failover: on exception, return stale cached value.

    Args:
        ttl: Time to live for cached values.
        exceptions: Tuple of exception types to catch. Defaults to (Exception,).
        condition: Cache condition callable.
    """
    _condition = get_cache_condition(condition)
    _serializer = serializer or default_serializer
    _ttl_seconds = ttl_to_seconds(ttl)
    _exceptions = exceptions or (Exception,)

    def decorator(func):
        _key_template = get_cache_key_template(func, key, prefix)
        _backend_ref = [backend]

        def _get_backend():
            from nb_cache.wrapper import _get_default_backend
            return _backend_ref[0] or _get_default_backend()

        if is_coroutine_function(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                be = _get_backend()
                cache_key = get_cache_key(func, _key_template, args, kwargs)

                try:
                    result = await func(*args, **kwargs)
                    if _condition(result):
                        encoded = _serializer.encode(result)
                        await be.set(cache_key, encoded, ttl=_ttl_seconds)
                    return result
                except _exceptions:
                    raw = await be.get(cache_key)
                    if raw is not None:
                        val = _serializer.decode(raw)
                        if val is not _SENTINEL:
                            return val
                    raise

            async_wrapper._cache_key_template = _key_template
            async_wrapper._cache_backend_ref = _backend_ref
            async_wrapper._cache_tags = tags
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                be = _get_backend()
                cache_key = get_cache_key(func, _key_template, args, kwargs)

                try:
                    result = func(*args, **kwargs)
                    if _condition(result):
                        encoded = _serializer.encode(result)
                        be.set_sync(cache_key, encoded, ttl=_ttl_seconds)
                    return result
                except _exceptions:
                    raw = be.get_sync(cache_key)
                    if raw is not None:
                        val = _serializer.decode(raw)
                        if val is not _SENTINEL:
                            return val
                    raise

            sync_wrapper._cache_key_template = _key_template
            sync_wrapper._cache_backend_ref = _backend_ref
            sync_wrapper._cache_tags = tags
            return sync_wrapper

    return decorator
