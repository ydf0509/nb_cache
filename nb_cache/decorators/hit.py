# -*- coding: utf-8 -*-
"""Hit-based cache decorator.

Drops the cached value after N hits, forcing a refresh.
Optionally refreshes in background after `update_after` hits.
"""
import asyncio
import functools
import threading

from nb_cache._compat import is_coroutine_function, create_task
from nb_cache.condition import get_cache_condition
from nb_cache.key import get_cache_key, get_cache_key_template
from nb_cache.serialize import default_serializer, _SENTINEL
from nb_cache.ttl import ttl_to_seconds

_HIT_COUNT_PREFIX = "__hit_cnt__:"


def hit(ttl, cache_hits, update_after=0, key=None, condition=None,
        prefix="hit", tags=(), backend=None, serializer=None):
    """Cache that invalidates after a number of hits.

    Args:
        ttl: Time to live.
        cache_hits: Number of hits before eviction.
        update_after: Trigger background update after this many hits (0=disabled).
        condition: Cache condition callable.
    """
    _condition = get_cache_condition(condition)
    _serializer = serializer or default_serializer
    _ttl_seconds = ttl_to_seconds(ttl)

    def decorator(func):
        _key_template = get_cache_key_template(func, key, prefix)
        _backend_ref = [backend]
        _refreshing = set()

        def _get_backend():
            from nb_cache.wrapper import _get_default_backend
            return _backend_ref[0] or _get_default_backend()

        if is_coroutine_function(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                be = _get_backend()
                cache_key = get_cache_key(func, _key_template, args, kwargs)
                hit_key = _HIT_COUNT_PREFIX + cache_key

                raw = await be.get(cache_key)
                if raw is not None:
                    val = _serializer.decode(raw)
                    if val is not _SENTINEL:
                        count = await be.incr(hit_key)
                        if count >= cache_hits:
                            await be.delete(cache_key)
                            await be.delete(hit_key)
                        elif update_after and count >= update_after and cache_key not in _refreshing:
                            _refreshing.add(cache_key)

                            async def _refresh():
                                try:
                                    result = await func(*args, **kwargs)
                                    if _condition(result):
                                        encoded = _serializer.encode(result)
                                        await be.set(cache_key, encoded, ttl=_ttl_seconds)
                                        await be.delete(hit_key)
                                finally:
                                    _refreshing.discard(cache_key)

                            create_task(_refresh())
                        return val

                result = await func(*args, **kwargs)
                if _condition(result):
                    encoded = _serializer.encode(result)
                    await be.set(cache_key, encoded, ttl=_ttl_seconds)
                    await be.delete(hit_key)
                return result

            async_wrapper._cache_key_template = _key_template
            async_wrapper._cache_backend_ref = _backend_ref
            async_wrapper._cache_tags = tags
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                be = _get_backend()
                cache_key = get_cache_key(func, _key_template, args, kwargs)
                hit_key = _HIT_COUNT_PREFIX + cache_key

                raw = be.get_sync(cache_key)
                if raw is not None:
                    val = _serializer.decode(raw)
                    if val is not _SENTINEL:
                        count = be.incr_sync(hit_key)
                        if count >= cache_hits:
                            be.delete_sync(cache_key)
                            be.delete_sync(hit_key)
                        elif update_after and count >= update_after and cache_key not in _refreshing:
                            _refreshing.add(cache_key)

                            def _refresh():
                                try:
                                    result = func(*args, **kwargs)
                                    if _condition(result):
                                        encoded = _serializer.encode(result)
                                        be.set_sync(cache_key, encoded, ttl=_ttl_seconds)
                                        be.delete_sync(hit_key)
                                finally:
                                    _refreshing.discard(cache_key)

                            t = threading.Thread(target=_refresh, daemon=True)
                            t.start()
                        return val

                result = func(*args, **kwargs)
                if _condition(result):
                    encoded = _serializer.encode(result)
                    be.set_sync(cache_key, encoded, ttl=_ttl_seconds)
                    be.delete_sync(hit_key)
                return result

            sync_wrapper._cache_key_template = _key_template
            sync_wrapper._cache_backend_ref = _backend_ref
            sync_wrapper._cache_tags = tags
            return sync_wrapper

    return decorator


def dynamic(ttl=86400, key=None, condition=None, prefix="dynamic",
            tags=(), backend=None, serializer=None):
    """Alias for hit with cache_hits=3, update_after=1."""
    return hit(ttl, cache_hits=3, update_after=1, key=key, condition=condition,
               prefix=prefix, tags=tags, backend=backend, serializer=serializer)
