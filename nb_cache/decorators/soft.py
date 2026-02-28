# -*- coding: utf-8 -*-
"""Soft-expiration cache decorator.

Values have a soft TTL and a hard TTL. After soft TTL, the value is refreshed
on the next access but the stale value is returned. After hard TTL, the value
is truly gone and must be recomputed.
"""
import asyncio
import functools
import threading
import time

from nb_cache._compat import is_coroutine_function, create_task
from nb_cache.condition import get_cache_condition
from nb_cache.key import get_cache_key, get_cache_key_template
from nb_cache.serialize import default_serializer, _SENTINEL
from nb_cache.ttl import ttl_to_seconds

_SOFT_META_PREFIX = "__soft_ts__:"


def soft(ttl, key=None, soft_ttl=None, condition=None, prefix="soft",
         tags=(), backend=None, serializer=None):
    """Cache with soft expiration.

    Args:
        ttl: Hard TTL - maximum time the value lives.
        soft_ttl: Soft TTL - after this, value is refreshed in background.
            Defaults to ttl / 2.
        condition: Cache condition callable.
    """
    _condition = get_cache_condition(condition)
    _serializer = serializer or default_serializer
    _ttl_seconds = ttl_to_seconds(ttl)
    _soft_ttl = ttl_to_seconds(soft_ttl) if soft_ttl else (_ttl_seconds / 2 if _ttl_seconds else None)

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
                meta_key = _SOFT_META_PREFIX + cache_key

                raw = await be.get(cache_key)
                if raw is not None:
                    val = _serializer.decode(raw)
                    if val is not _SENTINEL:
                        ts_raw = await be.get(meta_key)
                        if ts_raw is not None:
                            try:
                                set_time = float(ts_raw)
                            except (ValueError, TypeError):
                                set_time = 0
                            elapsed = time.time() - set_time
                            if _soft_ttl and elapsed > _soft_ttl and cache_key not in _refreshing:
                                _refreshing.add(cache_key)

                                async def _refresh():
                                    try:
                                        result = await func(*args, **kwargs)
                                        if _condition(result):
                                            encoded = _serializer.encode(result)
                                            await be.set(cache_key, encoded, ttl=_ttl_seconds)
                                            await be.set(meta_key, str(time.time()).encode(), ttl=_ttl_seconds)
                                    finally:
                                        _refreshing.discard(cache_key)

                                create_task(_refresh())
                        return val

                result = await func(*args, **kwargs)
                if _condition(result):
                    encoded = _serializer.encode(result)
                    await be.set(cache_key, encoded, ttl=_ttl_seconds)
                    await be.set(meta_key, str(time.time()).encode(), ttl=_ttl_seconds)
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
                meta_key = _SOFT_META_PREFIX + cache_key

                raw = be.get_sync(cache_key)
                if raw is not None:
                    val = _serializer.decode(raw)
                    if val is not _SENTINEL:
                        ts_raw = be.get_sync(meta_key)
                        if ts_raw is not None:
                            try:
                                set_time = float(ts_raw)
                            except (ValueError, TypeError):
                                set_time = 0
                            elapsed = time.time() - set_time
                            if _soft_ttl and elapsed > _soft_ttl and cache_key not in _refreshing:
                                _refreshing.add(cache_key)

                                def _refresh():
                                    try:
                                        result = func(*args, **kwargs)
                                        if _condition(result):
                                            encoded = _serializer.encode(result)
                                            be.set_sync(cache_key, encoded, ttl=_ttl_seconds)
                                            be.set_sync(meta_key, str(time.time()).encode(), ttl=_ttl_seconds)
                                    finally:
                                        _refreshing.discard(cache_key)

                                t = threading.Thread(target=_refresh, daemon=True)
                                t.start()
                        return val

                result = func(*args, **kwargs)
                if _condition(result):
                    encoded = _serializer.encode(result)
                    be.set_sync(cache_key, encoded, ttl=_ttl_seconds)
                    be.set_sync(meta_key, str(time.time()).encode(), ttl=_ttl_seconds)
                return result

            sync_wrapper._cache_key_template = _key_template
            sync_wrapper._cache_backend_ref = _backend_ref
            sync_wrapper._cache_tags = tags
            return sync_wrapper

    return decorator
