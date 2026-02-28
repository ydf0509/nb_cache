# -*- coding: utf-8 -*-
"""Early refresh cache decorator.

Refreshes the cache before expiry to prevent cache stampede.
When the remaining TTL drops below `early_ttl`, the next request triggers
a background refresh while returning the stale value.
"""
import asyncio
import functools
import threading

from nb_cache._compat import is_coroutine_function, create_task
from nb_cache.condition import get_cache_condition
from nb_cache.key import get_cache_key, get_cache_key_template
from nb_cache.serialize import default_serializer, _SENTINEL
from nb_cache.ttl import ttl_to_seconds


def early(ttl, key=None, early_ttl=None, condition=None, prefix="early",
          tags=(), backend=None, serializer=None):
    """Cache with early background refresh.

    Args:
        ttl: Total time to live.
        early_ttl: When remaining TTL falls below this, trigger refresh.
            Defaults to ttl / 4.
        condition: Cache condition callable.
        prefix: Key prefix.
        tags: Tags for invalidation.
        backend: Backend instance.
        serializer: Serializer instance.
    """
    _condition = get_cache_condition(condition)
    _serializer = serializer or default_serializer
    _ttl_seconds = ttl_to_seconds(ttl)
    _early_ttl = ttl_to_seconds(early_ttl) if early_ttl else (_ttl_seconds / 4 if _ttl_seconds else None)

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

                raw = await be.get(cache_key)
                if raw is not None:
                    val = _serializer.decode(raw)
                    if val is not _SENTINEL:
                        remaining = await be.get_expire(cache_key)
                        if remaining is not None and _early_ttl and 0 < remaining < _early_ttl:
                            if cache_key not in _refreshing:
                                _refreshing.add(cache_key)

                                async def _refresh():
                                    try:
                                        result = await func(*args, **kwargs)
                                        if _condition(result):
                                            encoded = _serializer.encode(result)
                                            await be.set(cache_key, encoded, ttl=_ttl_seconds)
                                    finally:
                                        _refreshing.discard(cache_key)

                                create_task(_refresh())
                        return val

                result = await func(*args, **kwargs)
                if _condition(result):
                    encoded = _serializer.encode(result)
                    await be.set(cache_key, encoded, ttl=_ttl_seconds)
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

                raw = be.get_sync(cache_key)
                if raw is not None:
                    val = _serializer.decode(raw)
                    if val is not _SENTINEL:
                        remaining = be.get_expire_sync(cache_key)
                        if remaining is not None and _early_ttl and 0 < remaining < _early_ttl:
                            if cache_key not in _refreshing:
                                _refreshing.add(cache_key)

                                def _refresh():
                                    try:
                                        result = func(*args, **kwargs)
                                        if _condition(result):
                                            encoded = _serializer.encode(result)
                                            be.set_sync(cache_key, encoded, ttl=_ttl_seconds)
                                    finally:
                                        _refreshing.discard(cache_key)

                                t = threading.Thread(target=_refresh, daemon=True)
                                t.start()
                        return val

                result = func(*args, **kwargs)
                if _condition(result):
                    encoded = _serializer.encode(result)
                    be.set_sync(cache_key, encoded, ttl=_ttl_seconds)
                return result

            sync_wrapper._cache_key_template = _key_template
            sync_wrapper._cache_backend_ref = _backend_ref
            sync_wrapper._cache_tags = tags
            return sync_wrapper

    return decorator
