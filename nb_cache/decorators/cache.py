# -*- coding: utf-8 -*-
"""Basic cache decorator with sync/async support and optional locking."""
import asyncio
import functools
import logging

from nb_cache._compat import is_coroutine_function
from nb_cache.condition import get_cache_condition
from nb_cache.key import get_cache_key, get_cache_key_template, get_func_name
from nb_cache.serialize import default_serializer, _SENTINEL
from nb_cache.ttl import ttl_to_seconds

logger = logging.getLogger("nb_cache.cache")


def _final_key(be, cache_key):
    """通过 backend 的 _make_key 方法获取最终写入存储的完整 key。"""
    if hasattr(be, '_make_key'):
        return be._make_key(cache_key)
    return cache_key


def cache(ttl, key=None, condition=None, prefix="", lock=False,
          lock_ttl=None, tags=(), backend=None, serializer=None,
          tag_registry=None, key_include_func=True):
    """Basic cache decorator.

    Supports both sync and async functions transparently.

    Args:
        ttl: Time to live (seconds, timedelta, or string like "1h").
        key: Key template string. If None, auto-generated from function signature.
        condition: Callable(result) -> bool, determines if result should be cached.
        prefix: Key prefix string.
        lock: If True, use locking to prevent cache stampede.
        lock_ttl: Lock TTL (defaults to ttl if not set).
        tags: Tuple of tag strings for tag-based invalidation.
        backend: Cache backend instance. If None, uses the global default.
        serializer: Serializer instance. If None, uses default.
        tag_registry: TagRegistry instance for tag-based invalidation.
        key_include_func: If False, module path and function name are excluded
            from the generated key. Default True.
    """
    _condition = get_cache_condition(condition)
    _serializer = serializer or default_serializer
    _ttl_seconds = ttl_to_seconds(ttl)
    _lock_ttl = ttl_to_seconds(lock_ttl) if lock_ttl else _ttl_seconds

    def decorator(func):
        _key_template = get_cache_key_template(func, key, prefix, key_include_func=key_include_func)
        _backend_ref = [backend]
        _registry = tag_registry

        def _get_backend():
            from nb_cache.wrapper import _get_default_backend
            return _backend_ref[0] or _get_default_backend()

        if is_coroutine_function(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                be = _get_backend()
                cache_key = get_cache_key(func, _key_template, args, kwargs)
                logger.debug("[nb_cache] func=%s  final_key=%s  ttl=%s",
                             get_func_name(func), _final_key(be, cache_key), _ttl_seconds)

                raw = await be.get(cache_key)
                if raw is not None:
                    val = _serializer.decode(raw)
                    if val is not _SENTINEL:
                        return val

                if lock:
                    acquired = await be.set_lock(cache_key, _lock_ttl)
                    if not acquired:
                        for _ in range(50):
                            await asyncio.sleep(0.1)
                            raw = await be.get(cache_key)
                            if raw is not None:
                                val = _serializer.decode(raw)
                                if val is not _SENTINEL:
                                    return val
                        return await func(*args, **kwargs)

                try:
                    result = await func(*args, **kwargs)
                finally:
                    if lock:
                        await be.unlock(cache_key)

                if _condition(result):
                    encoded = _serializer.encode(result)
                    await be.set(cache_key, encoded, ttl=_ttl_seconds)
                    if tags and _registry is not None:
                        _registry.register(cache_key, tags)

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
                logger.debug("[nb_cache] func=%s  final_key=%s  ttl=%s",
                             get_func_name(func), _final_key(be, cache_key), _ttl_seconds)

                raw = be.get_sync(cache_key)
                if raw is not None:
                    val = _serializer.decode(raw)
                    if val is not _SENTINEL:
                        return val

                if lock:
                    acquired = be.set_lock_sync(cache_key, _lock_ttl)
                    if not acquired:
                        import time as _time
                        for _ in range(50):
                            _time.sleep(0.1)
                            raw = be.get_sync(cache_key)
                            if raw is not None:
                                val = _serializer.decode(raw)
                                if val is not _SENTINEL:
                                    return val
                        return func(*args, **kwargs)

                try:
                    result = func(*args, **kwargs)
                finally:
                    if lock:
                        be.unlock_sync(cache_key)

                if _condition(result):
                    encoded = _serializer.encode(result)
                    be.set_sync(cache_key, encoded, ttl=_ttl_seconds)
                    if tags and _registry is not None:
                        _registry.register(cache_key, tags)

                return result

            sync_wrapper._cache_key_template = _key_template
            sync_wrapper._cache_backend_ref = _backend_ref
            sync_wrapper._cache_tags = tags
            return sync_wrapper

    return decorator
