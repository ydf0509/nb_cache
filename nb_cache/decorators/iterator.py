# -*- coding: utf-8 -*-
"""Iterator/generator cache decorator.

Caches the results of sync generators and async generators.
"""
import asyncio
import functools

from nb_cache._compat import is_coroutine_function
from nb_cache.condition import get_cache_condition
from nb_cache.key import get_cache_key, get_cache_key_template
from nb_cache.serialize import default_serializer, _SENTINEL
from nb_cache.ttl import ttl_to_seconds
import inspect


def iterator(ttl, key=None, condition=None, prefix="iter",
             backend=None, serializer=None):
    """Cache decorator for generators and async generators.

    Collects all yielded items, caches them as a list, and replays
    from cache on subsequent calls.

    Args:
        ttl: Time to live.
        key: Key template.
        condition: Cache condition.
        prefix: Key prefix.
        backend: Backend instance.
        serializer: Serializer instance.
    """
    _condition = get_cache_condition(condition)
    _serializer = serializer or default_serializer
    _ttl_seconds = ttl_to_seconds(ttl)

    def decorator(func):
        _key_template = get_cache_key_template(func, key, prefix)
        _backend_ref = [backend]

        def _get_backend():
            from nb_cache.wrapper import _get_default_backend
            return _backend_ref[0] or _get_default_backend()

        if inspect.isasyncgenfunction(func):
            @functools.wraps(func)
            async def async_gen_wrapper(*args, **kwargs):
                be = _get_backend()
                cache_key = get_cache_key(func, _key_template, args, kwargs)

                raw = await be.get(cache_key)
                if raw is not None:
                    val = _serializer.decode(raw)
                    if val is not _SENTINEL and isinstance(val, list):
                        for item in val:
                            yield item
                        return

                items = []
                async for item in func(*args, **kwargs):
                    items.append(item)
                    yield item

                if _condition(items):
                    encoded = _serializer.encode(items)
                    await be.set(cache_key, encoded, ttl=_ttl_seconds)

            async_gen_wrapper._cache_key_template = _key_template
            async_gen_wrapper._cache_backend_ref = _backend_ref
            return async_gen_wrapper

        elif inspect.isgeneratorfunction(func):
            @functools.wraps(func)
            def sync_gen_wrapper(*args, **kwargs):
                be = _get_backend()
                cache_key = get_cache_key(func, _key_template, args, kwargs)

                raw = be.get_sync(cache_key)
                if raw is not None:
                    val = _serializer.decode(raw)
                    if val is not _SENTINEL and isinstance(val, list):
                        for item in val:
                            yield item
                        return

                items = []
                for item in func(*args, **kwargs):
                    items.append(item)
                    yield item

                if _condition(items):
                    encoded = _serializer.encode(items)
                    be.set_sync(cache_key, encoded, ttl=_ttl_seconds)

            sync_gen_wrapper._cache_key_template = _key_template
            sync_gen_wrapper._cache_backend_ref = _backend_ref
            return sync_gen_wrapper

        elif is_coroutine_function(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                be = _get_backend()
                cache_key = get_cache_key(func, _key_template, args, kwargs)

                raw = await be.get(cache_key)
                if raw is not None:
                    val = _serializer.decode(raw)
                    if val is not _SENTINEL:
                        return val

                result = await func(*args, **kwargs)
                if _condition(result):
                    encoded = _serializer.encode(result)
                    await be.set(cache_key, encoded, ttl=_ttl_seconds)
                return result

            async_wrapper._cache_key_template = _key_template
            async_wrapper._cache_backend_ref = _backend_ref
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
                        return val

                result = func(*args, **kwargs)
                if _condition(result):
                    encoded = _serializer.encode(result)
                    be.set_sync(cache_key, encoded, ttl=_ttl_seconds)
                return result

            sync_wrapper._cache_key_template = _key_template
            sync_wrapper._cache_backend_ref = _backend_ref
            return sync_wrapper

    return decorator
