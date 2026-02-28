# -*- coding: utf-8 -*-
"""Rate limiting decorators.

- rate_limit: Fixed-window rate limiting.
- slice_rate_limit: Sliding-window rate limiting.
"""
import functools
import time

from nb_cache._compat import is_coroutine_function
from nb_cache.exceptions import RateLimitError
from nb_cache.key import get_cache_key, get_cache_key_template
from nb_cache.ttl import ttl_to_seconds


def rate_limit(limit, period, ttl=None, action=None, prefix="rate_limit",
               key=None, backend=None):
    """Fixed-window rate limiter.

    Args:
        limit: Maximum number of calls allowed in the period.
        period: Time window in seconds.
        ttl: TTL for the counter key. Defaults to period.
        action: Callable to invoke when rate is exceeded (instead of raising).
        prefix: Key prefix.
        key: Key template.
        backend: Backend instance.
    """
    _period = ttl_to_seconds(period) or 60
    _ttl_seconds = ttl_to_seconds(ttl) if ttl else _period

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
                rate_key = "__rl__:{}".format(cache_key)

                count = await be.incr(rate_key)
                if count == 1:
                    await be.expire(rate_key, int(_period))

                if count > limit:
                    if action:
                        return action(*args, **kwargs)
                    raise RateLimitError(
                        "Rate limit exceeded: {} calls in {}s".format(limit, _period))

                return await func(*args, **kwargs)

            async_wrapper._cache_key_template = _key_template
            async_wrapper._cache_backend_ref = _backend_ref
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                be = _get_backend()
                cache_key = get_cache_key(func, _key_template, args, kwargs)
                rate_key = "__rl__:{}".format(cache_key)

                count = be.incr_sync(rate_key)
                if count == 1:
                    be.expire_sync(rate_key, int(_period))

                if count > limit:
                    if action:
                        return action(*args, **kwargs)
                    raise RateLimitError(
                        "Rate limit exceeded: {} calls in {}s".format(limit, _period))

                return func(*args, **kwargs)

            sync_wrapper._cache_key_template = _key_template
            sync_wrapper._cache_backend_ref = _backend_ref
            return sync_wrapper

    return decorator


def slice_rate_limit(limit, period, key=None, action=None,
                     prefix="srl", backend=None):
    """Sliding-window rate limiter using multiple time slices.

    Divides the period into slices and tracks counts per slice for
    a smoother rate limiting behavior.

    Args:
        limit: Maximum number of calls allowed in the period.
        period: Time window in seconds.
        key: Key template.
        action: Callable to invoke when rate is exceeded.
        prefix: Key prefix.
        backend: Backend instance.
    """
    _period = ttl_to_seconds(period) or 60
    _num_slices = 10
    _slice_duration = _period / _num_slices

    def decorator(func):
        _key_template = get_cache_key_template(func, key, prefix)
        _backend_ref = [backend]

        def _get_backend():
            from nb_cache.wrapper import _get_default_backend
            return _backend_ref[0] or _get_default_backend()

        def _current_slice_key(cache_key):
            now = time.time()
            slice_index = int(now / _slice_duration) % _num_slices
            return "__srl__:{}:{}".format(cache_key, slice_index)

        def _all_slice_keys(cache_key):
            now = time.time()
            current_idx = int(now / _slice_duration)
            keys = []
            for i in range(_num_slices):
                idx = (current_idx - i) % _num_slices
                keys.append("__srl__:{}:{}".format(cache_key, idx))
            return keys

        if is_coroutine_function(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                be = _get_backend()
                cache_key = get_cache_key(func, _key_template, args, kwargs)

                slice_keys = _all_slice_keys(cache_key)
                values = await be.get_many(*slice_keys)
                total = sum(int(v) for v in values if v is not None)

                if total >= limit:
                    if action:
                        return action(*args, **kwargs)
                    raise RateLimitError(
                        "Sliding rate limit exceeded: {} calls in {}s".format(limit, _period))

                current_key = _current_slice_key(cache_key)
                await be.incr(current_key)
                await be.expire(current_key, int(_period))

                return await func(*args, **kwargs)

            async_wrapper._cache_key_template = _key_template
            async_wrapper._cache_backend_ref = _backend_ref
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                be = _get_backend()
                cache_key = get_cache_key(func, _key_template, args, kwargs)

                slice_keys = _all_slice_keys(cache_key)
                values = be.get_many_sync(*slice_keys)
                total = sum(int(v) for v in values if v is not None)

                if total >= limit:
                    if action:
                        return action(*args, **kwargs)
                    raise RateLimitError(
                        "Sliding rate limit exceeded: {} calls in {}s".format(limit, _period))

                current_key = _current_slice_key(cache_key)
                be.incr_sync(current_key)
                be.expire_sync(current_key, int(_period))

                return func(*args, **kwargs)

            sync_wrapper._cache_key_template = _key_template
            sync_wrapper._cache_backend_ref = _backend_ref
            return sync_wrapper

    return decorator
