# -*- coding: utf-8 -*-
"""Circuit breaker decorator.

Tracks error rate and opens the circuit when it exceeds a threshold,
preventing further calls for a cooldown period.

States: CLOSED -> OPEN -> HALF_OPEN -> CLOSED
"""
import functools
import time

from nb_cache._compat import is_coroutine_function
from nb_cache.exceptions import CircuitBreakerOpen
from nb_cache.key import get_cache_key, get_cache_key_template
from nb_cache.ttl import ttl_to_seconds


def circuit_breaker(errors_rate, period, ttl, half_open_ttl=None,
                    exceptions=None, key=None, min_calls=1,
                    prefix="circuit_breaker", backend=None):
    """Circuit breaker decorator.

    Args:
        errors_rate: Error rate threshold (0.0 to 1.0) to trip the breaker.
        period: Time window (seconds) to calculate error rate.
        ttl: How long the circuit stays OPEN.
        half_open_ttl: TTL for half-open state. Defaults to ttl/2.
        exceptions: Exception types to track. Defaults to (Exception,).
        min_calls: Minimum calls before error rate is evaluated.
        prefix: Key prefix.
        backend: Backend instance.
    """
    _period = ttl_to_seconds(period) or 60
    _ttl_seconds = ttl_to_seconds(ttl) or 60
    _half_open_ttl = ttl_to_seconds(half_open_ttl) if half_open_ttl else (_ttl_seconds / 2)
    _exceptions = tuple(exceptions) if exceptions else (Exception,)

    def decorator(func):
        _key_template = get_cache_key_template(func, key, prefix)
        _backend_ref = [backend]

        _STATE_KEY = "__cb_state__:"
        _CALLS_KEY = "__cb_calls__:"
        _ERRORS_KEY = "__cb_errors__:"
        _TRIP_KEY = "__cb_trip__:"

        def _get_backend():
            from nb_cache.wrapper import _get_default_backend
            return _backend_ref[0] or _get_default_backend()

        if is_coroutine_function(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                be = _get_backend()
                base_key = get_cache_key(func, _key_template, args, kwargs)
                state_key = _STATE_KEY + base_key
                calls_key = _CALLS_KEY + base_key
                errors_key = _ERRORS_KEY + base_key
                trip_key = _TRIP_KEY + base_key

                state = await be.get(state_key)
                if state == b"open":
                    raise CircuitBreakerOpen(
                        "Circuit breaker is open for: {}".format(base_key))
                if state == b"half_open":
                    pass

                try:
                    result = await func(*args, **kwargs)
                except _exceptions:
                    await be.incr(errors_key)
                    await be.expire(errors_key, int(_period))
                    await be.incr(calls_key)
                    await be.expire(calls_key, int(_period))
                    await _check_and_trip(be, state_key, calls_key, errors_key, trip_key)
                    raise
                else:
                    await be.incr(calls_key)
                    await be.expire(calls_key, int(_period))
                    if state == b"half_open":
                        await be.delete(state_key)
                        await be.delete(calls_key)
                        await be.delete(errors_key)
                    return result

            async def _check_and_trip(be, state_key, calls_key, errors_key, trip_key):
                calls_raw = await be.get(calls_key)
                errors_raw = await be.get(errors_key)
                total = int(calls_raw) if calls_raw else 0
                errors = int(errors_raw) if errors_raw else 0
                if total >= min_calls and errors / max(total, 1) >= errors_rate:
                    await be.set(state_key, b"open", ttl=_ttl_seconds)
                    await _schedule_half_open(be, state_key)

            async def _schedule_half_open(be, state_key):
                import asyncio
                await asyncio.sleep(0)
                await be.set(state_key, b"open", ttl=_ttl_seconds)

            async_wrapper._cache_key_template = _key_template
            async_wrapper._cache_backend_ref = _backend_ref
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                be = _get_backend()
                base_key = get_cache_key(func, _key_template, args, kwargs)
                state_key = _STATE_KEY + base_key
                calls_key = _CALLS_KEY + base_key
                errors_key = _ERRORS_KEY + base_key
                trip_key = _TRIP_KEY + base_key

                state = be.get_sync(state_key)
                if state == b"open":
                    raise CircuitBreakerOpen(
                        "Circuit breaker is open for: {}".format(base_key))

                try:
                    result = func(*args, **kwargs)
                except _exceptions:
                    be.incr_sync(errors_key)
                    be.expire_sync(errors_key, int(_period))
                    be.incr_sync(calls_key)
                    be.expire_sync(calls_key, int(_period))
                    _check_and_trip_sync(be, state_key, calls_key, errors_key)
                    raise
                else:
                    be.incr_sync(calls_key)
                    be.expire_sync(calls_key, int(_period))
                    if state == b"half_open":
                        be.delete_sync(state_key)
                        be.delete_sync(calls_key)
                        be.delete_sync(errors_key)
                    return result

            def _check_and_trip_sync(be, state_key, calls_key, errors_key):
                calls_raw = be.get_sync(calls_key)
                errors_raw = be.get_sync(errors_key)
                total = int(calls_raw) if calls_raw else 0
                errors = int(errors_raw) if errors_raw else 0
                if total >= min_calls and errors / max(total, 1) >= errors_rate:
                    be.set_sync(state_key, b"open", ttl=_ttl_seconds)

            sync_wrapper._cache_key_template = _key_template
            sync_wrapper._cache_backend_ref = _backend_ref
            return sync_wrapper

    return decorator
