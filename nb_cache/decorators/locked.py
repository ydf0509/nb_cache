# -*- coding: utf-8 -*-
"""Lock decorator and thunder protection.

- locked: Ensures only one caller executes the function at a time.
- thunder_protection: Deduplicates concurrent calls with same key.
"""
import asyncio
import functools
import time

from nb_cache._compat import is_coroutine_function
from nb_cache.exceptions import LockedError
from nb_cache.key import get_cache_key, get_cache_key_template
from nb_cache.ttl import ttl_to_seconds


def locked(ttl=None, key=None, wait=True, prefix="locked",
           check_interval=0.1, backend=None):
    """Lock decorator to prevent concurrent execution.

    Args:
        ttl: Lock TTL. Defaults to 60 seconds.
        wait: If True, wait for lock. If False, raise LockedError.
        check_interval: Seconds between lock checks when waiting.
        backend: Backend instance.
    """
    _ttl_seconds = ttl_to_seconds(ttl) or 60
    _check_interval = check_interval or 0.1

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
                lock_key = "__dlock__:{}".format(cache_key)

                acquired = await be.set_lock(lock_key, _ttl_seconds)
                if not acquired:
                    if not wait:
                        raise LockedError("Resource is locked: {}".format(lock_key))
                    deadline = time.time() + _ttl_seconds
                    while time.time() < deadline:
                        await asyncio.sleep(_check_interval)
                        acquired = await be.set_lock(lock_key, _ttl_seconds)
                        if acquired:
                            break
                    if not acquired:
                        raise LockedError("Timeout waiting for lock: {}".format(lock_key))

                try:
                    return await func(*args, **kwargs)
                finally:
                    await be.unlock(lock_key)

            async_wrapper._cache_key_template = _key_template
            async_wrapper._cache_backend_ref = _backend_ref
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                be = _get_backend()
                cache_key = get_cache_key(func, _key_template, args, kwargs)
                lock_key = "__dlock__:{}".format(cache_key)

                acquired = be.set_lock_sync(lock_key, _ttl_seconds)
                if not acquired:
                    if not wait:
                        raise LockedError("Resource is locked: {}".format(lock_key))
                    deadline = time.time() + _ttl_seconds
                    while time.time() < deadline:
                        time.sleep(_check_interval)
                        acquired = be.set_lock_sync(lock_key, _ttl_seconds)
                        if acquired:
                            break
                    if not acquired:
                        raise LockedError("Timeout waiting for lock: {}".format(lock_key))

                try:
                    return func(*args, **kwargs)
                finally:
                    be.unlock_sync(lock_key)

            sync_wrapper._cache_key_template = _key_template
            sync_wrapper._cache_backend_ref = _backend_ref
            return sync_wrapper

    return decorator


def thunder_protection(ttl=None, key=None, prefix="thunder", backend=None):
    """Deduplicate concurrent calls with same arguments.

    The first caller executes the function; concurrent callers with the same
    key wait for the result instead of executing again.
    """
    _ttl_seconds = ttl_to_seconds(ttl) or 60

    def decorator(func):
        _key_template = get_cache_key_template(func, key, prefix)
        _backend_ref = [backend]
        _pending_async = {}
        _pending_sync = {}
        import threading
        _sync_lock = threading.Lock()

        def _get_backend():
            from nb_cache.wrapper import _get_default_backend
            return _backend_ref[0] or _get_default_backend()

        if is_coroutine_function(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                cache_key = get_cache_key(func, _key_template, args, kwargs)

                if cache_key in _pending_async:
                    return await _pending_async[cache_key]

                future = asyncio.get_event_loop().create_future()
                _pending_async[cache_key] = future
                try:
                    result = await func(*args, **kwargs)
                    future.set_result(result)
                    return result
                except Exception as e:
                    future.set_exception(e)
                    raise
                finally:
                    _pending_async.pop(cache_key, None)

            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                cache_key = get_cache_key(func, _key_template, args, kwargs)

                with _sync_lock:
                    if cache_key in _pending_sync:
                        event, result_holder = _pending_sync[cache_key]
                        waiting = True
                    else:
                        waiting = False

                if waiting:
                    event.wait(timeout=_ttl_seconds)
                    if result_holder.get('error'):
                        raise result_holder['error']
                    return result_holder.get('result')

                event = threading.Event()
                result_holder = {}
                with _sync_lock:
                    _pending_sync[cache_key] = (event, result_holder)

                try:
                    result = func(*args, **kwargs)
                    result_holder['result'] = result
                    return result
                except Exception as e:
                    result_holder['error'] = e
                    raise
                finally:
                    event.set()
                    with _sync_lock:
                        _pending_sync.pop(cache_key, None)

            return sync_wrapper

    return decorator
