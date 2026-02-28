# -*- coding: utf-8 -*-
"""Python version compatibility utilities for Python 3.6+."""
import sys
import asyncio
import inspect

PY36 = sys.version_info[:2] == (3, 6)
PY37_PLUS = sys.version_info >= (3, 7)
PY38_PLUS = sys.version_info >= (3, 8)
PY310_PLUS = sys.version_info >= (3, 10)


def get_event_loop():
    if PY310_PLUS:
        try:
            return asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.new_event_loop()
    else:
        return asyncio.get_event_loop()


def is_coroutine_function(func):
    if hasattr(func, '__wrapped__'):
        return asyncio.iscoroutinefunction(func.__wrapped__)
    return asyncio.iscoroutinefunction(func)


def create_task(coro):
    loop = get_event_loop()
    if hasattr(loop, 'create_task'):
        return loop.create_task(coro)
    return asyncio.ensure_future(coro, loop=loop)


def run_sync(coro):
    """Run a coroutine synchronously. Works across Python versions."""
    if PY37_PLUS:
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(asyncio.run, coro)
                    return future.result()
            else:
                return loop.run_until_complete(coro)
        except RuntimeError:
            return asyncio.run(coro)
    else:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import concurrent.futures
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                new_loop = asyncio.new_event_loop()
                future = pool.submit(new_loop.run_until_complete, coro)
                return future.result()
        return loop.run_until_complete(coro)
