# -*- coding: utf-8 -*-
"""Bloom filter decorators.

- bloom: Check existence via a probabilistic bloom filter.
- dual_bloom: Two bloom filters for both positive and negative lookups.
"""
import functools
import hashlib
import math

from nb_cache._compat import is_coroutine_function
from nb_cache.key import get_cache_key, get_cache_key_template


def _optimal_params(capacity, false_positive_rate):
    """Calculate optimal bit array size and number of hash functions."""
    m = int(-capacity * math.log(false_positive_rate / 100.0) / (math.log(2) ** 2))
    k = max(1, int((m / capacity) * math.log(2)))
    return m, k


def _get_hash_indexes(key, m, k):
    """Generate k bit indexes from key using double hashing."""
    h1 = int(hashlib.md5(key.encode()).hexdigest(), 16)
    h2 = int(hashlib.sha1(key.encode()).hexdigest(), 16)
    return [(h1 + i * h2) % m for i in range(k)]


class BloomFilter(object):
    """Simple in-memory bloom filter backed by a cache backend."""

    def __init__(self, name, capacity, false_positive_rate, backend):
        self.name = name
        self.capacity = capacity
        self.m, self.k = _optimal_params(capacity, false_positive_rate)
        self._backend = backend
        self._bit_key = "__bloom__:{}".format(name)

    def _indexes(self, item):
        return _get_hash_indexes(str(item), self.m, self.k)

    def add_sync(self, item):
        be = self._backend
        for idx in self._indexes(item):
            key = "{}:{}".format(self._bit_key, idx)
            be.set_sync(key, b"1")

    async def add(self, item):
        be = self._backend
        for idx in self._indexes(item):
            key = "{}:{}".format(self._bit_key, idx)
            await be.set(key, b"1")

    def check_sync(self, item):
        be = self._backend
        for idx in self._indexes(item):
            key = "{}:{}".format(self._bit_key, idx)
            if be.get_sync(key) is None:
                return False
        return True

    async def check(self, item):
        be = self._backend
        for idx in self._indexes(item):
            key = "{}:{}".format(self._bit_key, idx)
            if await be.get(key) is None:
                return False
        return True


def bloom(capacity, name=None, false_positives=1, prefix="bloom", backend=None):
    """Bloom filter decorator.

    Wraps a function so that results are tracked in a bloom filter.
    Before calling, checks the bloom filter and skips execution if the
    item is definitely not present.

    Args:
        capacity: Expected number of elements.
        name: Bloom filter name (defaults to function name).
        false_positives: False positive rate percentage (1 = 1%).
        prefix: Key prefix.
        backend: Backend instance.
    """
    def decorator(func):
        _backend_ref = [backend]
        _name = name or "{}:{}".format(prefix, func.__qualname__)

        def _get_backend():
            from nb_cache.wrapper import _get_default_backend
            return _backend_ref[0] or _get_default_backend()

        _bf = [None]

        def _get_bf():
            if _bf[0] is None:
                _bf[0] = BloomFilter(_name, capacity, false_positives, _get_backend())
            return _bf[0]

        if is_coroutine_function(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                bf = _get_bf()
                _key_template = get_cache_key_template(func, None, prefix)
                cache_key = get_cache_key(func, _key_template, args, kwargs)
                if not await bf.check(cache_key):
                    return None
                result = await func(*args, **kwargs)
                await bf.add(cache_key)
                return result

            async_wrapper._bloom_filter = _get_bf
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                bf = _get_bf()
                _key_template = get_cache_key_template(func, None, prefix)
                cache_key = get_cache_key(func, _key_template, args, kwargs)
                if not bf.check_sync(cache_key):
                    return None
                result = func(*args, **kwargs)
                bf.add_sync(cache_key)
                return result

            sync_wrapper._bloom_filter = _get_bf
            return sync_wrapper

    return decorator


def dual_bloom(capacity, name=None, false=1, prefix="dual_bloom", backend=None):
    """Dual bloom filter: one for positive, one for negative results.

    Args:
        capacity: Expected number of elements.
        name: Filter name.
        false: False positive rate percentage.
        prefix: Key prefix.
        backend: Backend instance.
    """
    def decorator(func):
        _backend_ref = [backend]
        _name = name or "{}:{}".format(prefix, func.__qualname__)

        def _get_backend():
            from nb_cache.wrapper import _get_default_backend
            return _backend_ref[0] or _get_default_backend()

        _bf_pos = [None]
        _bf_neg = [None]

        def _get_filters():
            be = _get_backend()
            if _bf_pos[0] is None:
                _bf_pos[0] = BloomFilter(_name + ":pos", capacity, false, be)
                _bf_neg[0] = BloomFilter(_name + ":neg", capacity, false, be)
            return _bf_pos[0], _bf_neg[0]

        if is_coroutine_function(func):
            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                bf_pos, bf_neg = _get_filters()
                _key_template = get_cache_key_template(func, None, prefix)
                cache_key = get_cache_key(func, _key_template, args, kwargs)
                if await bf_neg.check(cache_key):
                    return None
                result = await func(*args, **kwargs)
                if result is not None:
                    await bf_pos.add(cache_key)
                else:
                    await bf_neg.add(cache_key)
                return result

            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                bf_pos, bf_neg = _get_filters()
                _key_template = get_cache_key_template(func, None, prefix)
                cache_key = get_cache_key(func, _key_template, args, kwargs)
                if bf_neg.check_sync(cache_key):
                    return None
                result = func(*args, **kwargs)
                if result is not None:
                    bf_pos.add_sync(cache_key)
                else:
                    bf_neg.add_sync(cache_key)
                return result

            return sync_wrapper

    return decorator
