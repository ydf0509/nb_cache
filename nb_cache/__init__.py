# -*- coding: utf-8 -*-
"""nb_cache — 更强的缓存装饰器

支持同步和异步函数，支持加锁防止缓存击穿，
支持内存和Redis作为缓存器，支持Redis+内存双缓存提高性能。

Usage::

    from nb_cache import Cache

    cache = Cache()
    cache.setup("mem://")

    @cache.cache(ttl=60)
    def get_data(key):
        return expensive_query(key)

    @cache.cache(ttl="1h", lock=True)
    async def get_data_async(key):
        return await expensive_query_async(key)
"""

__version__ = "0.1.0"

from nb_cache.wrapper import Cache, register_backend
from nb_cache.condition import NOT_NONE, with_exceptions, only_exceptions
from nb_cache.exceptions import (
    CacheError,
    BackendNotInitializedError,
    CacheBackendInteractionError,
    LockError,
    LockedError,
    CircuitBreakerOpen,
    RateLimitError,
    SerializationError,
    TagError,
)
from nb_cache.transaction import TransactionMode
from nb_cache.key import get_cache_key_template
from nb_cache.helpers import noself, add_prefix, memory_limit, invalidate_further
from nb_cache.serialize import (
    Serializer,
    PickleSerializer,
    JsonSerializer,
    GzipCompressor,
    ZlibCompressor,
    HashSigner,
)
from nb_cache.ttl import ttl_to_seconds
from nb_cache.backends.base import BaseBackend
from nb_cache.backends.memory import MemoryBackend

mem = Cache()
mem.setup("mem://")

__all__ = [
    '__version__',
    # Main class
    'Cache',
    'mem',
    # Conditions
    'NOT_NONE',
    'with_exceptions',
    'only_exceptions',
    # Exceptions
    'CacheError',
    'BackendNotInitializedError',
    'CacheBackendInteractionError',
    'LockError',
    'LockedError',
    'CircuitBreakerOpen',
    'RateLimitError',
    'SerializationError',
    'TagError',
    # Transaction
    'TransactionMode',
    # Key
    'get_cache_key_template',
    # Helpers
    'noself',
    'add_prefix',
    'memory_limit',
    'invalidate_further',
    # Serialization
    'Serializer',
    'PickleSerializer',
    'JsonSerializer',
    'GzipCompressor',
    'ZlibCompressor',
    'HashSigner',
    # TTL
    'ttl_to_seconds',
    # Backends
    'BaseBackend',
    'MemoryBackend',
    'register_backend',
]
