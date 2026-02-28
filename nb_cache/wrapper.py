# -*- coding: utf-8 -*-
"""Cache wrapper — the main entry point for nb_cache.

Integrates backends, decorators, commands, tags, transactions,
and middleware into a single unified API.
"""
import contextlib
import functools
import threading

from nb_cache._compat import is_coroutine_function
from nb_cache.backends.memory import MemoryBackend
from nb_cache.condition import NOT_NONE, get_cache_condition, with_exceptions, only_exceptions
from nb_cache.exceptions import BackendNotInitializedError
from nb_cache.key import get_cache_key_template
from nb_cache.serialize import (
    Serializer, PickleSerializer, JsonSerializer,
    GzipCompressor, ZlibCompressor, NullCompressor, HashSigner,
    default_serializer,
)
from nb_cache.tags import TagRegistry, get_default_tag_registry
from nb_cache.transaction import Transaction, TransactionMode
from nb_cache.ttl import ttl_to_seconds

_default_backend = [None]
_default_backend_lock = threading.Lock()


def _get_default_backend():
    """Get the global default backend, auto-initializing MemoryBackend if never set."""
    if _default_backend[0] is None:
        with _default_backend_lock:
            if _default_backend[0] is None:
                be = MemoryBackend()
                be.init_sync()
                _default_backend[0] = be
    return _default_backend[0]


def _set_default_backend(backend):
    _default_backend[0] = backend


_BACKEND_REGISTRY = {
    'mem': 'nb_cache.backends.memory:MemoryBackend',
    'memory': 'nb_cache.backends.memory:MemoryBackend',
    'redis': 'nb_cache.backends.redis:RedisBackend',
    'rediss': 'nb_cache.backends.redis:RedisBackend',
    'dual': 'nb_cache.backends.dual:DualBackend',
}


def _resolve_backend_class(scheme):
    path = _BACKEND_REGISTRY.get(scheme)
    if path is None:
        raise ValueError("Unknown backend scheme: {!r}".format(scheme))
    module_path, cls_name = path.rsplit(':', 1)
    import importlib
    module = importlib.import_module(module_path)
    return getattr(module, cls_name)


def _parse_settings_url(url):
    """Parse a settings URL like 'mem://', 'redis://host:port/db?opt=val'."""
    from urllib.parse import urlparse, parse_qs
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()

    kwargs = {}
    if parsed.hostname:
        kwargs['host'] = parsed.hostname
    if parsed.port:
        kwargs['port'] = parsed.port
    if parsed.password:
        kwargs['password'] = parsed.password
    if parsed.path and parsed.path.strip('/'):
        try:
            kwargs['db'] = int(parsed.path.strip('/'))
        except ValueError:
            pass

    qs = parse_qs(parsed.query)
    for k, v in qs.items():
        val = v[0] if len(v) == 1 else v
        if val == 'true':
            val = True
        elif val == 'false':
            val = False
        else:
            try:
                val = int(val)
            except (ValueError, TypeError):
                try:
                    val = float(val)
                except (ValueError, TypeError):
                    pass
        kwargs[k] = val

    if scheme in ('redis', 'rediss'):
        proto = 'rediss' if scheme == 'rediss' else 'redis'
        redis_url = "{}://".format(proto)
        if parsed.password:
            redis_url += ":{}@".format(parsed.password)
        redis_url += parsed.hostname or 'localhost'
        if parsed.port:
            redis_url += ":{}".format(parsed.port)
        if parsed.path:
            redis_url += parsed.path
        kwargs['url'] = redis_url

    return scheme, kwargs


def register_backend(scheme, backend_path):
    """Register a custom backend class.

    Args:
        scheme: URL scheme (e.g. 'custom').
        backend_path: Dotted path like 'mymodule:MyBackend'.
    """
    _BACKEND_REGISTRY[scheme] = backend_path


class Cache(object):
    """Main cache interface. Integrates backend, decorators, tags, etc.

    Usage::

        from nb_cache import cache

        # Setup with URL
        cache.setup("mem://")
        cache.setup("redis://localhost:6379/0")

        # Direct operations
        cache.set_sync("key", "value", ttl=60)
        val = cache.get_sync("key")

        # As decorator
        @cache.cache(ttl=60)
        def get_user(user_id):
            return db.query(user_id)
    """

    def __init__(self):
        self._backend = None
        self._middlewares = []
        self._tag_registry = get_default_tag_registry()
        self._serializer = default_serializer
        self._is_setup = False

    @property
    def is_setup(self):
        return self._is_setup

    @property
    def is_init(self):
        return self._backend is not None and self._backend.is_init

    # --- Setup ---

    def setup(self, settings_url, middlewares=None, prefix="", **kwargs):
        """Configure the cache backend from a URL.

        Args:
            settings_url: URL like 'mem://', 'redis://host:port/db'.
            middlewares: List of Middleware instances.
            prefix: Global key prefix.
            **kwargs: Extra arguments passed to the backend.

        Supported URL schemes: mem://, redis://, rediss://, dual://
        """
        scheme, parsed_kwargs = _parse_settings_url(settings_url)
        parsed_kwargs.update(kwargs)

        secret = parsed_kwargs.pop('secret', '')
        digestmod = parsed_kwargs.pop('digestmod', 'md5')
        compress = parsed_kwargs.pop('compress_type', None)
        pickle_type = parsed_kwargs.pop('pickle_type', 'pickle')

        if prefix:
            parsed_kwargs['prefix'] = prefix

        cls = _resolve_backend_class(scheme)
        self._backend = cls(**parsed_kwargs)

        if middlewares:
            self._middlewares = list(middlewares)

        signer = HashSigner(secret=secret, digestmod=digestmod)
        if pickle_type == 'json':
            ser = JsonSerializer()
        else:
            ser = PickleSerializer()
        if compress == 'gzip':
            comp = GzipCompressor()
        elif compress == 'zlib':
            comp = ZlibCompressor()
        else:
            comp = NullCompressor()
        self._serializer = Serializer(serializer=ser, compressor=comp, signer=signer)

        self._backend.init_sync()
        self._is_setup = True
        return self

    # --- Init / Close ---

    async def init(self):
        """Re-initialize the async client (only needed if you use setup() in a sync
        context but later need to swap to a freshly-created async event loop)."""
        if self._backend is None:
            raise BackendNotInitializedError("Call setup() first")
        await self._backend.init()

    def init_sync(self):
        """Explicitly re-initialize the backend. Usually not needed — setup() calls
        this automatically."""
        if self._backend is None:
            raise BackendNotInitializedError("Call setup() first")
        self._backend.init_sync()

    async def close(self):
        if self._backend:
            await self._backend.close()

    def close_sync(self):
        if self._backend:
            self._backend.close_sync()

    async def ping(self):
        return await self._backend.ping()

    def ping_sync(self):
        return self._backend.ping_sync()

    def _ensure_backend(self):
        if self._backend is None:
            self.setup("mem://")
            self.init_sync()

    # --- Direct cache operations (async) ---

    async def get(self, key):
        self._ensure_backend()
        return await self._backend.get(key)

    async def set(self, key, value, ttl=None):
        self._ensure_backend()
        await self._backend.set(key, value, ttl=ttl)

    async def delete(self, key):
        self._ensure_backend()
        self._tag_registry.remove_key(key)
        return await self._backend.delete(key)

    async def exists(self, key):
        self._ensure_backend()
        return await self._backend.exists(key)

    async def expire(self, key, ttl):
        self._ensure_backend()
        return await self._backend.expire(key, ttl)

    async def get_expire(self, key):
        self._ensure_backend()
        return await self._backend.get_expire(key)

    async def clear(self):
        self._ensure_backend()
        self._tag_registry.clear()
        await self._backend.clear()

    async def incr(self, key, amount=1):
        self._ensure_backend()
        return await self._backend.incr(key, amount)

    async def get_many(self, *keys):
        self._ensure_backend()
        return await self._backend.get_many(*keys)

    async def set_many(self, pairs, ttl=None):
        self._ensure_backend()
        await self._backend.set_many(pairs, ttl=ttl)

    async def delete_many(self, *keys):
        self._ensure_backend()
        for k in keys:
            self._tag_registry.remove_key(k)
        await self._backend.delete_many(*keys)

    async def delete_match(self, pattern):
        self._ensure_backend()
        await self._backend.delete_match(pattern)

    async def scan(self, pattern):
        self._ensure_backend()
        return await self._backend.scan(pattern)

    async def get_match(self, pattern):
        self._ensure_backend()
        return await self._backend.get_match(pattern)

    async def get_keys_count(self):
        self._ensure_backend()
        return await self._backend.get_keys_count()

    async def get_size(self):
        self._ensure_backend()
        return await self._backend.get_size()

    async def set_lock(self, key, ttl):
        self._ensure_backend()
        return await self._backend.set_lock(key, ttl)

    async def unlock(self, key):
        self._ensure_backend()
        return await self._backend.unlock(key)

    async def is_locked(self, key):
        self._ensure_backend()
        return await self._backend.is_locked(key)

    # --- Direct cache operations (sync) ---

    def get_sync(self, key):
        self._ensure_backend()
        return self._backend.get_sync(key)

    def set_sync(self, key, value, ttl=None):
        self._ensure_backend()
        self._backend.set_sync(key, value, ttl=ttl)

    def delete_sync(self, key):
        self._ensure_backend()
        self._tag_registry.remove_key(key)
        return self._backend.delete_sync(key)

    def exists_sync(self, key):
        self._ensure_backend()
        return self._backend.exists_sync(key)

    def expire_sync(self, key, ttl):
        self._ensure_backend()
        return self._backend.expire_sync(key, ttl)

    def get_expire_sync(self, key):
        self._ensure_backend()
        return self._backend.get_expire_sync(key)

    def clear_sync(self):
        self._ensure_backend()
        self._tag_registry.clear()
        self._backend.clear_sync()

    def incr_sync(self, key, amount=1):
        self._ensure_backend()
        return self._backend.incr_sync(key, amount)

    def get_many_sync(self, *keys):
        self._ensure_backend()
        return self._backend.get_many_sync(*keys)

    def set_many_sync(self, pairs, ttl=None):
        self._ensure_backend()
        self._backend.set_many_sync(pairs, ttl=ttl)

    def delete_many_sync(self, *keys):
        self._ensure_backend()
        for k in keys:
            self._tag_registry.remove_key(k)
        self._backend.delete_many_sync(*keys)

    def delete_match_sync(self, pattern):
        self._ensure_backend()
        self._backend.delete_match_sync(pattern)

    def scan_sync(self, pattern):
        self._ensure_backend()
        return self._backend.scan_sync(pattern)

    def get_match_sync(self, pattern):
        self._ensure_backend()
        return self._backend.get_match_sync(pattern)

    # --- Lock context managers ---

    @contextlib.contextmanager
    def lock(self, key, ttl=60):
        """Sync lock context manager.

        Usage::

            with cache.lock("resource", ttl=10):
                do_something()
        """
        self._ensure_backend()
        _ttl = ttl_to_seconds(ttl) or 60
        acquired = self._backend.set_lock_sync(key, _ttl)
        if not acquired:
            import time
            deadline = time.time() + _ttl
            while time.time() < deadline:
                time.sleep(0.1)
                acquired = self._backend.set_lock_sync(key, _ttl)
                if acquired:
                    break
            if not acquired:
                from nb_cache.exceptions import LockedError
                raise LockedError("Cannot acquire lock: {}".format(key))
        try:
            yield
        finally:
            self._backend.unlock_sync(key)

    @contextlib.asynccontextmanager
    async def alock(self, key, ttl=60):
        """Async lock context manager.

        Usage::

            async with cache.alock("resource", ttl=10):
                await do_something()
        """
        import asyncio
        self._ensure_backend()
        _ttl = ttl_to_seconds(ttl) or 60
        acquired = await self._backend.set_lock(key, _ttl)
        if not acquired:
            import time
            deadline = time.time() + _ttl
            while time.time() < deadline:
                await asyncio.sleep(0.1)
                acquired = await self._backend.set_lock(key, _ttl)
                if acquired:
                    break
            if not acquired:
                from nb_cache.exceptions import LockedError
                raise LockedError("Cannot acquire lock: {}".format(key))
        try:
            yield
        finally:
            await self._backend.unlock(key)

    # --- Tags ---

    def delete_tags_sync(self, *tags):
        """Delete all keys associated with the given tags (sync)."""
        keys = self._tag_registry.get_all_keys(*tags)
        for k in keys:
            self._backend.delete_sync(k)
        for tag in tags:
            self._tag_registry.remove_tag(tag)

    async def delete_tags(self, *tags):
        """Delete all keys associated with the given tags (async)."""
        keys = self._tag_registry.get_all_keys(*tags)
        for k in keys:
            await self._backend.delete(k)
        for tag in tags:
            self._tag_registry.remove_tag(tag)

    # --- Transactions ---

    def transaction(self, mode=TransactionMode.FAST):
        """Create a transaction (works as both sync and async context manager).

        Usage::

            with cache.transaction() as tx:
                tx.set("k1", "v1")
                tx.set("k2", "v2")

            async with cache.transaction() as tx:
                tx.set("k1", "v1")
        """
        self._ensure_backend()
        return Transaction(self._backend, mode=mode)

    # --- Decorator shortcuts ---

    def cache(self, ttl, key=None, condition=None, prefix="", lock=False,
              lock_ttl=None, tags=(), serializer=None):
        from nb_cache.decorators.cache import cache as _cache
        return _cache(ttl, key=key, condition=condition, prefix=prefix,
                      lock=lock, lock_ttl=lock_ttl, tags=tags,
                      backend=self._backend, serializer=serializer or self._serializer,
                      tag_registry=self._tag_registry if tags else None)

    def failover(self, ttl, key=None, exceptions=None, condition=None,
                 prefix="fail", tags=(), serializer=None):
        from nb_cache.decorators.failover import failover as _failover
        return _failover(ttl, key=key, exceptions=exceptions, condition=condition,
                         prefix=prefix, tags=tags, backend=self._backend,
                         serializer=serializer or self._serializer)

    def early(self, ttl, key=None, early_ttl=None, condition=None,
              prefix="early", tags=(), serializer=None):
        from nb_cache.decorators.early import early as _early
        return _early(ttl, key=key, early_ttl=early_ttl, condition=condition,
                      prefix=prefix, tags=tags, backend=self._backend,
                      serializer=serializer or self._serializer)

    def soft(self, ttl, key=None, soft_ttl=None, condition=None,
             prefix="soft", tags=(), serializer=None):
        from nb_cache.decorators.soft import soft as _soft
        return _soft(ttl, key=key, soft_ttl=soft_ttl, condition=condition,
                     prefix=prefix, tags=tags, backend=self._backend,
                     serializer=serializer or self._serializer)

    def hit(self, ttl, cache_hits, update_after=0, key=None, condition=None,
            prefix="hit", tags=(), serializer=None):
        from nb_cache.decorators.hit import hit as _hit
        return _hit(ttl, cache_hits=cache_hits, update_after=update_after,
                    key=key, condition=condition, prefix=prefix, tags=tags,
                    backend=self._backend, serializer=serializer or self._serializer)

    def locked(self, ttl=None, key=None, wait=True, prefix="locked",
               check_interval=0.1):
        from nb_cache.decorators.locked import locked as _locked
        return _locked(ttl=ttl, key=key, wait=wait, prefix=prefix,
                       check_interval=check_interval, backend=self._backend)

    def thunder_protection(self, ttl=None, key=None, prefix="thunder"):
        from nb_cache.decorators.locked import thunder_protection as _tp
        return _tp(ttl=ttl, key=key, prefix=prefix, backend=self._backend)

    def circuit_breaker(self, errors_rate, period, ttl, half_open_ttl=None,
                        exceptions=None, key=None, min_calls=1,
                        prefix="circuit_breaker"):
        from nb_cache.decorators.circuit_breaker import circuit_breaker as _cb
        return _cb(errors_rate=errors_rate, period=period, ttl=ttl,
                   half_open_ttl=half_open_ttl, exceptions=exceptions,
                   key=key, min_calls=min_calls, prefix=prefix,
                   backend=self._backend)

    def rate_limit(self, limit, period, ttl=None, action=None,
                   prefix="rate_limit", key=None):
        from nb_cache.decorators.rate_limit import rate_limit as _rl
        return _rl(limit=limit, period=period, ttl=ttl, action=action,
                   prefix=prefix, key=key, backend=self._backend)

    def slice_rate_limit(self, limit, period, key=None, action=None,
                         prefix="srl"):
        from nb_cache.decorators.rate_limit import slice_rate_limit as _srl
        return _srl(limit=limit, period=period, key=key, action=action,
                    prefix=prefix, backend=self._backend)

    def bloom(self, capacity, name=None, false_positives=1, prefix="bloom"):
        from nb_cache.decorators.bloom import bloom as _bloom
        return _bloom(capacity=capacity, name=name, false_positives=false_positives,
                      prefix=prefix, backend=self._backend)

    def dual_bloom(self, capacity, name=None, false=1, prefix="dual_bloom"):
        from nb_cache.decorators.bloom import dual_bloom as _db
        return _db(capacity=capacity, name=name, false=false,
                   prefix=prefix, backend=self._backend)

    def iterator(self, ttl, key=None, condition=None, prefix="iter",
                 serializer=None):
        from nb_cache.decorators.iterator import iterator as _iter
        return _iter(ttl=ttl, key=key, condition=condition, prefix=prefix,
                     backend=self._backend, serializer=serializer or self._serializer)

    # --- Invalidation decorator ---

    def invalidate(self, func_or_cache_instance, args_map=None):
        """Decorator that invalidates cache for a cached function.

        Usage::

            @cache.cache(ttl=60)
            def get_user(user_id):
                ...

            @cache.invalidate(get_user)
            def update_user(user_id, data):
                ...
        """
        def decorator(invalidating_func):
            if is_coroutine_function(invalidating_func):
                @functools.wraps(invalidating_func)
                async def async_wrapper(*args, **kwargs):
                    result = await invalidating_func(*args, **kwargs)
                    key_template = getattr(func_or_cache_instance, '_cache_key_template', None)
                    if key_template:
                        from nb_cache.key import get_cache_key
                        cache_key = get_cache_key(func_or_cache_instance.__wrapped__
                                                  if hasattr(func_or_cache_instance, '__wrapped__')
                                                  else invalidating_func,
                                                  key_template, args, kwargs)
                        await self.delete(cache_key)
                    return result
                return async_wrapper
            else:
                @functools.wraps(invalidating_func)
                def sync_wrapper(*args, **kwargs):
                    result = invalidating_func(*args, **kwargs)
                    key_template = getattr(func_or_cache_instance, '_cache_key_template', None)
                    if key_template:
                        from nb_cache.key import get_cache_key
                        cache_key = get_cache_key(func_or_cache_instance.__wrapped__
                                                  if hasattr(func_or_cache_instance, '__wrapped__')
                                                  else invalidating_func,
                                                  key_template, args, kwargs)
                        self.delete_sync(cache_key)
                    return result
                return sync_wrapper
        return decorator

    # --- Middleware ---

    def add_middleware(self, middleware):
        self._middlewares.append(middleware)

    # --- Backend access ---

    @property
    def backend(self):
        return self._backend
