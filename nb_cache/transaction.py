# -*- coding: utf-8 -*-
"""Transaction support for cache operations.

Provides FAST, LOCKED, and SERIALIZABLE transaction modes.
"""
import enum
import threading


class TransactionMode(enum.Enum):
    FAST = "fast"
    LOCKED = "locked"
    SERIALIZABLE = "serializable"


class Transaction(object):
    """Cache transaction that buffers operations and commits/rollbacks.

    Usage::

        with cache.transaction() as tx:
            tx.set("key1", "val1")
            tx.set("key2", "val2")
        # auto-commits on exit

        # or with explicit rollback:
        tx = cache.transaction()
        tx.begin()
        tx.set("key1", "val1")
        tx.rollback()
    """

    def __init__(self, backend, mode=TransactionMode.FAST):
        self._backend = backend
        self._mode = mode
        self._buffer = []
        self._lock = threading.RLock() if mode != TransactionMode.FAST else None
        self._active = False

    def begin(self):
        if self._lock:
            self._lock.acquire()
        self._active = True
        self._buffer = []

    def set(self, key, value, ttl=None):
        self._buffer.append(('set', key, value, ttl))

    def delete(self, key):
        self._buffer.append(('delete', key, None, None))

    def commit_sync(self):
        try:
            for op, key, value, ttl in self._buffer:
                if op == 'set':
                    self._backend.set_sync(key, value, ttl=ttl)
                elif op == 'delete':
                    self._backend.delete_sync(key)
        finally:
            self._buffer = []
            self._active = False
            if self._lock:
                try:
                    self._lock.release()
                except RuntimeError:
                    pass

    async def commit(self):
        try:
            for op, key, value, ttl in self._buffer:
                if op == 'set':
                    await self._backend.set(key, value, ttl=ttl)
                elif op == 'delete':
                    await self._backend.delete(key)
        finally:
            self._buffer = []
            self._active = False
            if self._lock:
                try:
                    self._lock.release()
                except RuntimeError:
                    pass

    def rollback(self):
        self._buffer = []
        self._active = False
        if self._lock:
            try:
                self._lock.release()
            except RuntimeError:
                pass

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback()
        else:
            self.commit_sync()
        return False

    async def __aenter__(self):
        self.begin()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.rollback()
        else:
            await self.commit()
        return False
