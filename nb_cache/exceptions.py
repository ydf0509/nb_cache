# -*- coding: utf-8 -*-
"""Exception hierarchy for nb_cache."""


class CacheError(Exception):
    """Base exception for all cache errors."""
    pass


class BackendNotInitializedError(CacheError):
    """Raised when backend is not initialized."""
    pass


class CacheBackendInteractionError(CacheError):
    """Raised when a backend interaction fails."""
    pass


class LockError(CacheError):
    """Raised when a lock cannot be acquired."""
    pass


class LockedError(LockError):
    """Raised when a resource is already locked."""
    pass


class CircuitBreakerOpen(CacheError):
    """Raised when the circuit breaker is open."""
    pass


class RateLimitError(CacheError):
    """Raised when a rate limit is exceeded."""
    pass


class SerializationError(CacheError):
    """Raised when serialization/deserialization fails."""
    pass


class TagError(CacheError):
    """Raised on tag-related errors."""
    pass
