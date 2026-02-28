# -*- coding: utf-8 -*-
"""Cache condition utilities.

Conditions determine whether a function result should be cached.
"""

_SENTINEL = object()


def NOT_NONE(result):
    """Cache only if the result is not None."""
    return result is not None


def always_true(result):
    """Always cache the result."""
    return True


def with_exceptions(*exceptions):
    """Create a condition that caches results and specified exceptions.

    Usage::

        @cache(ttl=60, condition=with_exceptions(ValueError, KeyError))
        def my_func():
            ...
    """
    if not exceptions:
        exceptions = (Exception,)

    def _condition(result):
        return True

    _condition._cache_exceptions = exceptions
    return _condition


def only_exceptions(*exceptions):
    """Create a condition that only caches when an exception is raised.

    Usage::

        @cache(ttl=60, condition=only_exceptions(TimeoutError))
        def my_func():
            ...
    """
    if not exceptions:
        exceptions = (Exception,)

    def _condition(result):
        return False

    _condition._cache_exceptions = exceptions
    _condition._only_exceptions = True
    return _condition


def get_cache_condition(condition):
    """Normalize a condition argument into a callable."""
    if condition is None:
        return NOT_NONE
    if callable(condition):
        return condition
    raise TypeError("condition must be callable or None, got {!r}".format(type(condition)))
