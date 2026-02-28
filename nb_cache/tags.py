# -*- coding: utf-8 -*-
"""Tag-based cache invalidation system.

Tags allow grouping cache keys so they can be invalidated together.
"""
import threading


class TagRegistry(object):
    """Registry mapping tags to cache keys."""

    def __init__(self):
        self._tags = {}  # tag -> set of keys
        self._lock = threading.RLock()

    def register(self, key, tags):
        if not tags:
            return
        with self._lock:
            for tag in tags:
                if tag not in self._tags:
                    self._tags[tag] = set()
                self._tags[tag].add(key)

    def get_keys(self, tag):
        with self._lock:
            return set(self._tags.get(tag, set()))

    def get_all_keys(self, *tags):
        keys = set()
        for tag in tags:
            keys.update(self.get_keys(tag))
        return keys

    def remove_key(self, key):
        with self._lock:
            for tag in list(self._tags.keys()):
                self._tags[tag].discard(key)
                if not self._tags[tag]:
                    del self._tags[tag]

    def remove_tag(self, tag):
        with self._lock:
            self._tags.pop(tag, None)

    def clear(self):
        with self._lock:
            self._tags.clear()


_default_tag_registry = TagRegistry()


def get_default_tag_registry():
    return _default_tag_registry
