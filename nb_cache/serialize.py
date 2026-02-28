# -*- coding: utf-8 -*-
"""Serialization, signing, and compression pipeline for cache values."""
import hashlib
import pickle
import json
import zlib
import gzip
import io

_SENTINEL = object()


class PickleSerializer(object):
    """Standard pickle serializer."""

    def dumps(self, obj):
        return pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)

    def loads(self, data):
        return pickle.loads(data)


class JsonSerializer(object):
    """JSON serializer for simple types."""

    def dumps(self, obj):
        return json.dumps(obj, ensure_ascii=False, default=str).encode('utf-8')

    def loads(self, data):
        if isinstance(data, (bytes, bytearray)):
            data = data.decode('utf-8')
        return json.loads(data)


class NullCompressor(object):
    def compress(self, data):
        return data

    def decompress(self, data):
        return data


class GzipCompressor(object):
    def __init__(self, level=6):
        self._level = level

    def compress(self, data):
        return gzip.compress(data, compresslevel=self._level)

    def decompress(self, data):
        return gzip.decompress(data)


class ZlibCompressor(object):
    def __init__(self, level=6):
        self._level = level

    def compress(self, data):
        return zlib.compress(data, self._level)

    def decompress(self, data):
        return zlib.decompress(data)


class HashSigner(object):
    """Signs serialized data with a hash to detect tampering."""

    def __init__(self, secret="", digestmod="md5"):
        self._secret = secret.encode('utf-8') if isinstance(secret, str) else secret
        self._digestmod = digestmod

    def sign(self, data):
        if not self._secret:
            return data
        import hmac
        sig = hmac.new(self._secret, data, self._digestmod).digest()
        return sig + data

    def unsign(self, data):
        if not self._secret:
            return data
        import hmac
        h = hmac.new(self._secret, b'', self._digestmod)
        sig_len = h.digest_size
        if len(data) < sig_len:
            return None
        sig = data[:sig_len]
        payload = data[sig_len:]
        expected = hmac.new(self._secret, payload, self._digestmod).digest()
        if not hmac.compare_digest(sig, expected):
            return None
        return payload


class Serializer(object):
    """Unified serialization pipeline: serialize -> sign -> compress."""

    def __init__(self, serializer=None, compressor=None, signer=None):
        self._serializer = serializer or PickleSerializer()
        self._compressor = compressor or NullCompressor()
        self._signer = signer or HashSigner()

    def encode(self, obj):
        data = self._serializer.dumps(obj)
        data = self._signer.sign(data)
        data = self._compressor.compress(data)
        return data

    def decode(self, data):
        if data is None:
            return _SENTINEL
        try:
            data = self._compressor.decompress(data)
            data = self._signer.unsign(data)
            if data is None:
                return _SENTINEL
            return self._serializer.loads(data)
        except Exception:
            return _SENTINEL

    @property
    def SENTINEL(self):
        return _SENTINEL


default_serializer = Serializer()
