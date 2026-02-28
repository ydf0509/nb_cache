# -*- coding: utf-8 -*-
"""Core functionality tests for nb_cache."""
import asyncio
import time
import pytest

from nb_cache import Cache, NOT_NONE, ttl_to_seconds, MemoryBackend
from nb_cache.serialize import default_serializer, _SENTINEL
from nb_cache.key import get_cache_key, get_cache_key_template
from nb_cache.ttl import ttl_to_seconds as _ttl
from nb_cache.exceptions import LockedError, RateLimitError, CircuitBreakerOpen


# ============================================================
# TTL parsing tests
# ============================================================

class TestTTLParsing:
    def test_int(self):
        assert _ttl(60) == 60.0

    def test_float(self):
        assert _ttl(1.5) == 1.5

    def test_none(self):
        assert _ttl(None) is None

    def test_zero(self):
        assert _ttl(0) is None

    def test_timedelta(self):
        from datetime import timedelta
        assert _ttl(timedelta(hours=1)) == 3600.0

    def test_string_hours(self):
        assert _ttl("1h") == 3600.0

    def test_string_minutes(self):
        assert _ttl("30m") == 1800.0

    def test_string_combined(self):
        assert _ttl("1d12h30m10s") == 131410.0

    def test_string_seconds(self):
        assert _ttl("45s") == 45.0

    def test_string_numeric(self):
        assert _ttl("120") == 120.0

    def test_callable(self):
        assert _ttl(lambda: 42) == 42.0


# ============================================================
# Key generation tests
# ============================================================

class TestKeyGeneration:
    def test_auto_key(self):
        def my_func(a, b):
            pass
        key = get_cache_key(my_func, None, (1, 2), {})
        assert "my_func" in key
        assert "a=1" in key
        assert "b=2" in key

    def test_template_key(self):
        def my_func(user_id, name):
            pass
        template = "{user_id}:{name}"
        key = get_cache_key(my_func, template, (123, "alice"), {})
        assert key == "123:alice"

    def test_key_template_generation(self):
        def my_func(x, y):
            pass
        template = get_cache_key_template(my_func, prefix="test")
        assert "test" in template
        assert "{x}" in template
        assert "{y}" in template


# ============================================================
# Serialization tests
# ============================================================

class TestSerialization:
    def test_encode_decode(self):
        obj = {"key": "value", "num": 42}
        encoded = default_serializer.encode(obj)
        decoded = default_serializer.decode(encoded)
        assert decoded == obj

    def test_decode_none(self):
        result = default_serializer.decode(None)
        assert result is _SENTINEL

    def test_various_types(self):
        for val in [42, "hello", [1, 2, 3], {"a": 1}, (1, 2), True, None]:
            encoded = default_serializer.encode(val)
            decoded = default_serializer.decode(encoded)
            assert decoded == val


# ============================================================
# Memory backend tests
# ============================================================

class TestMemoryBackend:
    def setup_method(self):
        self.be = MemoryBackend(size=100)
        self.be.init_sync()

    def teardown_method(self):
        self.be.close_sync()

    def test_get_set(self):
        self.be.set_sync("k1", "v1")
        assert self.be.get_sync("k1") == "v1"

    def test_get_missing(self):
        assert self.be.get_sync("missing") is None

    def test_delete(self):
        self.be.set_sync("k1", "v1")
        self.be.delete_sync("k1")
        assert self.be.get_sync("k1") is None

    def test_exists(self):
        self.be.set_sync("k1", "v1")
        assert self.be.exists_sync("k1") is True
        assert self.be.exists_sync("k2") is False

    def test_ttl_expiry(self):
        self.be.set_sync("k1", "v1", ttl=0.2)
        assert self.be.get_sync("k1") == "v1"
        time.sleep(0.3)
        assert self.be.get_sync("k1") is None

    def test_incr(self):
        assert self.be.incr_sync("counter") == 1
        assert self.be.incr_sync("counter") == 2
        assert self.be.incr_sync("counter", 5) == 7

    def test_clear(self):
        self.be.set_sync("k1", "v1")
        self.be.set_sync("k2", "v2")
        self.be.clear_sync()
        assert self.be.get_sync("k1") is None
        assert self.be.get_sync("k2") is None

    def test_get_many(self):
        self.be.set_sync("a", 1)
        self.be.set_sync("b", 2)
        result = self.be.get_many_sync("a", "b", "c")
        assert result == [1, 2, None]

    def test_set_many(self):
        self.be.set_many_sync({"x": 10, "y": 20})
        assert self.be.get_sync("x") == 10
        assert self.be.get_sync("y") == 20

    def test_lru_eviction(self):
        be = MemoryBackend(size=3)
        be.init_sync()
        be.set_sync("a", 1)
        be.set_sync("b", 2)
        be.set_sync("c", 3)
        be.set_sync("d", 4)  # should evict 'a'
        assert be.get_sync("a") is None
        assert be.get_sync("d") == 4

    def test_lock(self):
        assert self.be.set_lock_sync("res", 5) is True
        assert self.be.is_locked_sync("res") is True
        assert self.be.set_lock_sync("res", 5) is False
        self.be.unlock_sync("res")
        assert self.be.is_locked_sync("res") is False

    def test_scan(self):
        self.be.set_sync("user:1", "a")
        self.be.set_sync("user:2", "b")
        self.be.set_sync("post:1", "c")
        keys = self.be.scan_sync("user:*")
        assert sorted(keys) == ["user:1", "user:2"]

    def test_delete_match(self):
        self.be.set_sync("user:1", "a")
        self.be.set_sync("user:2", "b")
        self.be.set_sync("post:1", "c")
        self.be.delete_match_sync("user:*")
        assert self.be.get_sync("user:1") is None
        assert self.be.get_sync("post:1") == "c"

    def test_expire(self):
        self.be.set_sync("k1", "v1")
        self.be.expire_sync("k1", 0.2)
        assert self.be.get_sync("k1") == "v1"
        time.sleep(0.3)
        assert self.be.get_sync("k1") is None

    def test_get_expire(self):
        self.be.set_sync("k1", "v1", ttl=10)
        remaining = self.be.get_expire_sync("k1")
        assert remaining is not None
        assert 9 <= remaining <= 10


# ============================================================
# Async memory backend tests
# ============================================================

class TestMemoryBackendAsync:
    def setup_method(self):
        self.be = MemoryBackend(size=100)
        self.be.init_sync()

    def teardown_method(self):
        self.be.close_sync()

    def test_async_get_set(self):
        async def _test():
            await self.be.set("k1", "v1")
            val = await self.be.get("k1")
            assert val == "v1"
        asyncio.get_event_loop().run_until_complete(_test())

    def test_async_lock(self):
        async def _test():
            assert await self.be.set_lock("res", 5) is True
            assert await self.be.is_locked("res") is True
            await self.be.unlock("res")
            assert await self.be.is_locked("res") is False
        asyncio.get_event_loop().run_until_complete(_test())


# ============================================================
# Cache decorator tests (sync)
# ============================================================

class TestCacheDecoratorSync:
    def setup_method(self):
        self.c = Cache()
        self.c.setup("mem://")  # setup() 自动初始化，无需调用 init_sync()
        self.call_count = 0

    def test_basic_cache(self):
        @self.c.cache(ttl=60)
        def get_data(x):
            self.call_count += 1
            return x * 2

        assert get_data(5) == 10
        assert self.call_count == 1
        assert get_data(5) == 10
        assert self.call_count == 1  # served from cache
        assert get_data(6) == 12
        assert self.call_count == 2  # different args

    def test_cache_with_lock(self):
        @self.c.cache(ttl=60, lock=True)
        def get_data(x):
            self.call_count += 1
            return x * 3

        assert get_data(5) == 15
        assert self.call_count == 1
        assert get_data(5) == 15
        assert self.call_count == 1

    def test_cache_ttl_expiry(self):
        @self.c.cache(ttl=0.3)
        def get_data(x):
            self.call_count += 1
            return x

        assert get_data(1) == 1
        assert self.call_count == 1
        time.sleep(0.4)
        assert get_data(1) == 1
        assert self.call_count == 2  # cache expired


# ============================================================
# Cache decorator tests (async)
# ============================================================

class TestCacheDecoratorAsync:
    def setup_method(self):
        self.c = Cache()
        self.c.setup("mem://")
        self.call_count = 0

    def test_async_cache(self):
        @self.c.cache(ttl=60)
        async def get_data(x):
            self.call_count += 1
            return x * 2

        loop = asyncio.get_event_loop()
        assert loop.run_until_complete(get_data(5)) == 10
        assert self.call_count == 1
        assert loop.run_until_complete(get_data(5)) == 10
        assert self.call_count == 1


# ============================================================
# Failover decorator tests
# ============================================================

class TestFailoverSync:
    def setup_method(self):
        self.c = Cache()
        self.c.setup("mem://")
        self.call_count = 0
        self.should_fail = False

    def test_failover(self):
        @self.c.failover(ttl=60, exceptions=(ValueError,))
        def get_data(x):
            self.call_count += 1
            if self.should_fail:
                raise ValueError("fail")
            return x * 2

        assert get_data(5) == 10
        self.should_fail = True
        assert get_data(5) == 10  # returns cached value

    def test_failover_no_cache(self):
        @self.c.failover(ttl=60, exceptions=(ValueError,))
        def get_data(x):
            raise ValueError("fail")

        with pytest.raises(ValueError):
            get_data(99)  # no cached value, re-raises


# ============================================================
# Rate limit tests
# ============================================================

class TestRateLimitSync:
    def setup_method(self):
        self.c = Cache()
        self.c.setup("mem://")

    def test_rate_limit(self):
        @self.c.rate_limit(limit=3, period=60)
        def api_call():
            return "ok"

        assert api_call() == "ok"
        assert api_call() == "ok"
        assert api_call() == "ok"
        with pytest.raises(RateLimitError):
            api_call()  # 4th call exceeds limit


# ============================================================
# Locked decorator tests
# ============================================================

class TestLockedSync:
    def setup_method(self):
        self.c = Cache()
        self.c.setup("mem://")

    def test_locked_wait_false(self):
        @self.c.locked(ttl=5, wait=False)
        def critical():
            return "done"

        assert critical() == "done"


# ============================================================
# Lock context manager tests
# ============================================================

class TestLockContextManager:
    def setup_method(self):
        self.c = Cache()
        self.c.setup("mem://")

    def test_sync_lock(self):
        with self.c.lock("test_resource", ttl=5):
            assert self.c.backend.is_locked_sync("test_resource") is True
        assert self.c.backend.is_locked_sync("test_resource") is False

    def test_async_lock(self):
        async def _test():
            async with self.c.alock("test_resource", ttl=5):
                assert await self.c.backend.is_locked("test_resource") is True
            assert await self.c.backend.is_locked("test_resource") is False

        asyncio.get_event_loop().run_until_complete(_test())


# ============================================================
# Transaction tests
# ============================================================

class TestTransaction:
    def setup_method(self):
        self.c = Cache()
        self.c.setup("mem://")

    def test_sync_transaction_commit(self):
        with self.c.transaction() as tx:
            tx.set("k1", "v1", ttl=60)
            tx.set("k2", "v2", ttl=60)
        assert self.c.get_sync("k1") == "v1"
        assert self.c.get_sync("k2") == "v2"

    def test_sync_transaction_rollback(self):
        try:
            with self.c.transaction() as tx:
                tx.set("k3", "v3")
                raise RuntimeError("oops")
        except RuntimeError:
            pass
        assert self.c.get_sync("k3") is None

    def test_async_transaction(self):
        async def _test():
            async with self.c.transaction() as tx:
                tx.set("ak1", "av1")
                tx.set("ak2", "av2")
            assert await self.c.get("ak1") == "av1"

        asyncio.get_event_loop().run_until_complete(_test())


# ============================================================
# Cache wrapper direct operations tests
# ============================================================

class TestCacheWrapperOperations:
    def setup_method(self):
        self.c = Cache()
        self.c.setup("mem://")

    def test_set_get_sync(self):
        self.c.set_sync("key1", "val1", ttl=60)
        assert self.c.get_sync("key1") == "val1"

    def test_delete_sync(self):
        self.c.set_sync("key1", "val1")
        self.c.delete_sync("key1")
        assert self.c.get_sync("key1") is None

    def test_exists_sync(self):
        self.c.set_sync("key1", "val1")
        assert self.c.exists_sync("key1") is True
        assert self.c.exists_sync("key_missing") is False

    def test_incr_sync(self):
        assert self.c.incr_sync("cnt") == 1
        assert self.c.incr_sync("cnt") == 2

    def test_clear_sync(self):
        self.c.set_sync("a", 1)
        self.c.set_sync("b", 2)
        self.c.clear_sync()
        assert self.c.get_sync("a") is None

    def test_async_operations(self):
        async def _test():
            await self.c.set("akey", "aval", ttl=60)
            assert await self.c.get("akey") == "aval"
            assert await self.c.exists("akey") is True
            await self.c.delete("akey")
            assert await self.c.get("akey") is None

        asyncio.get_event_loop().run_until_complete(_test())


# ============================================================
# Iterator cache tests
# ============================================================

class TestIteratorCache:
    def setup_method(self):
        self.c = Cache()
        self.c.setup("mem://")
        self.call_count = 0

    def test_sync_generator_cache(self):
        @self.c.iterator(ttl=60)
        def gen_items():
            self.call_count += 1
            for i in range(3):
                yield i

        result1 = list(gen_items())
        assert result1 == [0, 1, 2]
        assert self.call_count == 1

        result2 = list(gen_items())
        assert result2 == [0, 1, 2]
        assert self.call_count == 1  # from cache


# ============================================================
# URL parsing tests
# ============================================================

class TestURLParsing:
    def test_mem_url(self):
        c = Cache()
        c.setup("mem://")
        assert c.is_setup
        assert isinstance(c.backend, MemoryBackend)

    def test_setup_with_prefix(self):
        c = Cache()
        c.setup("mem://", prefix="myapp")
        assert c.is_setup

    def test_setup_auto_init(self):
        """setup() should auto-initialize — no need to call init_sync() manually."""
        c = Cache()
        c.setup("mem://")
        c.set_sync("auto_init_test", "ok")
        assert c.get_sync("auto_init_test") == "ok"


# ============================================================
# Key template tests — dot-access and callable key
# ============================================================

class TestKeyTemplates:
    def setup_method(self):
        self.c = Cache()
        self.c.setup("mem://")
        self.call_count = 0

    # --- dot-access ---

    def test_dot_access_dict(self):
        """key='{user.id}:{user.name}' reads user['id'] and user['name']."""
        @self.c.cache(ttl=60, key="{user.id}:{user.name}")
        def get_user(user):
            self.call_count += 1
            return user

        u1 = {"id": 1, "name": "Alice", "extra": "ignored"}
        u2 = {"id": 1, "name": "Alice", "extra": "different_extra"}

        r1 = get_user(u1)
        assert r1 == u1
        assert self.call_count == 1

        r2 = get_user(u2)
        assert self.call_count == 1  # same id+name → cache hit despite different extra

    def test_dot_access_different_values(self):
        """Different attribute values produce different cache keys."""
        @self.c.cache(ttl=60, key="{user.id}")
        def get_user(user):
            self.call_count += 1
            return user['id']

        get_user({"id": 10})
        get_user({"id": 20})
        assert self.call_count == 2  # different ids, two cache misses

    def test_dot_access_with_hash(self):
        """Dot-access combined with :hash modifier."""
        @self.c.cache(ttl=60, key="{obj.data:hash}")
        def get_result(obj):
            self.call_count += 1
            return obj['data']

        get_result({"data": [1, 2, 3]})
        get_result({"data": [1, 2, 3]})
        assert self.call_count == 1

    def test_dot_access_with_lower(self):
        """Dot-access combined with :lower modifier."""
        @self.c.cache(ttl=60, key="{user.name:lower}")
        def get_user(user):
            self.call_count += 1
            return user

        get_user({"name": "Alice"})
        get_user({"name": "ALICE"})
        assert self.call_count == 1  # both map to "alice"

    # --- callable key ---

    def test_callable_key(self):
        """key=callable receives the same args as the decorated function."""
        def make_key(user_id, role):
            return "perm:{}:{}".format(role, user_id)

        @self.c.cache(ttl=60, key=make_key)
        def check(user_id, role):
            self.call_count += 1
            return True

        check(1, "admin")
        check(1, "admin")
        assert self.call_count == 1  # cache hit

        check(1, "viewer")
        assert self.call_count == 2  # different role → different key

    def test_callable_key_complex_logic(self):
        """Callable key can encode business logic that templates cannot."""
        def make_key(items, page):
            sorted_ids = sorted(i['id'] for i in items)
            return "list:{}:p{}".format("-".join(str(x) for x in sorted_ids), page)

        @self.c.cache(ttl=60, key=make_key)
        def get_list(items, page):
            self.call_count += 1
            return items

        items_a = [{"id": 3}, {"id": 1}, {"id": 2}]
        items_b = [{"id": 1}, {"id": 2}, {"id": 3}]

        get_list(items_a, 1)
        get_list(items_b, 1)
        assert self.call_count == 1  # sorted IDs equal → same key → cache hit


# ============================================================
# condition 参数测试
# ============================================================

class TestCacheCondition:
    def setup_method(self):
        self.c = Cache().setup("mem://")
        self.call_count = 0

    def test_not_none_skips_none(self):
        """NOT_NONE：返回 None 时不缓存，下次仍会调用函数。"""
        @self.c.cache(ttl=60, condition=NOT_NONE)
        def get_data(x):
            self.call_count += 1
            return None if x == 0 else x

        get_data(0)
        get_data(0)
        assert self.call_count == 2  # None 不缓存，每次都执行

        get_data(1)
        get_data(1)
        assert self.call_count == 3  # 非 None，第二次走缓存

    def test_custom_condition_lambda(self):
        """自定义条件：只缓存 status=='ok' 的结果。"""
        @self.c.cache(ttl=60, condition=lambda r: r is not None and r.get("status") == "ok")
        def get_order(order_id):
            self.call_count += 1
            return {"status": "ok", "id": order_id} if order_id > 0 else {"status": "fail"}

        get_order(1)  # ok，应缓存
        get_order(1)
        assert self.call_count == 1  # 命中缓存

        get_order(-1)  # fail，不缓存
        get_order(-1)
        assert self.call_count == 3  # 每次都执行

    def test_always_cache_including_none(self):
        """condition=lambda r: True：包括 None 都缓存。"""
        @self.c.cache(ttl=60, condition=lambda r: True)
        def might_none(x):
            self.call_count += 1
            return None

        might_none(1)
        might_none(1)
        assert self.call_count == 1  # None 也被缓存，第二次命中


# ============================================================
# failover 装饰器测试
# ============================================================

class TestFailover:
    def setup_method(self):
        self.c = Cache().setup("mem://")
        self.call_count = 0
        self.should_fail = False

    def test_failover_returns_cached_on_exception(self):
        """函数抛异常时，failover 返回缓存的旧值。"""
        @self.c.failover(ttl=60, exceptions=(ValueError,))
        def get_data(x):
            self.call_count += 1
            if self.should_fail:
                raise ValueError("fail")
            return x * 2

        assert get_data(5) == 10
        self.should_fail = True
        assert get_data(5) == 10  # 返回缓存旧值
        assert self.call_count == 2  # 第二次尝试执行了（异常后才回退）

    def test_failover_reraises_when_no_cache(self):
        """没有缓存时，failover 不能回退，直接抛出原始异常。"""
        @self.c.failover(ttl=60, exceptions=(RuntimeError,))
        def always_fail():
            raise RuntimeError("no cache available")

        with pytest.raises(RuntimeError):
            always_fail()

    def test_failover_async(self):
        """异步函数 failover。"""
        should_fail = [False]

        @self.c.failover(ttl=60, exceptions=(ValueError,))
        async def get_async(x):
            self.call_count += 1
            if should_fail[0]:
                raise ValueError("async fail")
            return x * 3

        async def _test():
            assert await get_async(4) == 12
            should_fail[0] = True
            assert await get_async(4) == 12  # 回退到缓存

        asyncio.get_event_loop().run_until_complete(_test())


# ============================================================
# early 装饰器测试
# ============================================================

class TestEarlyRefresh:
    def setup_method(self):
        self.c = Cache().setup("mem://")
        self.call_count = 0

    def test_early_basic_cache(self):
        """early 在 TTL 内正常命中缓存。"""
        @self.c.early(ttl=60, early_ttl=5)
        def get_data(x):
            self.call_count += 1
            return x * 2

        assert get_data(3) == 6
        assert self.call_count == 1
        assert get_data(3) == 6
        assert self.call_count == 1  # 命中缓存

    def test_early_cache_expires(self):
        """TTL 过期后重新计算。"""
        @self.c.early(ttl=0.2, early_ttl=0.05)
        def get_data(x):
            self.call_count += 1
            return x

        get_data(1)
        assert self.call_count == 1
        time.sleep(0.3)
        get_data(1)
        assert self.call_count == 2


# ============================================================
# soft 装饰器测试
# ============================================================

class TestSoftExpiry:
    def setup_method(self):
        self.c = Cache().setup("mem://")
        self.call_count = 0

    def test_soft_basic_cache(self):
        """soft_ttl 未到期时，直接返回缓存值。"""
        @self.c.soft(ttl=60, soft_ttl=30)
        def get_data(x):
            self.call_count += 1
            return x * 2

        assert get_data(5) == 10
        assert self.call_count == 1
        assert get_data(5) == 10
        assert self.call_count == 1


# ============================================================
# hit 装饰器测试
# ============================================================

class TestHitCache:
    def setup_method(self):
        self.c = Cache().setup("mem://")
        self.call_count = 0

    def test_hit_invalidates_after_n_hits(self):
        """cache_hits 次命中后自动失效，下次重新调用函数。"""
        @self.c.hit(ttl=60, cache_hits=2)
        def get_data(x):
            self.call_count += 1
            return x

        get_data(1)          # miss → 调用函数，缓存
        assert self.call_count == 1
        get_data(1)          # hit 1
        get_data(1)          # hit 2 → 达到上限，失效
        get_data(1)          # miss → 重新调用
        assert self.call_count == 2


# ============================================================
# circuit_breaker 装饰器测试
# ============================================================

class TestCircuitBreaker:
    def setup_method(self):
        self.c = Cache().setup("mem://")

    def test_circuit_breaker_opens_on_errors(self):
        """错误率超标后熔断，后续调用抛 CircuitBreakerOpen。"""
        call_count = [0]

        @self.c.circuit_breaker(errors_rate=0.5, period=10, ttl=5)
        def unstable():
            call_count[0] += 1
            raise RuntimeError("service error")

        # 多次调用触发熔断
        for _ in range(5):
            try:
                unstable()
            except (RuntimeError, CircuitBreakerOpen):
                pass

        # 熔断后应直接抛 CircuitBreakerOpen（不再调用函数）
        with pytest.raises(CircuitBreakerOpen):
            unstable()


# ============================================================
# slice_rate_limit 测试
# ============================================================

class TestSliceRateLimit:
    def setup_method(self):
        self.c = Cache().setup("mem://")

    def test_slice_rate_limit(self):
        """滑动窗口限流：超出限制后抛 RateLimitError。"""
        @self.c.slice_rate_limit(limit=3, period=60)
        def api_call():
            return "ok"

        assert api_call() == "ok"
        assert api_call() == "ok"
        assert api_call() == "ok"
        with pytest.raises(RateLimitError):
            api_call()


# ============================================================
# tags 标签批量失效测试
# ============================================================

class TestTags:
    def setup_method(self):
        self.c = Cache().setup("mem://")
        self.call_count = 0

    def test_delete_tags_invalidates_tagged_entries(self):
        """按标签批量删除后，下次调用重新执行函数。"""
        @self.c.cache(ttl=60, tags=("users",))
        def get_profile(user_id):
            self.call_count += 1
            return {"id": user_id}

        get_profile(1)
        get_profile(1)
        assert self.call_count == 1  # 命中缓存

        self.c.delete_tags_sync("users")

        get_profile(1)
        assert self.call_count == 2  # 标签失效，重新调用

    def test_delete_tags_multiple(self):
        """多个标签，删除其中一个标签使对应缓存失效。"""
        @self.c.cache(ttl=60, tags=("users", "profiles"))
        def get_user(user_id):
            self.call_count += 1
            return user_id

        get_user(10)
        assert self.call_count == 1

        self.c.delete_tags_sync("profiles")
        get_user(10)
        assert self.call_count == 2

    def test_delete_tags_async(self):
        """异步按标签删除。"""
        @self.c.cache(ttl=60, tags=("async_tag",))
        async def get_async(x):
            self.call_count += 1
            return x

        async def _test():
            await get_async(1)
            assert self.call_count == 1
            await self.c.delete_tags("async_tag")
            await get_async(1)
            assert self.call_count == 2

        asyncio.get_event_loop().run_until_complete(_test())


# ============================================================
# serializer 自定义序列化器测试
# ============================================================

class TestCustomSerializer:
    def setup_method(self):
        self.c = Cache().setup("mem://")
        self.call_count = 0

    def test_json_serializer(self):
        """使用 JsonSerializer 序列化，能正确存取 dict。"""
        from nb_cache.serialize import Serializer
        from nb_cache import JsonSerializer

        json_ser = Serializer(serializer=JsonSerializer())

        @self.c.cache(ttl=60, serializer=json_ser)
        def get_data(key):
            self.call_count += 1
            return {"key": key, "value": 42}

        r1 = get_data("x")
        assert r1 == {"key": "x", "value": 42}
        r2 = get_data("x")
        assert r2 == {"key": "x", "value": 42}
        assert self.call_count == 1  # 命中缓存


# ============================================================
# prefix 叠加测试
# ============================================================

class TestPrefix:
    def test_decorator_prefix_isolated_from_no_prefix(self):
        """装饰器级别 prefix 与无 prefix 的 key 互相隔离。"""
        c = Cache().setup("mem://")
        call_count = [0]

        @c.cache(ttl=60, prefix="v1")
        def get_data(x):
            call_count[0] += 1
            return x

        @c.cache(ttl=60, prefix="v2")
        def get_data_v2(x):
            call_count[0] += 1
            return x * 10

        get_data(1)    # key 含 v1 前缀
        get_data_v2(1) # key 含 v2 前缀，与上面不同
        assert call_count[0] == 2  # 两个都未命中对方缓存

    def test_setup_prefix_applied(self):
        """setup(prefix=...) 全局前缀：两个不同 prefix 的实例数据隔离。"""
        c1 = Cache().setup("mem://", prefix="app1")
        c2 = Cache().setup("mem://", prefix="app2")

        c1.set_sync("key", "from_app1")
        assert c2.get_sync("key") is None  # 前缀不同，隔离


# ============================================================
# TTL 多种格式测试（通过装饰器验证）
# ============================================================

class TestTTLFormats:
    def setup_method(self):
        self.c = Cache().setup("mem://")

    def test_ttl_int(self):
        @self.c.cache(ttl=60)
        def fn():
            return 1
        assert fn() == 1

    def test_ttl_float(self):
        @self.c.cache(ttl=0.5)
        def fn():
            return 2
        assert fn() == 2

    def test_ttl_string_seconds(self):
        @self.c.cache(ttl="10s")
        def fn():
            return 3
        assert fn() == 3

    def test_ttl_string_minutes(self):
        @self.c.cache(ttl="5m")
        def fn():
            return 4
        assert fn() == 4

    def test_ttl_string_hours(self):
        @self.c.cache(ttl="1h")
        def fn():
            return 5
        assert fn() == 5

    def test_ttl_string_combined(self):
        @self.c.cache(ttl="1h30m")
        def fn():
            return 6
        assert fn() == 6

    def test_ttl_timedelta(self):
        from datetime import timedelta

        @self.c.cache(ttl=timedelta(minutes=10))
        def fn():
            return 7
        assert fn() == 7


# ============================================================
# iterator 异步生成器测试
# ============================================================

class TestIteratorAsync:
    def setup_method(self):
        self.c = Cache().setup("mem://")
        self.call_count = 0

    def test_async_generator_cache(self):
        """异步生成器缓存：第二次调用从缓存还原，不重新执行。"""
        @self.c.iterator(ttl=60)
        async def gen_items():
            self.call_count += 1
            for i in range(3):
                yield i

        async def _collect(gen):
            return [x async for x in gen()]

        async def _test():
            r1 = await _collect(gen_items)
            assert r1 == [0, 1, 2]
            assert self.call_count == 1

            r2 = await _collect(gen_items)
            assert r2 == [0, 1, 2]
            assert self.call_count == 1  # 命中缓存

        asyncio.get_event_loop().run_until_complete(_test())


# ============================================================
# lock 上下文管理器测试（补充 async）
# ============================================================

class TestLockContextManagerAsync:
    def setup_method(self):
        self.c = Cache().setup("mem://")

    def test_async_lock_exclusion(self):
        """异步锁：持锁期间 is_locked 为 True，释放后为 False。"""
        async def _test():
            async with self.c.alock("res2", ttl=5):
                assert await self.c.backend.is_locked("res2") is True
            assert await self.c.backend.is_locked("res2") is False

        asyncio.get_event_loop().run_until_complete(_test())


if __name__ == '__main__':
    pytest.main([__file__, '-v'])


