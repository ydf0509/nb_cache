# -*- coding: utf-8 -*-
"""
测试 Cache 实例隔离性。

设计原则：每个 Cache() 实例拥有独立的后端，setup() 只影响当前实例，
不再存在任何全局默认后端共享状态。用户必须显式实例化 Cache 并调用 setup()。
"""

import pytest
from nb_cache import Cache
from nb_cache.backends.memory import MemoryBackend


class TestCacheInstanceIsolation:

    def test_two_instances_have_separate_backends(self):
        """两个独立实例的后端对象不是同一个。"""
        c1 = Cache()
        c1.setup("mem://")

        c2 = Cache()
        c2.setup("mem://")

        assert c1.backend is not c2.backend

    def test_data_not_shared_between_instances(self):
        """一个实例写入的数据对另一个实例不可见。"""
        c1 = Cache()
        c1.setup("mem://")

        c2 = Cache()
        c2.setup("mem://")

        c1.set_sync("shared_key", "from_c1")

        assert c2.get_sync("shared_key") is None

    def test_setup_does_not_affect_other_instance(self):
        """c1 setup 之后再对 c2 setup，c1 的后端不变。"""
        c1 = Cache()
        c1.setup("mem://")
        backend_c1 = c1.backend

        c2 = Cache()
        c2.setup("mem://")

        assert c1.backend is backend_c1

    def test_each_instance_tracks_own_backend_type(self):
        """每个实例独立持有 MemoryBackend，类型正确。"""
        c1 = Cache()
        c1.setup("mem://")

        c2 = Cache()
        c2.setup("mem://")

        assert isinstance(c1.backend, MemoryBackend)
        assert isinstance(c2.backend, MemoryBackend)

    def test_decorator_bound_to_instance(self):
        """装饰器绑定到对应实例，不同实例的缓存互不干扰。"""
        c1 = Cache()
        c1.setup("mem://")

        c2 = Cache()
        c2.setup("mem://")

        call_count_c1 = [0]
        call_count_c2 = [0]

        @c1.cache(ttl=60)
        def fn_via_c1(x):
            call_count_c1[0] += 1
            return x * 10

        @c2.cache(ttl=60)
        def fn_via_c2(x):
            call_count_c2[0] += 1
            return x * 20

        fn_via_c1(1)
        fn_via_c1(1)  # 应走 c1 缓存
        assert call_count_c1[0] == 1

        fn_via_c2(1)
        fn_via_c2(1)  # 应走 c2 缓存
        assert call_count_c2[0] == 1

    def test_clear_one_instance_does_not_affect_other(self):
        """清空 c1 不影响 c2 的数据。"""
        c1 = Cache()
        c1.setup("mem://")

        c2 = Cache()
        c2.setup("mem://")

        c1.set_sync("key", "v1")
        c2.set_sync("key", "v2")

        c1.clear_sync()

        assert c1.get_sync("key") is None
        assert c2.get_sync("key") == "v2"
