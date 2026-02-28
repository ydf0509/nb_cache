# -*- coding: utf-8 -*-
"""nb_cache key 生成 Demo"""
import sys
import asyncio
from nb_cache import Cache
from nb_cache.key import get_cache_key, get_cache_key_template
import nb_log

nb_log.get_logger("nb_cache.cache")

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# 默认：key 含模块路径+函数名
cache = Cache()
cache.setup("redis://", prefix="testp2")

@cache.cache(ttl=700, key='sf:{a}_{b}')
def simple_func(a, b):
    return a + b

print(simple_func(1, 2))  # key: testp2:__main__:simple_func:sf:1_2

# include_func_name=False：key 只保留业务部分
cache2 = Cache()
cache2.setup("redis://", prefix="testp2", key_include_func=False)

@cache2.cache(ttl=700, key='aiof:{x}_{y}')
async def aio_fun(x, y):
    return x + y

print(asyncio.run(aio_fun(3, 4)))  # key: testp2:aiof:3_4

# 也可以在单个装饰器上覆盖
@cache.cache(ttl=700, key='sf2:{a}_{b}', key_include_func=False)
def simple_func2(a, b):
    return a + b

print(simple_func2(5, 6))  # key: testp2:sf2:5_6

"""
2026-02-28 18:46:01 - nb_cache.cache - "D:\codes\nb_cache\nb_cache\decorators\cache.py:61" - async_wrapper - DEBUG - [nb_cache] func=__main__:aio_fun  final_key=testp2:__main__:aio_fun:aiof:3_4  ttl=700.0

2026-02-28 18:46:01  "d:\codes\nb_cache\tests\my_tests\test_key.py:29" -<module>-[print]-  7 
"""