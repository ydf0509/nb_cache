# -*- coding: utf-8 -*-
"""nb_cache 熔断器 Demo"""
import sys
import time
import asyncio
from nb_cache import Cache
from nb_cache.exceptions import CircuitBreakerOpen

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

cache = Cache()
cache.setup("redis://")

# cache._backend.delete_sync("circuit_breaker:*")

print("=" * 50)
print("Demo 1: 基础熔断器 - 错误率超过阈值触发熔断")
print("=" * 50)

call_count = 0

@cache.circuit_breaker(errors_rate=0.5, period=10, ttl=5,
                       min_calls=5
                       )
def unstable_service():
    global call_count
    call_count += 1
    if call_count % 2 == 0:
        raise Exception("服务异常")
    return "服务正常"

for i in range(10):
    try:
        result = unstable_service()
        print(f"第 {i+1} 次调用：{result}")
    except CircuitBreakerOpen as e:
        print(f"第 {i+1} 次调用：熔断器打开 - {e}")
    except Exception as e:
        print(f"第 {i+1} 次调用：服务异常 - {e}")

print("\n等待 5 秒，熔断器自动恢复...")
time.sleep(5)

for i in range(3):
    try:
        result = unstable_service()
        print(f"恢复后第 {i+1} 次调用：{result}")
    except CircuitBreakerOpen as e:
        print(f"恢复后第 {i+1} 次调用：熔断器打开 - {e}")
    except Exception as e:
        print(f"恢复后第 {i+1} 次调用：服务异常 - {e}")


print("\n" + "=" * 50)
print("Demo 2: 指定异常类型 - 只捕获特定异常")
print("=" * 50)

call_count2 = 0

@cache.circuit_breaker(errors_rate=0.5, period=10, ttl=5, exceptions=(ValueError,))
def service_with_specific_exception():
    global call_count2
    call_count2 += 1
    if call_count2 % 3 == 0:
        raise ValueError("值错误")
    elif call_count2 % 5 == 0:
        raise RuntimeError("运行时错误（不触发熔断）")
    return "成功"

for i in range(10):
    try:
        result = service_with_specific_exception()
        print(f"第 {i+1} 次调用：{result}")
    except CircuitBreakerOpen as e:
        print(f"第 {i+1} 次调用：熔断器打开 - {e}")
    except Exception as e:
        print(f"第 {i+1} 次调用：异常 - {type(e).__name__}: {e}")


print("\n" + "=" * 50)
print("Demo 3: 异步函数熔断")
print("=" * 50)

async_call_count = 0

@cache.circuit_breaker(errors_rate=0.5, period=10, ttl=5,)
async def async_unstable_service():
    global async_call_count
    async_call_count += 1
    await asyncio.sleep(0.1)
    if async_call_count % 2 == 0:
        raise Exception("异步服务异常")
    return "异步服务正常"

async def test_async():
    for i in range(10):
        try:
            result = await async_unstable_service()
            print(f"异步第 {i+1} 次调用：{result}")
        except CircuitBreakerOpen as e:
            print(f"异步第 {i+1} 次调用：熔断器打开 - {e}")
        except Exception as e:
            print(f"异步第 {i+1} 次调用：服务异常 - {e}")
    
    print("\n等待 5 秒，异步熔断器自动恢复...")
    await asyncio.sleep(5)
    
    for i in range(3):
        try:
            result = await async_unstable_service()
            print(f"异步恢复后第 {i+1} 次调用：{result}")
        except CircuitBreakerOpen as e:
            print(f"异步恢复后第 {i+1} 次调用：熔断器打开 - {e}")
        except Exception as e:
            print(f"异步恢复后第 {i+1} 次调用：服务异常 - {e}")

asyncio.run(test_async())


print("\n" + "=" * 50)
print("Demo 4: 自定义最小调用次数")
print("=" * 50)

call_count3 = 0

@cache.circuit_breaker(errors_rate=0.5, period=10, ttl=5, min_calls=3)
def service_with_min_calls():
    global call_count3
    call_count3 += 1
    if call_count3 <= 2:
        raise Exception("前两次调用失败")
    return "第三次开始成功"

for i in range(6):
    try:
        result = service_with_min_calls()
        print(f"第 {i+1} 次调用：{result}")
    except CircuitBreakerOpen as e:
        print(f"第 {i+1} 次调用：熔断器打开 - {e}")
    except Exception as e:
        print(f"第 {i+1} 次调用：异常 - {e}")


print("\n" + "=" * 50)
print("所有 Demo 执行完成!")
print("=" * 50)
