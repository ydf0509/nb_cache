# -*- coding: utf-8 -*-
"""nb_cache 限流 Demo"""
import time
import asyncio
from nb_cache import Cache

import sys
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

cache = Cache()
cache.setup("redis://")

print("=" * 50)
print("Demo 1: 固定窗口限流 - 同步函数")
print("=" * 50)

@cache.rate_limit(limit=3, period=10)
def api_sync():
    return "success"

for i in range(5):
    try:
        result = api_sync()
        print(f"第 {i+1} 次调用: {result}")
    except Exception as e:
        print(f"第 {i+1} 次调用: 限流触发 - {e}")

print("\n等待 10 秒后限流重置...")
time.sleep(10)

for i in range(3):
    try:
        result = api_sync()
        print(f"等待后第 {i+1} 次调用: {result}")
    except Exception as e:
        print(f"等待后第 {i+1} 次调用: 限流触发 - {e}")


print("\n" + "=" * 50)
print("Demo 2: 固定窗口限流 - 按用户ID限流")
print("=" * 50)

@cache.rate_limit(limit=2, period=10, key="{user_id}")
def get_user_info_sync(user_id):
    return f"用户 {user_id} 的信息"

for user_id in ["user1", "user1", "user1", "user2"]:
    try:
        result = get_user_info_sync(user_id)
        print(f"用户 {user_id}: {result}")
    except Exception as e:
        print(f"用户 {user_id}: 限流触发")


print("\n" + "=" * 50)
print("Demo 3: 滑动窗口限流 - 更平滑的限流")
print("=" * 50)

@cache.slice_rate_limit(limit=3, period=10)
def api_slice_sync():
    return "success"

for i in range(6):
    try:
        result = api_slice_sync()
        print(f"第 {i+1} 次调用: {result}")
    except Exception as e:
        print(f"第 {i+1} 次调用: 限流触发 - {e}")


print("\n" + "=" * 50)
print("Demo 4: 异步函数限流")
print("=" * 50)

@cache.rate_limit(limit=2, period=10)
async def api_async():
    await asyncio.sleep(0.1)
    return "async success"

async def test_async():
    for i in range(4):
        try:
            result = await api_async()
            print(f"异步第 {i+1} 次调用: {result}")
        except Exception as e:
            print(f"异步第 {i+1} 次调用: 限流触发 - {e}")

asyncio.run(test_async())


print("\n" + "=" * 50)
print("Demo 5: 自定义 action 回调处理限流")
print("=" * 50)

def on_rate_limit(*args, **kwargs):
    return "自定义返回: 系统繁忙，请稍后再试"

@cache.rate_limit(limit=2, period=10, action=on_rate_limit)
def api_with_action():
    return "正常处理"

for i in range(4):
    result = api_with_action()
    print(f"第 {i+1} 次调用: {result}")


print("\n" + "=" * 50)
print("所有 Demo 执行完成!")
print("=" * 50)
