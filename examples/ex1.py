# -*- coding: utf-8 -*-
"""nb_cache 综合用法演示

体现:
  1. 内存缓存 / Redis缓存 / Redis+内存双缓存
  2. 同步函数 / 异步函数 共用同一装饰器
  3. 简单自定义 key / 复杂函数参数自动生成 key / hash格式key
"""
import asyncio
import time
import logging
from nb_cache import Cache

# import nb_log
# nb_log.get_logger('nb_cache')
logger = logging.getLogger("nb_cache.key")
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

# ================================================================
#  一、内存缓存（mem://）
# ================================================================
mem_cache = Cache().setup("mem://")

# ---------- 同步函数 + 自动生成 key ----------
@mem_cache.cache(ttl=10)
def get_user_from_db(user_id):
    """不指定 key，自动用 函数全名:user_id=xxx 作为缓存 key"""
    print("  [MEM][SYNC] 查询数据库 user_id={}".format(user_id))
    return {"id": user_id, "name": "张三"}


# ---------- 异步函数 + 简单自定义 key ----------
@mem_cache.cache(ttl=10, key="{user_id}")
async def get_user_async(user_id):
    """用 {user_id} 模板，缓存 key 就是 user_id 的值本身（如 '1001'）"""
    print("  [MEM][ASYNC] 异步查询 user_id={}".format(user_id))
    await asyncio.sleep(0.01)
    return {"id": user_id, "name": "李四"}


# ---------- 同步函数 + 复杂自定义 key（hash） ----------
@mem_cache.cache(ttl=10, key="order:{user_id}:{filters:hash}")
def query_orders(user_id, filters, page=1):
    """复杂 key: filters 是 dict，用 :hash 格式取 md5 前8位，避免 key 过长"""
    print("  [MEM][SYNC] 查询订单 user_id={} filters={} page={}".format(user_id, filters, page))
    return [{"order": 1001, "amount": 99.9}]


# ---------- 异步函数 + lock 防缓存击穿 ----------
@mem_cache.cache(ttl=10, lock=True)
async def get_hot_ranking():
    """热点数据加锁，并发请求时只有一个真正执行查询"""
    print("  [MEM][ASYNC][LOCK] 计算热门排行榜...")
    await asyncio.sleep(0.01)
    return ["item_a", "item_b", "item_c"]


# ================================================================
#  二、Redis 缓存（redis://）
# ================================================================
redis_ok = False
try:
    redis_cache = Cache().setup("redis://localhost:6379/0")
    redis_cache.ping_sync()
    redis_ok = True
    print("[OK] Redis 已连接\n")
except Exception as e:
    print("[SKIP] Redis 不可用 ({}), 跳过 Redis 示例\n".format(e))


# ---------- 同步函数 + 自动 key ----------
@redis_cache.cache(ttl=30)
def get_product_sync(product_id):
    print("  [REDIS][SYNC] 查询商品 product_id={}".format(product_id))
    return {"id": product_id, "title": "MacBook Pro", "price": 14999}


# ---------- 异步函数 + 简单自定义 key ----------
@redis_cache.cache(ttl=30, key="prod:{product_id}")
async def get_product_async(product_id):
    print("  [REDIS][ASYNC] 异步查询商品 product_id={}".format(product_id))
    await asyncio.sleep(0.01)
    return {"id": product_id, "title": "iPhone", "price": 7999}


# ---------- 同步 + 复杂 key + lock ----------
@redis_cache.cache(ttl=60, key="search:{keyword:lower}:{options:hash}", lock=True)
def search_products(keyword, options):
    """keyword 转小写做 key, options 用 hash, 加锁防击穿"""
    print("  [REDIS][SYNC][LOCK] 搜索: keyword={} options={}".format(keyword, options))
    return [{"id": 1, "title": keyword}]

# ----- 同步 + key 模板支持点号访问字典/对象属性 ----------
# {user.id} 和 {user.name} 会自动从 user 参数里取 user['id'] 和 user['name']
@redis_cache.cache(ttl=60, key="user:{user.id}:{user.name}")
def get_user(user):
    print("  [REDIS][SYNC] 查询用户 user.id={} user.name={}".format(user['id'], user['name']))
    return {"id": user['id'], "name": user['name']}


# ---- 异步 + key 通过自定义函数生成（适合复杂逻辑，无法用模板表达的情况）----------
def gen_user_key(user):
    """自定义 key 生成函数，接收和被装饰函数相同的参数，返回 key 字符串"""
    return "user:{}:{}".format(user['id'], user['name'])

@redis_cache.cache(ttl=60, key=gen_user_key)
async def get_user_async_gen_key(user):
    print("  [REDIS][ASYNC] 查询用户 user.id={} user.name={}".format(user['id'], user['name']))
    await asyncio.sleep(0.01)
    return {"id": user['id'], "name": user['name']}

# ================================================================
#  三、Redis + 内存双缓存（dual://）
# ================================================================
dual_ok = False
try:
    dual_cache = Cache().setup("dual://localhost:6379/0?memory_size=500&local_ttl=5")
    dual_cache.ping_sync()
    dual_ok = True
    print("[OK] Dual (Redis+内存) 已连接\n")
except Exception as e:
    print("[SKIP] Dual 不可用 ({}), 跳过双缓存示例\n".format(e))


# ---------- 同步 + 自动 key ----------
@dual_cache.cache(ttl=60)
def get_config_sync(config_key):
    """双缓存: 先查内存(~0ms)，miss则查Redis(~1ms)，写入时双写"""
    print("  [DUAL][SYNC] 读取配置 config_key={}".format(config_key))
    return {"key": config_key, "value": "some_config_value"}


# ---------- 异步 + 复杂 key ----------
@dual_cache.cache(ttl=60, key="session:{user_id}:{token:hash}")
async def get_session_async(user_id, token):
    print("  [DUAL][ASYNC] 查询会话 user_id={} token={}...".format(user_id, token[:8]))
    await asyncio.sleep(0.01)
    return {"user_id": user_id, "role": "admin", "login_time": "2026-02-28 10:00"}


# ================================================================
#  运行演示
# ================================================================
def demo_memory():
    print("=" * 60)
    print("【内存缓存】")
    print("=" * 60)

    print("\n--- 同步函数 (自动生成 key) ---")
    r1 = get_user_from_db(1001)
    print("  结果:", r1)
    r2 = get_user_from_db(1001)
    print("  结果(应走缓存，无SQL打印):", r2)
    r3 = get_user_from_db(1002)
    print("  结果(不同参数，重新查询):", r3)

    print("\n--- 同步函数 (复杂 key: hash格式) ---")
    f1 = {"status": "paid", "min_amount": 100}
    r4 = query_orders(1001, f1, page=1)
    print("  结果:", r4)
    r5 = query_orders(1001, f1, page=1)
    print("  结果(缓存命中):", r5)
    f2 = {"status": "shipped"}
    r6 = query_orders(1001, f2, page=1)
    print("  结果(filters不同，hash不同，重新查询):", r6)


async def demo_memory_async():
    print("\n--- 异步函数 (简单自定义 key: {user_id}) ---")
    r1 = await get_user_async(2001)
    print("  结果:", r1)
    r2 = await get_user_async(2001)
    print("  结果(缓存命中):", r2)

    print("\n--- 异步函数 (lock防击穿) ---")
    r3 = await get_hot_ranking()
    print("  结果:", r3)
    r4 = await get_hot_ranking()
    print("  结果(缓存命中):", r4)

    print("\n--- 异步函数 (自定义函数生成key) ---")
    r5 = await get_user_async_gen_key({"id": 2001, "name": "张三","description": "这是一个用户"})
    print("  结果:", r5)
    r6 = await get_user_async_gen_key({"id": 2001, "name": "张三","description": "这是一个用户"})
    print("  结果(缓存命中):", r6)

    print("\n--- 异步函数 (通过点号 . 访问一个对象或者字典的属性) ---")
    r7 =  get_user({"id": 2001, "name": "张三","description": "这是一个用户"})
    print("  结果:", r7)
    r8 =  get_user({"id": 2001, "name": "张三","description": "这是一个用户" , "age": 18})
    print("  结果(缓存命中):", r8)


def demo_redis():
    if not redis_ok:
        return
    print("\n" + "=" * 60)
    print("【Redis缓存】")
    print("=" * 60)

    print("\n--- 同步函数 (自动 key) ---")
    r1 = get_product_sync(5001)
    print("  结果:", r1)
    r2 = get_product_sync(5001)
    print("  结果(缓存命中):", r2)

    print("\n--- 同步函数 (复杂 key + lock) ---")
    opts = {"category": "electronics", "sort": "price_asc"}
    r3 = search_products("Laptop", opts)
    print("  结果:", r3)
    r4 = search_products("Laptop", opts)
    print("  结果(缓存命中):", r4)
    r5 = search_products("LAPTOP", opts)
    print("  结果(keyword转lower相同，缓存命中):", r5)


async def demo_redis_async():
    if not redis_ok:
        return
    print("\n--- 异步函数 (简单自定义 key) ---")
    r1 = await get_product_async(6001)
    print("  结果:", r1)
    r2 = await get_product_async(6001)
    print("  结果(缓存命中):", r2)


def demo_dual():
    if not dual_ok:
        return
    print("\n" + "=" * 60)
    print("【Redis+内存 双缓存】")
    print("=" * 60)

    print("\n--- 同步函数 (自动 key) ---")
    r1 = get_config_sync("app.max_retry")
    print("  结果:", r1)
    print("  第2次(内存L1命中):")
    r2 = get_config_sync("app.max_retry")
    print("  结果:", r2)


async def demo_dual_async():
    if not dual_ok:
        return
    print("\n--- 异步函数 (复杂 key: hash格式) ---")
    token = "abc123def456ghi789jkl"
    r1 = await get_session_async(9001, token)
    print("  结果:", r1)
    r2 = await get_session_async(9001, token)
    print("  结果(双缓存命中):", r2)


def main():
    demo_memory()
    asyncio.get_event_loop().run_until_complete(demo_memory_async())
    demo_redis()
    asyncio.get_event_loop().run_until_complete(demo_redis_async())
    demo_dual()
    asyncio.get_event_loop().run_until_complete(demo_dual_async())

    print("\n" + "=" * 60)
    print("演示完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()
