
# 🚀 nb_cache: Python 缓存界的“瑞士军刀”

[![Python Versions](https://img.shields.io/badge/python-3.6+-blue.svg)](https://pypi.org/project/nb-cache/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> **`nb_cache` 不仅是一个基础的缓存装饰器，它在彻底抹平 Python 同步与异步代码差异的同时，开箱即用地提供了内存/Redis双层缓存、防击穿、防雪崩、限流与熔断等企业级高可用特性。**

在如今混杂着 Sync 和 Async 的 Python 项目中，开发者往往需要为同步代码和异步代码寻找不同的缓存解决方案。`nb_cache` 彻底抹平了这一差异——**只需同一个装饰器，即可完美兼容同步与异步函数**。

如果你曾经使用过 `cashews`，你会对 `nb_cache` 感到非常亲切。`nb_cache` 吸收了其优秀的特性，并弥补了其最大的短板：**全面支持同/异步场景，且所有底层操作兼具 Sync 和 Async 两种 API**。

## ✨ 核心特性

- ☯️ **同/异步无缝统一**：无需区分 `@cache` 或 `@acache`，同一个装饰器自动识别普通函数与协程，内部自动路由。同一套上下文管理器同时支持 `with` 和 `async with`。
- 🚀 **丰富的高级后端**：
  - **Memory (`mem://`)**: 极速的本地 LRU 内存缓存。
  - **Redis (`redis://`)**: 分布式 Redis 缓存，支持连接池。
  - **双层缓存 (`dual://`)**: **(杀手级特性)** 本地内存(L1) + Redis(L2) 的透明双重缓存。读取时优先击中内存，写入时双写，彻底释放 Redis 压力。
- 🛡️ **企业级高可用防护**：
  - **防缓存击穿 (Stampede)**：只需一个参数 `lock=True`，即可在并发未命中时让多余请求等待，只放行一个请求去查库。
  - **防缓存雪崩 (Avalanche)**：提供 `@cache.early` (提前后台刷新) 和 `@cache.soft` (软过期，返回旧值并异步刷新) 完美解决雪崩。
  - **服务降级与失败回退**：提供 `@cache.failover`，当数据库或下游接口挂掉时，自动返回缓存的旧值兜底。
- 🔧 **极其丰富的“微服务”级装饰器**：除了缓存，还内置了**限流** (`rate_limit`, `slice_rate_limit`)、**熔断** (`circuit_breaker`)、**并发防抖** (`thunder_protection`)、**布隆过滤器** (`bloom`, `dual_bloom`)。
- 🔑 **智能 Key 路由与模板**：告别繁琐的 key 拼接。支持 `{user_id}`、`{user.name}` (直接读取对象属性)、`{data:hash}` (自动对大字典算md5) 等高级格式化模板。
- 🔒 **数据安全与压缩**：自带序列化流水线。一行配置即可开启 JSON/Pickle 序列化、Gzip/Zlib 压缩，以及 HMAC 签名（防止缓存数据被恶意篡改）。
- 🏷️ **标签系统与事务**：支持给缓存打标签 (Tags) 实现按业务模块批量失效，支持类似数据库的缓存事务 (Transaction) 自动回滚。

## 💡 为什么选择 nb_cache？

| 特性 | 传统自带 `lru_cache` | `redis-py` 原生 | `cashews` | 🏆 `nb_cache` |
| :--- | :---: | :---: | :---: | :---: |
| **同步函数支持** | ✅ | ✅ | ❌ | **✅ 完美支持** |
| **异步函数 (Asyncio) 支持** | ❌ | ✅ | ✅ | **✅ 完美支持** |
| **内存/Redis 双重缓存** | ❌ | ❌ | ✅ | **✅ 开箱即用** |
| **防击穿 (分布式锁合并请求)** | ❌ | 需手写代码 | ✅ | **✅ `lock=True`** |
| **防雪崩/后台自动刷新** | ❌ | 需手写代码 | ✅ | **✅ `@cache.early`** |
| **限流与熔断** | ❌ | 需手写代码 | ✅ | **✅ 内置支持** |

---

### 👉 接下来，请看下方的【安装】与【快速开始】，体验一行代码带来的架构升级：

## 安装

```bash
pip install nb_cache

# 使用 Redis 后端
pip install nb_cache[redis]

# 全部可选依赖
pip install nb_cache[all]
```

## 快速开始

`setup()` 会自动完成初始化，无需手动调用 `init_sync()`。

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

# 同步函数缓存
@cache.cache(ttl=60)
def get_user(user_id):
    return db.query(user_id)

# 异步函数缓存（同一个装饰器，自动识别）
@cache.cache(ttl="1h")
async def get_user_async(user_id):
    return await db.query_async(user_id)

# 加锁防止缓存击穿
@cache.cache(ttl=60, lock=True)
def get_hot_data(key):
    return expensive_query(key)
```

## 对比 cashews

如果你不懂 `nb_cache` 用法，可以参考 `cashews` 的用法。ai很熟练 `cashews`的用法。

`nb_cache` 对比 `cashews` 的优点：

- 装饰器支持同步和异步函数，通过同一个装饰器来支持。
- with 支持同步和异步上下文管理器。
- 所有操作支持asyncio和同步

`cashews` 最大缺点是 不支持同步，只能用于asyncio异步场景。



## 后端配置

### 内存缓存

```python
from nb_cache import Cache

cache = Cache().setup("mem://")
```

### Redis 缓存

```python
from nb_cache import Cache

cache = Cache().setup("redis://localhost:6379/0")
```

### Redis + 内存双缓存

```python
from nb_cache import Cache

# memory_size=1000: 内存最多存 1000 条; local_ttl=30: 内存层 TTL 30秒
cache = Cache().setup("dual://localhost:6379/0?memory_size=1000&local_ttl=30")
```

双缓存策略：读取时先查内存(L1)，miss 则查 Redis(L2) 并回填内存；写入时双写。

### 多实例

每个 `Cache()` 实例独立，可以配置不同后端：

```python
from nb_cache import Cache

mem_cache = Cache().setup("mem://")
redis_cache = Cache().setup("redis://localhost:6379/0")

@mem_cache.cache(ttl=60)
def local_data(key):
    ...

@redis_cache.cache(ttl=300)
def shared_data(key):
    ...
```

### setup() 参数详细介绍

`cache.setup(settings_url, middlewares=None, prefix="", **kwargs)` 是唯一需要调用的初始化方法。

#### 基本参数

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `settings_url` | str | 必填 | 后端连接 URL，决定使用哪种后端，见下文 |
| `middlewares` | list | `None` | 中间件列表，见 [中间件](#中间件) 章节 |
| `prefix` | str | `""` | 所有缓存 key 统一加前缀，方便多项目共用同一 Redis |

#### 序列化相关 kwargs（所有后端通用）

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `pickle_type` | str | `"pickle"` | 序列化方式：`"pickle"`（默认）或 `"json"` |
| `secret` | str | `""` | HMAC 签名密钥。设置后数据写入时签名，读取时校验，防止篡改 |
| `digestmod` | str | `"md5"` | 签名算法：`"md5"` / `"sha1"` / `"sha256"` |
| `compress_type` | str | `None` | 压缩方式：`None`（不压缩）/ `"gzip"` / `"zlib"` |

```python
from nb_cache import Cache

# 使用 JSON 序列化 + gzip 压缩 + HMAC 签名
cache = Cache().setup(
    "redis://localhost:6379/0",
    pickle_type="json",
    compress_type="gzip",
    secret="my-secret-key",
    digestmod="sha256",
)
```

#### 内存后端（`mem://`）专属 kwargs

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `size` | int | `0` | 最大缓存条数（LRU 淘汰）。`0` 表示不限 |
| `check_interval` | int | `60` | 被动过期清理的间隔秒数（仅影响后台清扫频率，不影响实时过期） |

```python
from nb_cache import Cache

# 最多 5000 条，每 30 秒清扫一次过期 key
cache = Cache().setup("mem://", size=5000, check_interval=30)
```

也可以通过 URL Query String 传递：

```python
cache.setup("mem://?size=5000&check_interval=30")
```

#### Redis 后端（`redis://` / `rediss://`）专属 kwargs

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `host` | str | `"localhost"` | Redis 主机（URL 中已包含时无需重复传） |
| `port` | int | `6379` | Redis 端口 |
| `db` | int | `0` | Redis 数据库编号（0–15） |
| `password` | str | `None` | Redis 认证密码 |
| `socket_timeout` | float | `None` | Socket 超时秒数，超时后抛出异常 |
| `max_connections` | int | `None` | 连接池最大连接数，`None` 表示不限 |
| `prefix` | str | `""` | Key 前缀（优先使用 `setup()` 的 `prefix` 参数） |

URL 中已经能表达 host、port、db、password，推荐优先用 URL：

```python
from nb_cache import Cache

# 等效写法一：URL 方式（推荐）
cache = Cache().setup("redis://:mypassword@192.168.1.100:6380/2")

# 等效写法二：URL + kwargs 混用（kwargs 会覆盖 URL 中的同名参数）
cache.setup("redis://192.168.1.100", port=6380, db=2, password="mypassword")

# 设置超时 + 连接池
cache.setup(
    "redis://localhost:6379/0",
    socket_timeout=2.0,
    max_connections=50,
)

# TLS 加密连接（rediss://）
cache.setup("rediss://localhost:6380/0", password="mypassword")

# 全局 key 前缀（同一个 Redis 服务不同项目隔离）
cache.setup("redis://localhost:6379/0", prefix="myapp:prod")
```

Query String 写法同样支持：

```python
cache.setup("redis://localhost:6379/0?socket_timeout=2&max_connections=50&prefix=myapp")
```

#### Redis+内存双缓存（`dual://`）专属 kwargs

在 Redis 参数基础上，额外支持：

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `memory_size` | int | `1000` | 内存层（L1）最大缓存条数 |
| `local_ttl` | float | `None` | 内存层统一 TTL（秒）。`None` 则与 Redis 层相同。建议设置较短（如 `5`~`30`），避免内存数据过期延迟 |

```python
from nb_cache import Cache

# 内存最多 2000 条，内存层 key 最多存 10 秒（Redis 层保持装饰器上的 ttl）
cache = Cache().setup(
    "dual://localhost:6379/0",
    memory_size=2000,
    local_ttl=10,
    max_connections=100,
    prefix="myapp",
)

# 等效 URL 写法
cache.setup("dual://localhost:6379/0?memory_size=2000&local_ttl=10&max_connections=100&prefix=myapp")
```

> **local_ttl 的作用**：双缓存场景下，数据先写 Redis，再写内存。若不设 `local_ttl`，内存层 TTL 与 Redis 相同，有可能造成内存里存放了大量长期未访问的数据。设置较短的 `local_ttl`（如 30 秒）可以让内存层快速自动清理冷数据，只保留热点。

## 缓存装饰器

### 基础缓存 `cache`

```python
from nb_cache import Cache, NOT_NONE

cache = Cache().setup("mem://")

# 自动生成 key（函数全名 + 参数）
@cache.cache(ttl=60)
def get_data(key):
    return query(key)

# 缓存条件：只缓存非 None 结果
@cache.cache(ttl=60, condition=NOT_NONE)
def get_data(key):
    return might_return_none(key)
```

#### cache() 参数详细介绍

`Cache().cache(ttl, key=None, condition=None, prefix="", lock=False, lock_ttl=None, tags=(), serializer=None)`

| 参数 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `ttl` | int / float / str / timedelta | 必填 | 缓存过期时间，支持多种格式，见 [TTL 格式](#ttl-格式) |
| `key` | str / callable | `None` | 缓存 key 模板或生成函数，`None` 时自动从函数名+参数生成 |
| `condition` | callable | `None`（等同 `NOT_NONE`） | 决定结果是否缓存的条件函数，见下文 |
| `prefix` | str | `""` | 附加到 key 前的额外前缀，与 `setup(prefix=...)` 叠加 |
| `lock` | bool | `False` | 是否开启分布式锁防缓存击穿，见下文 |
| `lock_ttl` | int / float / str | `None`（等同 `ttl`） | 锁的超时时间，默认与 `ttl` 相同 |
| `tags` | tuple[str] | `()` | 缓存标签，用于 `delete_tags_sync()` 按标签批量失效 |
| `serializer` | Serializer | `None`（用全局默认） | 自定义序列化器，覆盖 `setup()` 中配置的序列化方式 |

---

**`ttl` — 过期时间**

```python
from nb_cache import Cache
from datetime import timedelta

cache = Cache().setup("mem://")

@cache.cache(ttl=60)            # 60 秒
@cache.cache(ttl=1.5)           # 1.5 秒
@cache.cache(ttl="30m")         # 30 分钟
@cache.cache(ttl="1h30m")       # 1 小时 30 分钟
@cache.cache(ttl="1d")          # 1 天
@cache.cache(ttl=timedelta(hours=2))
```

---

**`key` — 缓存键**

不传时自动使用"函数全限定名 + 所有参数值"作为 key。

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

# 自动生成（推荐大多数场景）
@cache.cache(ttl=60)
def get_user(user_id, role):
    ...
# 生成 key 类似: "mymodule:get_user:role=admin:user_id=1"

# 简单字符串模板：{参数名}
@cache.cache(ttl=60, key="{user_id}")
def get_user(user_id, role):
    ...
# 生成 key: "1"（只用 user_id）

# 组合模板
@cache.cache(ttl=60, key="user:{user_id}:role:{role}")
def get_user(user_id, role):
    ...

# 点号访问 dict / 对象属性
@cache.cache(ttl=60, key="user:{user.id}:{user.name}")
def get_user(user):
    ...
# user={"id": 1, "name": "Alice"} → key: "user:1:Alice"

# 格式修饰符：:hash（取 md5 前8位，适合 dict/list 等复杂参数）
@cache.cache(ttl=60, key="myproj1:search:{keyword:lower}:{filters:hash}")
def search(keyword, filters):
    ...

# callable：参数与被装饰函数完全相同，返回 key 字符串
def make_key(user, action):
    return "perm:{}:{}:{}".format(user["org"], user["id"], action)

@cache.cache(ttl=300, key=make_key)
def check_permission(user, action):
    ...
```

---

**`condition` — 缓存条件**

决定函数返回值是否应该被缓存。默认等同于 `NOT_NONE`（即返回 None 时不缓存）。

```python
from nb_cache import Cache, NOT_NONE, with_exceptions, only_exceptions

cache = Cache().setup("mem://")

# NOT_NONE（默认）：只缓存非 None 结果
@cache.cache(ttl=60, condition=NOT_NONE)
def get_data(key):
    return query(key)   # 返回 None 时不缓存

# 自定义条件：结果满足某个业务规则才缓存
@cache.cache(ttl=60, condition=lambda r: r is not None and r.get("status") == "ok")
def get_order(order_id):
    ...

# 永远缓存（包括 None）
@cache.cache(ttl=60, condition=lambda r: True)
def might_return_none(key):
    ...
```

---

**`lock` — 防缓存击穿**

`lock=True` 时，当缓存 miss 且有多个并发请求同时到达时，只有第一个请求会真正调用函数，其余请求等待并复用第一个请求的结果，避免"击穿"。

- **内存后端**：使用进程级别的互斥锁
- **Redis 后端**：使用 Redis `SET NX EX` 分布式锁，跨进程/跨节点生效

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

# 高并发热点数据，开锁防击穿
@cache.cache(ttl=60, lock=True)
def get_hot_ranking():
    return db.query_heavy()

# 自定义锁超时（避免函数执行过慢时锁过早释放）
@cache.cache(ttl=60, lock=True, lock_ttl=30)
def slow_query(key):
    return db.expensive_query(key)
```

> `lock_ttl` 默认等于 `ttl`。若函数执行时间可能超过 `ttl`，建议单独设置 `lock_ttl` 为更合理的值（如函数最长执行时间的 2 倍）。

---

**`tags` — 标签批量失效**

给缓存打标签，方便按业务维度批量删除一组缓存，无需记录每个具体的 key。

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

@cache.cache(ttl=300, tags=("user", "user_profile"))
def get_user_profile(user_id):
    ...

@cache.cache(ttl=300, tags=("user",))
def get_user_orders(user_id):
    ...

# 用户数据有更新时，一次性清除所有打了 "user" 标签的缓存
cache.delete_tags_sync("user")
# 或异步
await cache.delete_tags("user")
```

---

**`prefix` — key 前缀**

用于在同一后端内进一步区分不同模块/版本的缓存，与 `setup(prefix=...)` 叠加：

```python
from nb_cache import Cache

# setup 全局前缀 "prod"，装饰器额外前缀 "v2"
# 最终 key 形如: "prod:v2:mymodule:get_user:user_id=1"
cache = Cache().setup("redis://localhost:6379/0", prefix="prod")

@cache.cache(ttl=60, prefix="v2")
def get_user(user_id):
    ...
```

---

**`serializer` — 序列化器**

覆盖全局序列化配置，对单个函数使用不同的序列化策略：

```python
from nb_cache import Cache, Serializer, JsonSerializer, GzipCompressor

cache = Cache().setup("mem://")

# 该函数专用 JSON + gzip 序列化（适合大响应体压缩存储）
json_gz = Serializer(serializer=JsonSerializer(), compressor=GzipCompressor())

@cache.cache(ttl=3600, serializer=json_gz)
def get_large_report(report_id):
    return generate_report(report_id)
```

### 失败回退 `failover`

异常时返回缓存的旧值：

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

@cache.failover(ttl=3600, exceptions=(ConnectionError, TimeoutError))
def get_remote_data(key):
    return remote_api.call(key)
```

### 提前刷新 `early`

剩余 TTL 低于 `early_ttl` 时在后台提前刷新，防止缓存雪崩：

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

@cache.early(ttl=60, early_ttl=10)
def get_data(key):
    return query(key)
```

### 软过期 `soft`

`soft_ttl` 到期后后台刷新并立刻返回旧值，不阻塞请求：

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

@cache.soft(ttl=120, soft_ttl=60)
def get_data(key):
    return query(key)
```

### 命中次数缓存 `hit`

N 次命中后自动失效，`update_after` 命中后提前后台刷新：

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

@cache.hit(ttl=3600, cache_hits=100, update_after=50)
def get_data(key):
    return query(key)
```

### 加锁装饰器 `locked`

保证同一时间只有一个调用在执行：

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

@cache.locked(ttl=30)
def critical_operation(resource_id):
    ...
```

### 熔断器 `circuit_breaker`

错误率超标时熔断，保护下游服务：

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

@cache.circuit_breaker(errors_rate=0.5, period=60, ttl=30)
def unstable_service(key):
    return call_service(key)
```

### 限流 `rate_limit`

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

# 固定窗口限流（60秒内最多 100 次）
@cache.rate_limit(limit=100, period=60)
def api_endpoint(user_id):
    ...

# 滑动窗口限流（更平滑）
@cache.slice_rate_limit(limit=100, period=60)
def api_endpoint(user_id):
    ...
```

### 迭代器缓存 `iterator`

缓存生成器/异步生成器的全部结果：

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

@cache.iterator(ttl=60)
def get_items():
    for item in query_all():
        yield item
```

## 缓存 Key 定制

### 简单模板（字符串占位符）

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

# 单参数
@cache.cache(ttl=60, key="{user_id}")
def get_user(user_id):
    ...

# 多参数组合
@cache.cache(ttl=60, key="order:{user_id}:{status}")
def get_orders(user_id, status):
    ...
```

### 点号访问对象/字典属性

当参数是 dict 或对象时，用 `{param.attr}` 取其中的字段：

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

@cache.cache(ttl=60, key="user:{user.id}:{user.name}")
def get_user(user):
    ...

# 调用时 user={"id": 1, "name": "Alice"}
# 生成 key → "user:1:Alice"
```

### 格式修饰符

| 修饰符 | 作用 | 示例 |
|---|---|---|
| `:hash` | 取 md5 前8位，适合复杂对象/长字符串 | `{filters:hash}` |
| `:lower` | 转小写，适合大小写不敏感的场景 | `{keyword:lower}` |

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

# dict 参数 hash 化，避免 key 过长
@cache.cache(ttl=60, key="search:{keyword:lower}:{filters:hash}")
def search(keyword, filters):
    ...
```

### 自定义函数生成 key

当逻辑复杂无法用模板表达时，传入一个可调用对象：

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

def build_key(user, action):
    # 接收和被装饰函数相同的参数
    return "perm:{}:{}:{}".format(user['org_id'], user['id'], action)

@cache.cache(ttl=300, key=build_key)
def check_permission(user, action):
    return db.query_permission(user['id'], action)
```

## 锁（上下文管理器）

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

# 同步锁
with cache.lock("resource_key", ttl=10):
    do_critical_work()

# 异步锁
async with cache.alock("resource_key", ttl=10):
    await do_critical_work()
```

## 标签系统

按标签批量失效缓存：

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

@cache.cache(ttl=60, tags=("users",))
def get_user(user_id):
    ...

@cache.cache(ttl=60, tags=("users", "profiles"))
def get_profile(user_id):
    ...

# 同步：按标签清除
cache.delete_tags_sync("users")

# 异步
await cache.delete_tags("users")
```

## 事务

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

# 同步事务（自动提交，异常自动回滚）
with cache.transaction() as tx:
    tx.set("key1", "val1", ttl=60)
    tx.set("key2", "val2", ttl=60)

# 异步事务
async with cache.transaction() as tx:
    tx.set("key1", "val1")
    tx.set("key2", "val2")
```

## 直接操作缓存

```python
from nb_cache import Cache

cache = Cache().setup("mem://")

# 同步
cache.set_sync("key", "value", ttl=60)
val = cache.get_sync("key")
cache.delete_sync("key")
cache.exists_sync("key")
cache.incr_sync("counter")

# 异步
await cache.set("key", "value", ttl=60)
val = await cache.get("key")
await cache.delete("key")
```

## TTL 格式

```python
from nb_cache import Cache
from datetime import timedelta

cache = Cache().setup("mem://")

@cache.cache(ttl=60)               # 秒数
@cache.cache(ttl=1.5)              # 小数秒
@cache.cache(ttl="30m")            # 30分钟
@cache.cache(ttl="1h")             # 1小时
@cache.cache(ttl="1d12h30m")       # 1天12小时30分钟
@cache.cache(ttl=timedelta(hours=1))  # timedelta
```

## 序列化

默认使用 pickle，支持切换 JSON：

```python
from nb_cache import Cache, JsonSerializer, Serializer

cache = Cache().setup("mem://")

json_ser = Serializer(serializer=JsonSerializer())

@cache.cache(ttl=60, serializer=json_ser)
def get_data():
    return {"name": "test"}
```

## 快速回答

#### 如何查看缓存最终生成的key是什么？ 

```
因为nb_cache 已经在 nb_cache.key 日志命名空间，用debug 日志级别打印了最终生成的key。

所以你可以通过 nb_log.get_logger('nb_cache.key') 来查看。

也可以通过 logger = logging.getLogger("nb_cache.key")  logger.setLevel(logging.DEBUG) 来查看。
```

## 许可证

MIT License
