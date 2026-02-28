
# 1. 目录说明

1. `nb_cache` 是本项目的核心库，用于实现缓存功能。
nb_cache 项目要支持python3.6 以上的语法。

2. `cashews` 是知名缓存三方包, 仅仅支持py3.9以上
cashews是给ai参考的，参考有哪些功能， nb_cache 要支持所有这些功能。

nb_cache 不要 从  cashews import 任何模块，cashews 只作为ai的思考灵感来源和参考对象。
 
# 2 要求
cashews 三方包对同步函数支持不好， nb_cache 要支持同步函数和异步函数式，通过同一个装饰器来支持。

ai需要先梳理 cashews 有哪些功能，全部列举出来

nb_cache 要求和 cashews 一样，支持所有功能。
```
要支持python3.6以上的版本
支持同步和异步函数，
支持加锁防止缓存击穿，
支持内存和redis作为缓存器，
支持redis+内存双缓存提高性能
支持设置ttl  
支持lock防止缓存击穿 
支持装饰器和with上下文用法
支持灵活的函数入参生成过滤key
```

# 3. ai在 nb_cache 目录下写代码，并持续编写修改项目根目录下的 README.md 介绍用法

# 10 github

https://github.com/ydf0509/nb_cache

# 11 需要打包发布到pypi













