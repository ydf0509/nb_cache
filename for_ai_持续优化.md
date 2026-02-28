

# 100 持续对ai要求修改迭代的 promote

## 100.1 能不能去掉 cache.init_sync()

我发现 cache在setup 之后，每次都要调用 init_sync() 或者 init() ，能不能去掉这个调用，让cache 自动初始化。

init 和 init_sync 的区别是?我发现所有使用例子都没用到 init ，只用到了 init_sync


## 100.2 在哪里加一个debug日志显示 最终生成的缓存key







