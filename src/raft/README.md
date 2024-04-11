# Raft

## 相关问题

- Committing entries from previous terms 问题
  - `no op` log

## 性能优化

以下猜想待验证：
本项目借鉴了各种 Raft 实现，发现有一种处理方式：将命令提交到日志后，不立即发送 AppendEntries RPC，而是等待心跳包一起发送，在代码实现上虽然更明了，
但会增加处理时间，因此，若采用立即发送 appendEntries RPC 的方式，可以大大提高性能。

## 批量测试

由于分布式的随机性和复杂性，还需要批量测试，因此需要通过脚本来批量测试。
