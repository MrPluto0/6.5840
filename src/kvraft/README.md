# KV Raft

根据要求完成一个基于 Raft 的 key、value 的存储服务，支持失败重试，服务端仅生效一次。

## Server 和 Raft

每个 server 对应 raft 的一个状态机，必定有一个 Server 对应 raft 的 Leader，为了在每次调用时，向 Leader 发起请求，客户端需要轮询 Server 找到 Leader。

## RPC 唯一性

为了使得每个 RPC 只生效一次，需要对每个客户端的请求序列进行记录，如果收到旧序列的请求，则忽略。

## 快照

快照机制对应了 Raft 实验中日志压缩，可参考 Raft 实验中测试文件的`applierSnap`函数，包括在合适时机创建快照，以及恢复快照。
