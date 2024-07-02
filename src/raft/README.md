# Raft 实验

## 介绍

该 Raft 实验项目参考了其他的各种代码，并做了一些修改，进行了各种优化。

本实验在结构上进行优化，以方便阅读。将 RPC 与相关的调用和处理函数封装到各自文件中；将日志相关的处理放在了 `log.go` 文件中。

如果你已经了解实现流程，可以跳过实验说明，查看优化的内容。

如果你想查看对应的代码，可以查看对应的分支，如`lab3A`。

## 实验说明

### Leader 选举

入手该代码的第一个任务是实现选举功能和心跳，该功能的实现以 Raft 论文中的状态转移流程图和 RPC 描述为主，可以暂时忽略其中的 logEntry、snapshot 等无关内容，最简化去实验选举流程。

实现过程中需要注意以下问题：

1. 区分选举时间和心跳时间：整个 Raft 实验中，时间仅涉及这两个概念，其中选举时间远大于心跳时间，论文和实验对该时间做出了一定的要求。
   > 可能会在`ticker`函数中发现实验预定义了一个 50~350s 秒的随机超时时间，不要和以上两个时间混淆，该时间可以用选举时间或者心跳时间去覆写，在下面的日志复制中会给出较优做法。
2. 考虑什么时候可以更新 Term：
   - Raft 状态更新时可以更新。
   - 接收方 收到 RPC 参数中 Term 比自身大的，可以更新。
   - 发送方收到 RPC 回复 Term 比自身大的，可以更新。
3. 如何更好处理互斥锁：在运行测试时加上`-race`，可以更好发现锁冲突问题，因此顺序执行和 go routine 的配合比较巧妙，能使得加最少的锁达到控制效果。

### 日志复制

这是第二个任务，在前者基础上实现发送 AppendEntries RPC，实现日志复制。它需要对`RequestVote`和`AppendEntries`进行加工，补充对日志的处理。

实现过程中需要注意以下问题：

1. 区分心跳和日志发送。心跳是没有 Log 的 AppendEnries。有些代码的实现中，将二者分别处理。例如 1s 发送 10 个心跳，当有新日志时，再发送一个带日志的 AppendEntries RPC，这会导致 10+n 个 rpc 的发送，还需要额外处理日志发送失败后重试问题，因此我们可以考虑将日志的发送合入心跳中，达到仍然发送 10 个 rpc 的效果，同时可以失败重试，但这会碰到一些性能问题，我们会在后面中提到。

2. 考虑日志流向：当有一个新日志时，流向为`服务层 -> Leader -> Follower`。后续处理逻辑时，当 Leader 收到 Majority 的响应（AppendEntries RPC）后，Leader 会回复服务层，同时在下一个 RPC 中，通告 Follower 去回复服务层。

3. 区分 nextIndex, matchIndex, PrevLogIndex。其中 nextIndex 表示 Leader 预期下一个发送给 Follower 的日志索引，matchIndex 表示 Leader 已确认 Follower 接受的最后一个日志索引，PrevLogIndex 则代表紧邻这发送给 Follower 的日志索引的前一个索引，一般来说`prevLogIndex = nextIndex - 1`。

4. 实现日志匹配算法。PrevLogIndex 用于检验是否有冲突，当有冲突时，就需要修改 nextIndex 以达到成功匹配并复制。

### 持久化

持久化的功能实现较为简单，主要是在状态更新的时候将状态持久化到磁盘中，在宕机后重新启动时，读取磁盘内容。

这一实验任务主要考虑的是哪些内容应该持久化，以及持久化的时机，不再赘述。

### 日志压缩

日志压缩的方案是 Snapshot（快照）。简单理解，将某一时刻的状态压缩复制，生成一个 snapshot 的数据持久化下来，缓存中的之前的数据就可以丢弃。该概念比较好理解，但需要结合测试用例去考虑如何实现，实现内容主要包括`Snapshot函数`、`InstallSnapshot RPC及调用`。

1. 理解 Snapshot 流程。实际上，Raft Server 本身不会生成快照内容，这是由上层的 service layer 完成，并调用 Raft 的 Snapshot 函数去完成的（测试用例中有生成 snapshot 的代码）。但是，由于 InstallSnapshot RPC 的存在，Leader 在发现落后的 Follower 时，会将其快照数据发送给 Follower，而上层服务需要记录每一个 Raft Server 的状态数据，因此此时它不知道 Follower 也更新了快照，所以 Follower 需要将该信息同步给 service layer，在该实验中表现为主动发送一个 Snapshot Msg。

2. 区别真实索引和虚拟索引。由于 Snapshot 会将缓存中的日志进行截断，因此这会导致 Index 值和真实索引不一致，规则为`realIndex = virtIndex - lastIncludeIndex`。可以在`log.go`查看。

3. 考虑何时调用`Install Snapshot RPC`，论文中写的是发现 Older Follower，可以理解为，需要同步给 Follower 的日志的一部分已经因为 snapshot 截断，无法发送，此时就可调用该 RPC。

## 实验优化

性能优化的核心在于减少 RPC 的发送和尽快发送 RPC，在测试时可以将信息输出到日志中分析并考虑。本节以`TestBackup3B`测试用例为标准，这是测试用例中第一个用时很久的测试，最初用时 40~50s。从以下三个方面去优化。

日志匹配算法优化。在最初的写法中，根据论文 RPC 流程图，在 PrevLogIndex 不匹配的时候，可以在下次发送时将 nextIndex-1，但是如果有 100 条日志落后，会发送 100 次 rpc 以达到匹配位置。论文给出了对应的思路，每次跳着找到上一个检测的位置。应用该策略后，测试用例用时达到 30s 左右。

> If desired, the protocol can be optimized to reduce the number of rejected AppendEntries RPCs. For example, when rejecting an AppendEntries request, the follower can include the term of the conflicting entry and the first index it stores for that term. With this information, the leader can decrement nextIndex to bypass all of the conflicting entries in that term; one AppendEntries RPC will be required for each term with conflicting entries, rather than one RPC per entry. In practice, we doubt this optimization is necessary, since failures happen infrequently and it is unlikely that there will be many inconsistent entries.

日志发送流程优化。前面提到，将带 Log 的 Entries 和心跳合并，可以减少 RPC 次数，降低代码复杂度，但是它会导致更长的测试时间。例如 Leader 在 1.01 秒收到 LogEntry，由于上面规则，它需要等到 1.10 秒发送心跳时，去发送该日志，这中间的时间间隔是 0.09 秒，于是导致测试时间过长。那么，我们只需要在每次收到日志时，直接先发送一次带 Log 的心跳，就可以避开这个时间间隔，而后如果发送失败，会在下一个心跳中重试。在该优化后，测试用例用时 20s 左右。

AppendEntries Majority 优化。在 Candidate 发送 RequestVote RPC 时，会进行票数统计，但是我们发现，appendEntries 并没有进行这样的 Majority 统计，于是导致这样的情况发送：（前提是我在 leader 发送 AppendEntries 时会重置选举时间）Leader1 在选举时间中收不到任何成功的回复，因此在该时间超时后，它会不断发送 AppendEntries RPC，而此时其他 Follower 由于收不到请求，会主动申请成为新的 Leader2，在日志中我发现这两个 leader 仍不断发送 AppendEntries RPC，这产生了多余的 RPC。合理的做法，应该是在 Leader 长时间接受不到心跳响应时，自动降级成为 Follower，于是我借鉴了其他的写法，增加了`quorumActive`函数，去判断在一个选举时间内是否收到了大多数 Follower 的响应（注意，这里是在一个选举时间内去判断，如果是在每次心跳时间内判断会导致 leader 过于容易失效，于是导致大量的重新选举）。在该优化后，测试用例用时 16~17s 左右，与 Raft 官方测试时间接近。

## 实验测试

测试用例都已经都经过测试过 5~6 次以上的测试，由于分布式的随机性和复杂性，还需要批量大量测试，因此需要通过脚本来批量测试。（尚未完成）
