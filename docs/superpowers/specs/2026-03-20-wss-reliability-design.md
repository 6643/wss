# WSS Reliability Design

## 1. 背景
- 当前 `wss` 已有基础收发能力。
- 现有实现存在 5 类关键问题:
  - 接收队列满时静默丢消息。
  - `ioTimeout` 会误伤整条连接。
  - 公开 option 暴露未导出的 `serverConn`。
  - 未限制消息大小。
  - 浏览器端重连存在 stale stream 竞态。

## 2. 目标
- 修复接收压力下的静默丢消息问题。
- 修复单次读写超时误断连问题。
- 收敛公开 API，使包外可稳定使用。
- 增加默认安全边界。
- 修复浏览器端重连状态错乱。

## 3. 非目标
- 不修改消息帧格式。
- 不引入新依赖。
- 不重写 `stream` 包。
- 不保证接收侧“绝不丢消息”。

## 4. 边界
- 输入:
  - [client.go](/._/lib/go/wss/client.go)
  - [serve.go](/._/lib/go/wss/serve.go)
  - [write_stream.go](/._/lib/go/wss/write_stream.go)
  - [timeout.go](/._/lib/go/wss/timeout.go)
  - [client.ts](/._/lib/go/wss/client.ts)
  - [wss_test.go](/._/lib/go/wss/wss_test.go)
- 输出:
  - 一组小幅 API 调整。
  - 一组可验证的行为修复。
  - 配套文档和测试。
- 约束:
  - 保留 `stream` 的 `Try` API。
  - `wss` 内部接收主干不再依赖 `TryPut` 的静默失败行为。
  - 默认行为优先稳定性与可观测性。

## 5. 事实 / 假设 / 推断
### 5.1 事实
- Go 侧接收路径当前忽略 `TryPut` 返回值。
- Go 侧读写都复用 `ioTimeout`。
- `github.com/coder/websocket.Conn` 的 `Read/Write` 在 context 过期后会关闭连接。
- `WithServerOpened`、`WithServerClosed`、`WithServerErred` 当前使用未导出的 `*serverConn`。
- 浏览器端 `attachWSS` 后的异步 `opened/closed` 回调没有“连接代次”校验。

### 5.2 假设
- 业务可接受接收侧在压力下丢弃当前消息，但必须显式观测。
- 默认更看重“连接保持”和“错误显式暴露”，而不是“压力下强保消息完整性”。

### 5.3 推断
- 接收队列满时最小风险方案是“丢弃并上报，不关闭连接”。
- 读超时和写超时必须拆分，否则 API 名称和行为不一致。

## 6. 方案对比
### 6.1 主方案: 丢弃并上报，保持连接
- 做法:
  - 接收队列满时丢弃当前消息。
  - 触发导出错误 `ErrRecvQueueFull`。
  - 调用现有 `erred` 回调。
  - 连接保持不关闭。
- 为什么选:
  - 对瞬时繁忙友好。
  - 不再静默丢消息。
  - 语义简单，测试成本低。
- 成本:
  - 新增错误定义。
  - 调整接收路径。
  - 补测试和文档。
- 风险:
  - 接收语义变成 `best effort`。
  - 上层若要求强一致消息投递，需要额外机制。
- 回滚:
  - 可退回到旧的 `TryPut` 忽略错误逻辑，但会恢复静默丢消息，不建议。

### 6.2 备方案: 限时背压，超时后断连
- 做法:
  - 接收队列满时阻塞等待一定时间。
  - 超时后关闭连接。
- 为什么不选:
  - 参数复杂。
  - 故障边界更难解释。
  - 与当前“不要因为瞬时忙就断连”的选择冲突。

### 6.3 备方案: 满即断连
- 做法:
  - 接收队列满时立即关闭连接。
- 为什么不选:
  - 对瞬时繁忙不友好。
  - 用户已明确不接受。

## 7. 设计决策
### 7.1 Go 接收路径
- 新增导出错误:
  - `ErrRecvQueueFull`
- 客户端和服务端接收路径都改为:
  1. 尝试把收到的消息放入接收队列。
  2. 若队列满，丢弃当前消息。
  3. 触发 `erred` 回调。
  4. 记录日志。
  5. 继续保持连接。
- 保持 `TryPut` 在 `stream` 包中存在。
- `wss` 内部不再忽略 `TryPut` 返回值。

### 7.2 Go 超时语义
- 新增 option:
  - `WithClientReadTimeout`
  - `WithClientWriteTimeout`
  - `WithServerReadTimeout`
  - `WithServerWriteTimeout`
- 保留兼容别名:
  - `WithClientIOTimeout`
  - `WithServerIOTimeout`
- 兼容别名行为:
  - 等价于同时设置读超时和写超时。
- 默认值:
  - `readTimeout = 0`
  - `writeTimeout = 2 * time.Minute`
- 关键修正:
  - `Put(ctx)` 的 `ctx` 只控制:
    - 写请求是否能入发送队列。
    - 调用方等待结果多久。
  - 底层 `conn.Write(...)` 使用连接上下文和 `writeTimeout`。
  - 调用方 `ctx` 超时不再直接关闭整条连接。

### 7.3 Go 公开类型
- 导出 `ServerConn`。
- `Server.Get(ctx)` 返回 `(*ServerConn, []byte, error)`。
- 服务端回调 option 改为使用 `*ServerConn`。
- `ServerConn` 对外只暴露稳定方法:
  - `Addr() string`
  - `Header() http.Header`

### 7.4 Read Limit
- 新增 option:
  - `WithClientReadLimit`
  - `WithServerReadLimit`
- 默认值:
  - `8 << 20`
- 行为:
  - 建连后立即对底层连接设置 `SetReadLimit`。
- 选择原因:
  - 无上限风险过高。
  - 8 MiB 足以覆盖常见文本和中等二进制消息。
- 回滚:
  - 调用方可显式调大或设为业务需要的更高值。

### 7.5 浏览器端重连
- 为每次连接建立递增 `connectionId`。
- 只有当前 `connectionId` 才能:
  - 推进 `Opened/Closed` 状态。
  - flush 队列。
  - 触发当前连接的关闭处理。
- 旧连接的晚到回调直接忽略。
- `forceReconnect` 只对当前活跃连接生效。

### 7.6 可观测性
- 当接收队列满时:
  - 记录错误日志。
  - 调用 `erred` 回调。
- 当前不新增 metrics 依赖。
- 文档中要求调用方在 `erred` 回调里做计数或告警。

## 8. 入口与影响范围
- 入口:
  - `NewClient`
  - `NewServer`
  - `Client.readLoop`
  - `Client.writeMessages`
  - `Server.handleConnection`
  - `Server.writeMessages`
  - 浏览器端 `newWssClient`
- 影响范围:
  - Go 客户端。
  - Go 服务端。
  - 浏览器端客户端。
  - README 和测试。

## 9. 验收标准
- 接收队列满时:
  - 当前消息被丢弃。
  - `erred` 回调收到 `ErrRecvQueueFull`。
  - 连接仍可继续收发。
- `Put(ctx)` 超时后:
  - 当前调用返回超时错误。
  - 连接仍可用于后续收发。
- 默认空闲读不因为旧 `ioTimeout` 误断连。
- `ServerConn` 可在包外正常作为回调参数类型使用。
- 超大消息触发 read limit 失败。
- 浏览器端旧连接回调不会污染新连接状态。

## 10. 测试策略
### 10.1 Go 测试
- 新增或补齐:
  - 接收队列满时上报错误但不断连。
  - `Put(ctx)` 超时不影响后续写。
  - 新旧 timeout option 兼容行为。
  - `readLimit` 超限行为。
  - `ServerConn` 包外可用性。

### 10.2 浏览器端测试
- 使用 `bun test` 增加最小重连竞态测试。
- 重点覆盖:
  - stale `opened`
  - stale `closed`
  - 手动 `reconnect`
  - 队列 flush 仅作用于当前连接

## 11. 实施顺序
1. 导出 `ServerConn` 并调整公开 option。
2. 拆分 Go 侧读写超时并保留兼容别名。
3. 补接收队列满错误语义。
4. 增加 read limit。
5. 修浏览器端重连竞态。
6. 更新 README 和测试。

## 12. 残余风险
- `ErrRecvQueueFull` 频繁出现时，上层仍需要自行决定是否限流或扩容消费能力。
- `8 MiB` 默认值可能不适合所有业务。
- 浏览器端测试若仅做最小覆盖，仍可能遗漏真实浏览器时序差异。

## 13. 回滚路径
- API 层面:
  - 兼容保留 `With*IOTimeout`，可降低升级阻力。
- 代码层面:
  - 各项修改可按提交粒度独立回滚:
    - `ServerConn` 导出
    - timeout 拆分
    - 接收队列满语义
    - read limit
    - TS 重连修复
- 运行层面:
  - `readLimit`、读写 timeout 都通过 option 可调。
