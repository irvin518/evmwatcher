# 可选共享 getLogs 查询

`SharedLogProvider` 只合并 `eth_getLogs` 请求，不拥有 watcher 的 WS、事件处理、去重或水位。未配置时走原有 RPC 路径；已配置时 `FilterLogs` 的结果仍交回各 watcher 的原有补扫和事件处理逻辑。

```go
// sharedClient 是调用方创建的独立 RPC 客户端；不要复用某个 watcher 的
// WS 客户端，以免该 watcher 停止时关闭其他 watcher 的共享查询连接。
provider, err := evmwatcher.NewSharedLogProvider(sharedClient, evmwatcher.SharedLogConfig{
    ChainID: "1672",
    RPCIdentity: "pharos-primary",
    Window: 30 * time.Millisecond,
    MaxBlockRange: 1000, // RPC 允许的单次范围，按实际节点限制配置
    MaxAddresses: 50,    // RPC 允许的单次地址数量
})
if err != nil { /* 处理配置错误 */ }

// mainStorage 和 vaultStorage 各自读取/写入独立水位；事件处理器也独立。
mainWatcher := evmwatcher.NewEVMWatcher("pharos", mainWS, mainData, mainStorage, mainHandler,
    evmwatcher.WithSharedLogProvider(provider, "1672", "pharos-primary"))
vaultWatcher := evmwatcher.NewEVMWatcher("interest-vault", vaultWS, vaultData, vaultStorage, vaultHandler,
    evmwatcher.WithSharedLogProvider(provider, "1672", "pharos-primary"))

// 调用方启动并停止每个 watcher。所有 watcher 停止之后：
provider.Close()
sharedClient.Close()
```

provider 只为其配置的 chain ID 和 RPC 身份合并查询；身份不匹配时使用该 watcher 自己的 RPC 客户端独立查询。传入同一 provider、相匹配的 chain ID、RPC 身份和确认数的 watcher，可在窗口内合并相同或重叠范围。`MaxBlockRange` 是包含首尾的最大区块数；超出限制、地址过多、不重叠、过滤条件不是固定地址加 topic0、或错过窗口的请求独立执行。节点明确拒绝合并后的地址或 topic 过滤条件时会分别重试原始查询；限流、超时和其他错误返回各 watcher 的原有重试路径。provider 不查询 header，也不缓存日志结果。

身份字符串由调用方保证准确：同一字符串必须代表同一网络和 RPC 端点；库无法根据标签证明两个连接实际指向同一个后端。每个 watcher 的 `chainName` 是存储键和回调身份，应该根据业务需要保持独立。

新增合约需要独立的 watcher 和明确设置的存储起始水位。对于要从区块 `N > 0` 开始扫描的 watcher，业务存储应返回已完成水位 `N-1`；返回 `-1` 会按现有行为从当前链头开始。原有 `AddWatcher` 并不为新合约建立独立历史水位，不能用于要求从旧区块追赶的新合约；已有 watcher 应保持其原配置。区块 0 的独立初始化语义受现有水位模型限制。

`Stats()` 中的 `Requests` 表示传入 provider 的请求数，`MergedBatches` 表示真正发起的多请求合并批次，`MergedRequests` 表示这些批次中的请求数，`IndependentCalls` 包括未合并和回退调用；这是进程内尝试次数，不是 RPC 服务商计费。可用 `MergedRequests / Requests` 观察请求合并率；需要另行统计节点 RPC、失败重试和实际账单。

原有事件去重是 watcher 内存级的，跨进程重启后业务处理仍须满足 `EventInterface` 文档中的幂等要求。共享查询不增加跨进程幂等、租约、fencing 或重组保证，也不改变原有逻辑。任何实际环境接入前应按业务自身的存储和重试语义验证。

调用方必须选择会对结果截断或超限明确报错的 RPC，并合理配置范围和地址限制；如果节点返回成功但静默截断，库无法仅凭响应判断不完整。原有 ABI 解码失败仅记录并丢弃事件，原有内存去重不会跨进程持久化；本次只共享查询，没有改变这些既有语义。

所有等待者取消时，provider 会取消本地共享 RPC context 并释放本地等待；JSON-RPC 节点是否同时停止已经开始的服务端计算取决于传输和节点实现。
