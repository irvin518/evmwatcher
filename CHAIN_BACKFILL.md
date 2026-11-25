# 每链公共补扫（可选）

`ChainBackfiller` 为固定的一组 watcher 维护一个公共补扫水位。每个 watcher 仍使用自己的 WS 客户端、订阅、`eth_blockNumber` 心跳和 `EventInterface`；注册到公共补扫后，watcher 不再自行调用 `getLogs`。补扫器在每次扫描时连接配置的 RPC，查询注册 watcher 的合约地址和事件签名，处理完成后关闭客户端。

业务实现 `ChainBackfillStorage`，持久化 `ChainBackfillProgress`。`CompareAndSwapChainBackfill` 必须在同一持久化存储中原子比较完整旧值（包括不存在状态、区块和成员指纹）并写入新值；比较不匹配或写入失败必须返回错误，不能返回成功。公共水位只在区间内所有已注册目标的全部 `getLogs` 子查询成功，且每条事件的 `OnEvent` 返回 `nil` 后提交。空结果也必须完成所有子查询。业务回调返回 `nil` 应表示业务处理成功，或事件已经可靠持久化并交由业务重试，不能只放进易失内存队列。

```go
start := uint64(123456) // 包含该区块；由业务明确指定
scanner, err := evmwatcher.NewChainBackfiller(evmwatcher.ChainBackfillConfig{
    ChainID: "1672", RPCURL: backfillRPC,
    ProgressKey: "pharos-registered-contracts-v1",
    StartBlock: &start,
    Confirmations: 0,
    Interval: 10 * time.Second,
    MaxBlockRange: 1000,
    MaxAddresses: 50,
    Storage: businessChainBackfillStorage,
})
// 检查 err。每个 watcher 的业务 handler 与 WS 地址独立。
main := evmwatcher.NewEVMWatcher("main", mainWS, mainData, nil, mainHandler)
vault := evmwatcher.NewEVMWatcher("vault", vaultWS, vaultData, nil, vaultHandler)
if err := scanner.RegisterWatcher(main); err != nil { /* 处理错误 */ }
if err := scanner.RegisterWatcher(vault); err != nil { /* 处理错误 */ }
// 检查每次启动错误，已启动的组件在失败时由调用方依次停止。
if err := main.Start(); err != nil { /* 处理错误 */ }
if err := vault.Start(); err != nil { /* 处理错误 */ }
if err := scanner.Start(ctx); err != nil { /* 处理错误 */ }
// 退出时先 scanner.Stop()，再停止各 watcher。
```

每个 watcher 和补扫器的确认数必须一致；若使用非零确认数，watcher 需同时配置 `WithConfirmations`。`Errors()` 返回异步错误通道（缓冲区可能丢弃重复错误，全部错误仍会记录到 logger）；`ScanOnce(ctx)` 最多扫描一个区间并返回错误，便于调用方调度和观测。RPC 限流或错误使本区间不推进，后台按 `Interval` 重试；范围或结果超限时会缩小区间重试。多个地址子批次全部成功后才开始事件分发。

WS 和补扫在同一个 watcher 内共用事件身份和处理确认。WS 的事件如果正在处理或重试，补扫等待该次 `OnEvent` 成功；处理完成的重复事件会在内存保留期内跳过。**跨进程重启、内存去重记录过期、CAS 冲突或多实例并发时，事件可能再次投递**；业务必须对 `(chainID, watcher身份, contract地址, blockHash, txHash, logIndex, removed)` 实现持久化幂等，并对并发调用安全。不能将这视作恰好一次。

注册集合、合约 ABI、事件选择、确认数和初始区块被绑定到公共水位的成员指纹。进程重启时可以保留所有原 watcher 并新增 watcher，复用原进度键：扫描器先通过 CAS 将新成员清单和原水位一起持久化，然后从已完成水位的下一块扫描所有成员。新成员加入之前的事件不会追溯。原成员缺失或其配置变更会使扫描失败；运行期间不能注册或修改成员。老版本记录没有成员清单时，必须先使用完全一致的原成员运行一次以安全迁移，再重启增加新成员。

`ChainBackfillProgress.Manifest` 是状态的一部分，业务存储必须与 `Exists`、`Block`、`Membership` 一起持久化并原子比较与更新；旧适配器若丢弃该字段会导致新增成员不能安全工作。CAS 不构成分布式租约：多进程业务处理仍可能并发，须限制单扫描所有者。

默认未注册 `ChainBackfiller` 的 watcher 保持原有补扫行为；原 `SharedLogProvider` 是另一种按需合并查询的独立选择，两种模式不能对同一 watcher 同时启用。注册公共补扫的 watcher 也不接受 `BlockNumberProvider`，以确保其周期链头请求实际使用各自的 WS 客户端。本方案不自行实现持久化事务、分布式租约、链重组回滚或业务重放；业务须提供可靠的水位 CAS、事件幂等和重组处理。RPC 返回成功但静默截断时无法证明日志完整。业务 handler 若长时间阻塞不返回，也可能导致对应 watcher 停止等待；需要业务自身设置超时与告警。切勿因本地测试通过就直接用于生产。

公共水位不会自动回退：若区块在提交后发生重组，且 WS 在断连期间漏掉 `removed` 通知，当前公共补扫器不会自动回扫已提交区块，也无法自行撤销业务效果。确认数可降低风险，但不能保证最终性；需要业务或后续链重组对账机制补偿。当前实现不能在所有重组场景下保证事件不丢失。
