# Spec: Optional Block Confirmations for `evmwatcher`

## Goal

给 `github.com/irvin518/evmwatcher` 增加可选的 **N 区块确认深度**。  
`confirmations == 0`（默认）保持现有行为；`confirmations > 0` 时，只处理并推进水位到 `safeHead = head - confirmations`。

不要改 `StorageInterface` / `EventInterface` 签名。不要改默认行为。

---

## Public API

```go
// WithConfirmations delays delivery and watermark until a block is N blocks
// behind the chain head. 0 (default) keeps the current immediate-delivery behavior.
func WithConfirmations(n uint64) Option

// Optional but recommended: poll interval used when confirmations > 0.
// Default: 3 * time.Second.
func WithPollInterval(d time.Duration) Option
```

内部字段：

```go
confirmations uint64        // default 0
pollInterval  time.Duration // default 3s；仅 confirmations > 0 时使用
```

辅助方法：

```go
func (e *EVMWatcher) safeHead(head uint64) uint64 {
    if e.confirmations == 0 {
        return head
    }
    if head <= e.confirmations {
        return 0
    }
    return head - e.confirmations
}
```

---

## Behavior

### `confirmations == 0`（默认，必须与现在完全一致）

1. WSS `SubscribeFilterLogs`，日志到达即 `dispatch` → `OnEvent`
2. `backfill` 扫到当前 `head`
3. `checkpointLoop` 按现有逻辑上报 head / watermark
4. 断线重连 + gap backfill 逻辑不变

### `confirmations > 0`

**硬规则：任何 `OnEvent` 投递与 `SetWatchedBlockNumber` 水位，都不得超过 `safeHead(head)`。**

1. **启动 backfill**
   - 取 `head`，目标改为 `safeHead(head)`，不是裸 `head`
   - `head <= confirmations` 时：不扫、不推进，等下一轮

2. **实时路径（推荐实现，语义最清晰）**
   - **不要**把未确认的订阅日志直接 `OnEvent`
   - 可二选一：
     - **A（推荐）**：`confirmations > 0` 时不订阅，只靠轮询 `confirmationLoop`
     - **B**：保留订阅仅作唤醒，仍只用 `FilterLogs` 扫到 `safeHead` 后投递
   - **禁止**：订阅到日志就立刻 `OnEvent`

3. **`confirmationLoop`**（仅 `confirmations > 0` 启动）

```text
ticker = pollInterval (default 3s)
loop:
  head = BlockNumber()
  target = safeHead(head)
  if target == 0 || target <= lastBlock: continue
  backfill(ctx, target)   // 现有 batch / rate-limit / block-range 逻辑复用
```

4. **`checkpointLoop`**
   - `confirmations > 0` 时：上报 / 持久化用 `safeHead(head)`，禁止写裸 `head`
   - 或与 `confirmationLoop` 合并，避免两套逻辑抢水位

5. **`Removed` / reorg**
   - 因只投递已确认块，未确认重组自然被挡在 `safeHead` 外
   - 已投递后的深重组：保持现有 `Removed` 行为即可（若 A 路径几乎收不到 Removed，可接受）

6. **Stop**
   - 停 loop、排空已解码且 **block ≤ 当前 safeHead** 的事件
   - 最终 watermark ≤ `safeHead`，不得写成未确认高度

---

## Touch Points（改这些即可）

| 位置 | 改动 |
|------|------|
| `Option` / `NewEVMWatcher` | 增加 `WithConfirmations`、`WithPollInterval` 与字段默认值 |
| `safeHead` | 新增 |
| `Start` | backfill 上限改为 `safeHead(head)`；`confirmations > 0` 时启动 `confirmationLoop`，按方案 A/B 处理订阅 |
| `backfill` | 调用方传入的 `head` 已是 safe 上限；内部无需再减（或统一在入口调 `safeHead`，二选一，避免减两次） |
| `checkpointLoop` / 新 `confirmationLoop` | 见上 |
| `eventLoop` / `dispatch` / `notify` | `confirmations > 0` 且来自订阅的未确认日志不得投递 |
| `reportWatermark` | 候选高度若 > safeHead，先 clamp（若 loop 已保证，可省略，但建议加一层防护） |

---

## Non-goals

- 不改 Redis / storage key 设计（仍按 `chainName` 一条水位）
- 不按合约地址拆水位
- 不强制调用方开启确认；默认 0
- 不引入新的必选依赖

---

## Acceptance

1. `confirmations == 0`：行为与改前一致（订阅即时投递 + backfill 到 head）
2. `confirmations == 12`：`OnEvent` 的 `BlockNumber` 始终 ≤ `head - 12`
3. `SetWatchedBlockNumber` 写入值始终 ≤ `head - 12`
4. 重启后从 storage 水位 `+1` 继续，不会因未确认块被跳过而丢事件
5. `head < confirmations` 时不投递、不推进水位
6. 现有 `WithMaxBlockRange` / `WithRequestInterval` / rate-limit / range-shrink 在确认模式下仍生效

---

## Caller example（改库后业务侧用法，本次不必改业务）

```go
evmwatcher.NewEVMWatcher(chain, wss, data, storage, handler,
    evmwatcher.WithMaxBlockRange(2000),
    evmwatcher.WithConfirmations(12),      // 0 = off
    evmwatcher.WithPollInterval(3*time.Second),
)
```

---

## Implementation note for the executing agent

优先实现 **方案 A**：`confirmations > 0` 时禁用 live subscription delivery，只用 `confirmationLoop` + 现有 `backfill`/`filterLogs`。改动面最小、语义与「等 N 块再处理」一致。
