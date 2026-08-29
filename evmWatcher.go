package evmwatcher

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

const (
	// default block span of one eth_getLogs call, most providers cap it
	// between 2000 and 10000 blocks
	defaultMaxBlockRange = uint64(2000)
	// default minimum gap between two RPC calls, keeps the scan under the
	// provider rate limit
	defaultRequestInterval = 200 * time.Millisecond
	// interval of getting the node head and reporting it through SetWatchedBlockNumber
	checkpointInterval = 2 * time.Minute
	heartbeatInterval  = 30 * time.Second
	logChannelBuffer   = 4096
	eventChannelBuffer = 4096
	resubscribeDelay   = 5 * time.Second
	// minimum gap between two delivery-driven watermark writes, keeps a busy
	// stream from turning into one storage write per block
	deliveryReportInterval = 5 * time.Second
	rateLimitBackoff       = time.Second
	maxRateLimitBackoff    = 30 * time.Second
	defaultPollInterval    = 3 * time.Second
	largeGapThreshold      = uint64(1000)
	filterLogsTimeout      = 30 * time.Second
)

// WatcherData describes one contract and the events watched on it.
type WatcherData struct {
	ContractAddress string
	ContractABI     string
	// EventNames is the watched event name list, empty means every event in the ABI.
	EventNames []string
}

// Event is the decoded contract event delivered through EventInterface.
type Event struct {
	ContractAddress common.Address
	EventName       string
	BlockNumber     uint64
	BlockHash       common.Hash
	TxHash          common.Hash
	TxIndex         uint
	LogIndex        uint
	Args            map[string]any
	// Removed is true when the log has been reverted by a chain reorg.
	Removed bool
	Raw     types.Log
}

// watchTarget is the parsed form of WatcherData, events are indexed by topic0.
type watchTarget struct {
	address common.Address
	parsed  abi.ABI
	events  map[common.Hash]abi.Event
}

// EVMWatcher watches multiple contracts of one chain over a single connection,
// so that the whole chain shares one consistent block progress.
type EVMWatcher struct {
	// chainName identifies the watched chain for the storage and the event
	// callbacks, it is provided by the caller.
	chainName string
	wssURL    string
	ethClient *ethclient.Client

	storage StorageInterface
	event   EventInterface
	logger  Logger
	debug   bool

	mu          sync.RWMutex
	watcherData []WatcherData
	targets     map[common.Address]*watchTarget
	lastBlock   uint64
	logCh       chan types.Log
	eventCh     chan *Event
	sub         ethereum.Subscription
	started     bool
	// streaming is true only when the log subscription is alive and the gap it
	// left behind has been scanned, the progress may advance only in that case.
	streaming bool

	// maxBlockRange is shrunk at runtime once the provider reports a cap
	// smaller than the configured one.
	maxBlockRange uint64

	requestInterval time.Duration
	rpcMu           sync.Mutex
	lastRequest     time.Time

	// confirmations delays delivery and watermark until a block is N blocks
	// behind the chain head. 0 keeps the live subscription path.
	confirmations uint64
	pollInterval  time.Duration
	// latestHead is the freshest BlockNumber we have seen, used to clamp
	// watermarks and Stop flush to safeHead without an extra RPC.
	latestHead atomic.Uint64

	// pendingEvents counts the events decoded but not yet handed to OnEvent,
	// deliveredBlock is the highest block an event has been delivered for.
	// Together they tell how far the delivery lags behind the scan cursor.
	pendingEvents  atomic.Int64
	deliveredBlock atomic.Uint64
	// reportedBlock is the watermark persisted through SetWatchedBlockNumber,
	// it never regresses. Guarded by reportMu, which is held across the storage
	// call so the writes cannot land out of order.
	reportMu           sync.Mutex
	reportedBlock      uint64
	lastDeliveryReport time.Time

	reloadCh chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	// resubscribeMu guards the background resubscribe goroutine so that only
	// one rebuild runs at a time and eventLoop never blocks on it.
	resubscribeMu sync.Mutex
	resubscribing bool
	// backfillMu serializes concurrent backfill callers (startup goroutine,
	// checkpointLoop, confirmationLoop, resubscribe).
	backfillMu sync.Mutex
}

type Option func(*EVMWatcher)

// WithMaxBlockRange sets the block span of one eth_getLogs call, it is shrunk
// automatically when the provider rejects the range.
func WithMaxBlockRange(blocks uint64) Option {
	return func(e *EVMWatcher) {
		if blocks > 0 {
			e.maxBlockRange = blocks
		}
	}
}

// WithRequestInterval sets the minimum gap between two RPC calls.
func WithRequestInterval(interval time.Duration) Option {
	return func(e *EVMWatcher) {
		if interval > 0 {
			e.requestInterval = interval
		}
	}
}

// WithConfirmations delays delivery and watermark until a block is N blocks
// behind the chain head. 0 (default) keeps the current immediate-delivery behavior.
func WithConfirmations(n uint64) Option {
	return func(e *EVMWatcher) {
		e.confirmations = n
	}
}

// WithPollInterval sets the poll interval used when confirmations > 0.
func WithPollInterval(d time.Duration) Option {
	return func(e *EVMWatcher) {
		if d > 0 {
			e.pollInterval = d
		}
	}
}

func NewEVMWatcher(chainName, wssURL string, watcherData []WatcherData, storage StorageInterface, event EventInterface, options ...Option) *EVMWatcher {
	watcher := &EVMWatcher{
		chainName:       chainName,
		wssURL:          wssURL,
		watcherData:     watcherData,
		storage:         storage,
		event:           event,
		targets:         make(map[common.Address]*watchTarget),
		maxBlockRange:   defaultMaxBlockRange,
		requestInterval: defaultRequestInterval,
		pollInterval:    defaultPollInterval,
		reloadCh:        make(chan struct{}, 1),
	}
	for _, option := range options {
		option(watcher)
	}
	if watcher.logger == nil {
		watcher.logger = defaultLogger(chainName)
	}
	return watcher
}

func (e *EVMWatcher) Start() error {
	e.mu.Lock()
	if e.started {
		e.mu.Unlock()
		return fmt.Errorf("watcher already started")
	}
	if e.storage == nil || e.event == nil {
		e.mu.Unlock()
		return fmt.Errorf("storage and event interface are required")
	}
	// Without a chain name several chains would share the same progress key.
	if e.chainName == "" {
		e.mu.Unlock()
		return fmt.Errorf("chain name is required")
	}
	watcherData := append([]WatcherData(nil), e.watcherData...)
	e.mu.Unlock()

	targets := make(map[common.Address]*watchTarget, len(watcherData))
	for _, wd := range watcherData {
		target, err := newWatchTarget(wd)
		if err != nil {
			return err
		}
		targets[target.address] = target
	}
	if len(targets) == 0 {
		return fmt.Errorf("no watcher data configured")
	}

	client, err := ethclient.Dial(e.wssURL)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", e.wssURL, err)
	}
	ctx, cancel := context.WithCancel(context.Background())

	e.mu.Lock()
	e.ethClient = client
	e.targets = targets
	e.logCh = make(chan types.Log, logChannelBuffer)
	e.eventCh = make(chan *Event, eventChannelBuffer)
	e.ctx, e.cancel = ctx, cancel
	e.mu.Unlock()

	// The consumer has to run before the scan starts, otherwise the events
	// decoded while catching up would fill eventCh and block the scan.
	e.wg.Add(1)
	go e.notifyLoop()

	head, err := e.blockNumber(ctx)
	if err != nil {
		e.release(cancel, client)
		return fmt.Errorf("failed to get block number: %w", err)
	}
	e.latestHead.Store(head)

	stored, err := e.storage.GetWatchedBlockNumber(e.chainName)
	if err != nil {
		e.release(cancel, client)
		return fmt.Errorf("failed to get watched block number: %w", err)
	}

	startAt := head
	if e.confirmations > 0 {
		startAt = e.safeHead(head)
	}
	if stored < 0 {
		// no progress recorded, watch from the current (safe) head
		e.setLastBlock(startAt)
	} else {
		e.setLastBlock(uint64(stored))
	}
	// Everything up to the starting point was delivered by the previous run.
	initial := e.getLastBlock()
	e.deliveredBlock.Store(initial)
	e.reportMu.Lock()
	e.reportedBlock = initial
	e.reportMu.Unlock()

	mode := "subscription"
	if e.confirmations > 0 {
		mode = "confirmation"
	}
	e.logger.Infof("start watcher: head=%d stored=%d startAt=%d confirmations=%d mode=%s",
		head, stored, e.getLastBlock(), e.confirmations, mode)
	if stored >= 0 {
		gap := head - uint64(stored)
		if gap > largeGapThreshold {
			e.logger.Warnf("large block gap: head=%d stored=%d gap=%d", head, stored, gap)
		}
	}
	for _, target := range targets {
		for _, ev := range target.events {
			e.logger.Infof("watch event: contract=%s name=%s topic0=%s",
				target.address.Hex(), ev.Name, ev.ID.Hex())
		}
	}

	if e.confirmations > 0 {
		e.mu.Lock()
		e.started = true
		e.streaming = true
		e.mu.Unlock()

		e.wg.Add(1)
		go e.confirmationLoop()

		if startAt > 0 && e.getLastBlock() < startAt {
			e.startBackfill(startAt)
		}
		return nil
	}

	e.mu.Lock()
	e.started = true
	e.streaming = true
	e.mu.Unlock()

	// eventLoop must run before subscribe so logCh is drained immediately.
	e.wg.Add(2)
	go e.eventLoop()
	go e.checkpointLoop()

	if err := e.subscribe(); err != nil {
		e.Stop()
		return err
	}

	if e.getLastBlock() < head {
		e.startBackfill(head)
	}
	return nil
}

func (e *EVMWatcher) Stop() {
	e.mu.Lock()
	if !e.started {
		e.mu.Unlock()
		return
	}
	e.started = false
	e.streaming = false
	cancel, sub, client := e.cancel, e.sub, e.ethClient
	e.sub = nil
	e.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if sub != nil {
		sub.Unsubscribe()
	}
	e.wg.Wait()
	// Flush the final watermark after the pending events are drained.
	// Never persist above the current safe head when confirmations are on.
	candidate := e.getLastBlock()
	if e.confirmations > 0 {
		safe := e.safeHead(e.latestHead.Load())
		if safe < candidate {
			candidate = safe
		}
	}
	e.reportWatermark(candidate)
	if client != nil {
		client.Close()
	}
}

func (e *EVMWatcher) AddWatcher(watcherData WatcherData) error {
	target, err := newWatchTarget(watcherData)
	if err != nil {
		return err
	}

	e.mu.Lock()
	if _, ok := e.targets[target.address]; ok {
		e.mu.Unlock()
		return fmt.Errorf("contract %s is already watched", target.address.Hex())
	}
	e.watcherData = append(e.watcherData, watcherData)
	e.targets[target.address] = target
	started := e.started
	e.mu.Unlock()

	if started && e.confirmations == 0 {
		e.notifyReload()
	}
	return nil
}

func (e *EVMWatcher) RemoveWatcher(watcherData WatcherData) error {
	address := common.HexToAddress(watcherData.ContractAddress)

	e.mu.Lock()
	if _, ok := e.targets[address]; !ok {
		e.mu.Unlock()
		return fmt.Errorf("watcher data not found")
	}
	delete(e.targets, address)
	for i, w := range e.watcherData {
		if common.HexToAddress(w.ContractAddress) == address {
			e.watcherData = append(e.watcherData[:i], e.watcherData[i+1:]...)
			break
		}
	}
	started, remaining := e.started, len(e.targets)
	e.mu.Unlock()

	// An empty address list would match every contract on the chain, so keep
	// the current subscription until a new target is added.
	if started && remaining > 0 && e.confirmations == 0 {
		e.notifyReload()
	}
	return nil
}

func (e *EVMWatcher) GetWatcherData() []WatcherData {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return append([]WatcherData(nil), e.watcherData...)
}

// eventLoop is the only consumer of the log stream, it keeps the delivery order
// identical to the node push order.
func (e *EVMWatcher) eventLoop() {
	defer e.wg.Done()

	ctx := e.context()
	for {
		select {
		case <-ctx.Done():
			return
		case vLog := <-e.logCh:
			if e.debug && len(vLog.Topics) > 0 {
				e.logger.Debugf("raw log: block=%d tx=%s topic0=%s",
					vLog.BlockNumber, vLog.TxHash.Hex(), vLog.Topics[0].Hex())
			}
			e.dispatch(vLog)
		case err := <-e.subError():
			// The blocks missed from now on are unknown, freeze the progress
			// until the stream is back and the gap is scanned.
			e.setStreaming(false)
			e.logger.Errorf("log subscription broken: %v", err)
			e.startResubscribe()
		case <-e.reloadCh:
			if err := e.subscribe(); err != nil {
				e.setStreaming(false)
				e.logger.Errorf("failed to apply the new watcher list: %v", err)
				e.startResubscribe()
			}
		}
	}
}

// checkpointLoop keeps the RPC connection alive with periodic BlockNumber
// calls and heals gaps when the WSS filter stops pushing logs.
func (e *EVMWatcher) checkpointLoop() {
	defer e.wg.Done()

	ctx := e.context()
	heartbeat := time.NewTicker(heartbeatInterval)
	checkpoint := time.NewTicker(checkpointInterval)
	defer heartbeat.Stop()
	defer checkpoint.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			e.heartbeat(ctx)
		case <-checkpoint.C:
			// While the stream is down the covered range is unknown, let
			// resubscribe handle the gap instead.
			if !e.isStreaming() {
				continue
			}
			head, err := e.blockNumber(ctx)
			if err != nil {
				e.logger.Errorf("failed to get block number: %v", err)
				continue
			}
			cursor := e.getLastBlock()
			if head > cursor {
				if err := e.backfill(ctx, head); err != nil {
					e.logger.Errorf("checkpoint backfill failed: %v", err)
				}
			}
			e.logger.Infof("checkpoint tick: head=%d lastBlock=%d delivered=%d reported=%d streaming=%v",
				head, e.getLastBlock(), e.deliveredBlock.Load(), e.getReportedBlock(), e.isStreaming())
		}
	}
}

// confirmationLoop polls the chain head and scans up to safeHead when
// confirmations > 0. Live subscription is disabled in this mode.
func (e *EVMWatcher) confirmationLoop() {
	defer e.wg.Done()

	ctx := e.context()
	ticker := time.NewTicker(e.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			head, err := e.blockNumber(ctx)
			if err != nil {
				e.logger.Errorf("failed to get block number: %v", err)
				continue
			}
			target := e.safeHead(head)
			if target > e.getLastBlock() {
				if err := e.backfill(ctx, target); err != nil {
					e.logger.Errorf("failed to scan confirmed blocks up to %d: %v", target, err)
				}
			} else {
				e.heartbeatAt(ctx, head)
			}
		}
	}
}

// heartbeat persists the current scan progress and logs the chain head.
func (e *EVMWatcher) heartbeat(ctx context.Context) {
	e.heartbeatAt(ctx, 0)
}

func (e *EVMWatcher) heartbeatAt(ctx context.Context, head uint64) {
	if head == 0 {
		var err error
		head, err = e.blockNumber(ctx)
		if err != nil {
			e.logger.Errorf("heartbeat failed: %v", err)
			return
		}
	}
	cursor := e.getLastBlock()
	e.reportWatermark(cursor)
	e.logger.Infof("heartbeat: head=%d lastBlock=%d delivered=%d reported=%d",
		head, cursor, e.deliveredBlock.Load(), e.getReportedBlock())
}

// safeHead returns head - confirmations. When confirmations is 0 it returns head.
func (e *EVMWatcher) safeHead(head uint64) uint64 {
	if e.confirmations == 0 {
		return head
	}
	if head <= e.confirmations {
		return 0
	}
	return head - e.confirmations
}

// startBackfill runs backfill in the background so Start() is not blocked.
func (e *EVMWatcher) startBackfill(head uint64) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		ctx := e.context()
		if err := e.backfill(ctx, head); err != nil && ctx.Err() == nil {
			e.logger.Errorf("backfill failed up to %d: %v", head, err)
		}
	}()
}

// backfill scans from the recorded progress up to head in batches.
func (e *EVMWatcher) backfill(ctx context.Context, head uint64) error {
	e.backfillMu.Lock()
	defer e.backfillMu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		from := e.getLastBlock() + 1
		if from > head {
			return nil
		}
		to := from + e.getMaxBlockRange() - 1
		if to > head {
			to = head
		}

		e.logger.Infof("backfill batch start: from=%d to=%d target_head=%d", from, to, head)

		// The scanned range may be narrower than the requested one when the
		// provider rejects it.
		logs, scanned, err := e.filterLogs(ctx, from, to)
		if err != nil {
			return err
		}
		for _, vLog := range logs {
			e.dispatch(vLog)
		}

		e.logger.Infof("backfill batch done: from=%d to=%d logs=%d scanned_to=%d", from, to, len(logs), scanned)
		e.setLastBlock(scanned)
		e.reportWatermark(scanned)
	}
}

// filterLogs scans [from, to] and returns the logs together with the block
// number actually reached. The range is halved when the provider rejects it and
// the call is retried when the rate limit is hit.
func (e *EVMWatcher) filterLogs(ctx context.Context, from, to uint64) ([]types.Log, uint64, error) {
	query := e.buildQuery()
	backoff := rateLimitBackoff
	for {
		query.FromBlock = new(big.Int).SetUint64(from)
		query.ToBlock = new(big.Int).SetUint64(to)

		if err := e.waitRequestSlot(ctx); err != nil {
			return nil, 0, err
		}
		rpcCtx, cancel := context.WithTimeout(ctx, filterLogsTimeout)
		logs, err := e.client().FilterLogs(rpcCtx, query)
		cancel()
		if err == nil {
			return logs, to, nil
		}

		switch {
		case errors.Is(err, context.DeadlineExceeded):
			e.logger.Warnf("filterLogs timed out on [%d, %d] after %s, retry in %s", from, to, filterLogsTimeout, backoff)
			select {
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			case <-time.After(backoff):
			}
			if backoff < maxRateLimitBackoff {
				backoff *= 2
			}
		case isRateLimitError(err):
			e.logger.Warnf("rate limited on scanning [%d, %d], retry in %s", from, to, backoff)
			select {
			case <-ctx.Done():
				return nil, 0, ctx.Err()
			case <-time.After(backoff):
			}
			if backoff < maxRateLimitBackoff {
				backoff *= 2
			}
		case isBlockRangeError(err) && to > from:
			// A hard cap of the provider, remember it for the next batches.
			span := (to - from + 1) / 2
			e.setMaxBlockRange(span)
			to = from + span - 1
			e.logger.Warnf("block range rejected, shrink the range to %d blocks", span)
		case isResultLimitError(err) && to > from:
			// Only this range is too dense, keep the configured span.
			to = from + (to-from+1)/2 - 1
		default:
			return nil, 0, fmt.Errorf("failed to filter logs in [%d, %d]: %w", from, to, err)
		}
	}
}

func (e *EVMWatcher) blockNumber(ctx context.Context) (uint64, error) {
	if err := e.waitRequestSlot(ctx); err != nil {
		return 0, err
	}
	head, err := e.client().BlockNumber(ctx)
	if err != nil {
		return 0, err
	}
	e.latestHead.Store(head)
	return head, nil
}

// waitRequestSlot serializes the RPC calls and keeps the configured gap between
// them.
func (e *EVMWatcher) waitRequestSlot(ctx context.Context) error {
	e.rpcMu.Lock()
	defer e.rpcMu.Unlock()

	if wait := time.Until(e.lastRequest.Add(e.requestInterval)); wait > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}
	e.lastRequest = time.Now()
	return nil
}

// dispatch decodes one log and notifies the caller through EventInterface.
func (e *EVMWatcher) dispatch(vLog types.Log) {
	if len(vLog.Topics) == 0 {
		return
	}

	e.mu.RLock()
	target := e.targets[vLog.Address]
	e.mu.RUnlock()
	if target == nil {
		e.logger.Warnf("drop log: reason=unknown_address address=%s topic0=%s block=%d tx=%s",
			vLog.Address.Hex(), vLog.Topics[0].Hex(), vLog.BlockNumber, vLog.TxHash.Hex())
		return
	}
	// The node matches addresses and topics independently, which makes the
	// filter a cartesian product, so check the pair locally again.
	event, ok := target.events[vLog.Topics[0]]
	if !ok {
		e.logger.Warnf("drop log: reason=unknown_topic0 topic0=%s address=%s block=%d tx=%s",
			vLog.Topics[0].Hex(), vLog.Address.Hex(), vLog.BlockNumber, vLog.TxHash.Hex())
		return
	}

	args, err := decodeLog(target.parsed, event, vLog)
	if err != nil {
		e.logger.Errorf("failed to decode event %s of %s: %v", event.Name, vLog.Address.Hex(), err)
		return
	}
	// Hand the event over to notifyLoop. Wait with backpressure when the
	// channel is full instead of blocking forever without yielding.
	ev := &Event{
		ContractAddress: vLog.Address,
		EventName:       event.Name,
		BlockNumber:     vLog.BlockNumber,
		BlockHash:       vLog.BlockHash,
		TxHash:          vLog.TxHash,
		TxIndex:         vLog.TxIndex,
		LogIndex:        vLog.Index,
		Args:            args,
		Removed:         vLog.Removed,
		Raw:             vLog,
	}
	for {
		e.pendingEvents.Add(1)
		select {
		case e.eventCh <- ev:
			e.logger.Infof("dispatch: event=%s block=%d tx=%s log_index=%d",
				event.Name, vLog.BlockNumber, vLog.TxHash.Hex(), vLog.Index)
			return
		case <-e.context().Done():
			e.pendingEvents.Add(-1)
			return
		default:
			e.pendingEvents.Add(-1)
		}
		select {
		case e.eventCh <- ev:
			e.pendingEvents.Add(1)
			e.logger.Infof("dispatch: event=%s block=%d tx=%s log_index=%d",
				event.Name, vLog.BlockNumber, vLog.TxHash.Hex(), vLog.Index)
			return
		case <-e.context().Done():
			return
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// notifyLoop delivers the events to the caller, it is the only goroutine calling
// OnEvent so the delivery order stays the same as the chain order.
func (e *EVMWatcher) notifyLoop() {
	defer e.wg.Done()

	ctx := e.context()
	for {
		select {
		case event := <-e.eventCh:
			e.notify(event)
		case <-ctx.Done():
			// Deliver the already decoded events that are still within safeHead.
			for {
				select {
				case event := <-e.eventCh:
					if e.confirmations > 0 {
						safe := e.safeHead(e.latestHead.Load())
						if event.BlockNumber > safe {
							e.pendingEvents.Add(-1)
							continue
						}
					}
					e.notify(event)
				default:
					return
				}
			}
		}
	}
}

func (e *EVMWatcher) notify(event *Event) {
	if e.confirmations > 0 {
		safe := e.safeHead(e.latestHead.Load())
		if event.BlockNumber > safe {
			// Unconfirmed events must not be delivered; drop and leave watermark.
			e.pendingEvents.Add(-1)
			e.logger.Warnf("drop unconfirmed event %s of %s at block %d (safeHead %d)",
				event.EventName, event.ContractAddress.Hex(), event.BlockNumber, safe)
			return
		}
	}

	err := e.event.OnEvent(e.chainName, event)
	e.pendingEvents.Add(-1)
	if err != nil {
		// Do not advance the watermark on failure, so a restart can rescan
		// this block and deliver the event again.
		e.logger.Errorf("failed to notify event %s of %s: %v", event.EventName, event.ContractAddress.Hex(), err)
		return
	}
	e.logger.Infof("delivered: event=%s block=%d tx=%s",
		event.EventName, event.BlockNumber, event.TxHash.Hex())
	// Persist progress right after a successful delivery, so a restart will not
	// reprocess the same event.
	if event.BlockNumber > e.deliveredBlock.Load() {
		e.deliveredBlock.Store(event.BlockNumber)
	}
	e.setLastBlock(event.BlockNumber)
	e.reportDelivered(event.BlockNumber)
}

// reportDelivered persists the delivery progress at most once every
// deliveryReportInterval. The final flush in Stop bypasses the throttle by
// calling reportWatermark directly.
func (e *EVMWatcher) reportDelivered(blockNumber uint64) {
	e.reportMu.Lock()
	throttled := time.Since(e.lastDeliveryReport) < deliveryReportInterval
	if !throttled {
		e.lastDeliveryReport = time.Now()
	}
	e.reportMu.Unlock()
	if throttled {
		return
	}
	e.reportWatermark(blockNumber)
}

// hasBacklog reports whether some received logs or decoded events are still on
// their way to the caller.
func (e *EVMWatcher) hasBacklog() bool {
	return e.pendingEvents.Load() > 0 || len(e.logCh) > 0
}

// reportWatermark persists the progress through SetWatchedBlockNumber. candidate
// is the block the scan or the stream has reached; while undelivered events
// remain, only the block before the oldest undelivered one is safe to persist,
// otherwise a crash would lose the queued events for good.
func (e *EVMWatcher) reportWatermark(candidate uint64) {
	if e.confirmations > 0 {
		safe := e.safeHead(e.latestHead.Load())
		if safe == 0 {
			return
		}
		if candidate > safe {
			candidate = safe
		}
	}

	if e.hasBacklog() {
		delivered := e.deliveredBlock.Load()
		if delivered == 0 {
			return
		}
		// The oldest undelivered event may sit in the same block as the last
		// delivered one, so only the block before it is safe.
		if delivered-1 < candidate {
			candidate = delivered - 1
		}
	}

	e.reportMu.Lock()
	defer e.reportMu.Unlock()
	if candidate <= e.reportedBlock {
		return
	}
	if err := e.storage.SetWatchedBlockNumber(e.chainName, candidate); err != nil {
		e.logger.Errorf("failed to save watched block number %d: %v", candidate, err)
		return
	}
	e.reportedBlock = candidate
}

func (e *EVMWatcher) subscribe() error {
	query := e.buildQuery()
	if len(query.Addresses) == 0 {
		return fmt.Errorf("no watcher data configured")
	}

	topicCount := 0
	if len(query.Topics) > 0 {
		topicCount = len(query.Topics[0])
	}
	e.logger.Infof("filter query: addresses=%d topic0_count=%d", len(query.Addresses), topicCount)

	e.mu.RLock()
	client, ctx, logCh := e.ethClient, e.ctx, e.logCh
	e.mu.RUnlock()

	if err := e.waitRequestSlot(ctx); err != nil {
		return err
	}
	sub, err := client.SubscribeFilterLogs(ctx, query, logCh)
	if err != nil {
		e.logger.Errorf("subscription failed: %v", err)
		return fmt.Errorf("failed to subscribe logs: %w", err)
	}

	e.mu.Lock()
	old := e.sub
	e.sub = sub
	e.mu.Unlock()
	if old != nil {
		old.Unsubscribe()
	}
	e.logger.Infof("subscription established")
	return nil
}

// startResubscribe kicks off resubscribe in the background so eventLoop can
// keep draining logCh while the gap is being scanned.
func (e *EVMWatcher) startResubscribe() {
	e.resubscribeMu.Lock()
	if e.resubscribing {
		e.resubscribeMu.Unlock()
		return
	}
	e.resubscribing = true
	e.resubscribeMu.Unlock()

	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer func() {
			e.resubscribeMu.Lock()
			e.resubscribing = false
			e.resubscribeMu.Unlock()
		}()
		e.resubscribe()
	}()
}

// resubscribe keeps retrying until the subscription is rebuilt and the blocks
// missed while the stream was down are scanned. The underlying WSS connection is
// re-dialed by the rpc client itself on the next call.
func (e *EVMWatcher) resubscribe() {
	ctx := e.context()
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(resubscribeDelay):
		}

		head, err := e.blockNumber(ctx)
		if err != nil {
			e.logger.Errorf("failed to get block number: %v", err)
			continue
		}
		// Scan the gap before subscribing so the same logs are not delivered
		// twice from both FilterLogs and the live stream.
		if e.getLastBlock() < head {
			if err := e.backfill(ctx, head); err != nil {
				e.logger.Errorf("failed to scan the missed blocks: %v", err)
				continue
			}
		}
		if err := e.subscribe(); err != nil {
			e.logger.Errorf("failed to resubscribe logs: %v", err)
			continue
		}
		// Catch up blocks produced during subscribe setup.
		if head, err := e.blockNumber(ctx); err != nil {
			e.logger.Errorf("failed to get block number after resubscribe: %v", err)
		} else if e.getLastBlock() < head {
			if err := e.backfill(ctx, head); err != nil {
				e.logger.Errorf("failed to catch up after resubscribe: %v", err)
				continue
			}
		}

		e.setStreaming(true)
		e.logger.Infof("log subscription recovered at block %d", e.getLastBlock())
		return
	}
}

func (e *EVMWatcher) buildQuery() ethereum.FilterQuery {
	e.mu.RLock()
	defer e.mu.RUnlock()

	addresses := make([]common.Address, 0, len(e.targets))
	topicSet := make(map[common.Hash]struct{})
	for address, target := range e.targets {
		addresses = append(addresses, address)
		for topic := range target.events {
			topicSet[topic] = struct{}{}
		}
	}
	topics := make([]common.Hash, 0, len(topicSet))
	for topic := range topicSet {
		topics = append(topics, topic)
	}

	query := ethereum.FilterQuery{Addresses: addresses}
	if len(topics) > 0 {
		query.Topics = [][]common.Hash{topics}
	}
	return query
}

func (e *EVMWatcher) notifyReload() {
	select {
	case e.reloadCh <- struct{}{}:
	default:
	}
}

func (e *EVMWatcher) subError() <-chan error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.sub == nil {
		return nil
	}
	return e.sub.Err()
}

func (e *EVMWatcher) getReportedBlock() uint64 {
	e.reportMu.Lock()
	defer e.reportMu.Unlock()
	return e.reportedBlock
}

func (e *EVMWatcher) client() *ethclient.Client {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.ethClient
}

func (e *EVMWatcher) context() context.Context {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.ctx
}

func (e *EVMWatcher) isStreaming() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.streaming
}

func (e *EVMWatcher) setStreaming(streaming bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.streaming = streaming
}

func (e *EVMWatcher) getMaxBlockRange() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.maxBlockRange
}

func (e *EVMWatcher) setMaxBlockRange(blocks uint64) {
	if blocks == 0 {
		blocks = 1
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if blocks < e.maxBlockRange {
		e.maxBlockRange = blocks
	}
}

func (e *EVMWatcher) getLastBlock() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lastBlock
}

func (e *EVMWatcher) setLastBlock(blockNumber uint64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if blockNumber > e.lastBlock {
		e.lastBlock = blockNumber
	}
}

func (e *EVMWatcher) release(cancel context.CancelFunc, client *ethclient.Client) {
	e.mu.Lock()
	sub := e.sub
	e.sub = nil
	e.mu.Unlock()

	if sub != nil {
		sub.Unsubscribe()
	}
	cancel()
	e.wg.Wait()
	client.Close()
}

// The providers report their limits with plain messages instead of stable error
// codes, so the wording has to be matched.
var (
	rateLimitHints   = []string{"too many requests", "rate limit", "request limit", "compute units", "capacity", "throttl", "429"}
	blockRangeHints  = []string{"block range", "range is too wide", "range too large", "up to a", "limited to a", "blocks range"}
	resultLimitHints = []string{"more than", "response size", "too many results", "query timeout", "result set too large"}
)

func isRateLimitError(err error) bool {
	return matchErrorHints(err, rateLimitHints)
}

func isBlockRangeError(err error) bool {
	return matchErrorHints(err, blockRangeHints)
}

func isResultLimitError(err error) bool {
	return matchErrorHints(err, resultLimitHints)
}

func matchErrorHints(err error, hints []string) bool {
	message := strings.ToLower(err.Error())
	for _, hint := range hints {
		if strings.Contains(message, hint) {
			return true
		}
	}
	return false
}

func newWatchTarget(watcherData WatcherData) (*watchTarget, error) {
	if !common.IsHexAddress(watcherData.ContractAddress) {
		return nil, fmt.Errorf("invalid contract address: %s", watcherData.ContractAddress)
	}
	parsed, err := abi.JSON(strings.NewReader(watcherData.ContractABI))
	if err != nil {
		return nil, fmt.Errorf("failed to parse the ABI of %s: %w", watcherData.ContractAddress, err)
	}

	events := make(map[common.Hash]abi.Event)
	if len(watcherData.EventNames) == 0 {
		for _, event := range parsed.Events {
			events[event.ID] = event
		}
	} else {
		for _, name := range watcherData.EventNames {
			event, ok := parsed.Events[name]
			if !ok {
				return nil, fmt.Errorf("event %s is not found in the ABI of %s", name, watcherData.ContractAddress)
			}
			events[event.ID] = event
		}
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("no event to watch for %s", watcherData.ContractAddress)
	}

	return &watchTarget{
		address: common.HexToAddress(watcherData.ContractAddress),
		parsed:  parsed,
		events:  events,
	}, nil
}

func decodeLog(parsed abi.ABI, event abi.Event, vLog types.Log) (map[string]any, error) {
	args := make(map[string]any)
	if len(vLog.Data) > 0 {
		if err := parsed.UnpackIntoMap(args, event.Name, vLog.Data); err != nil {
			return nil, err
		}
	}

	var indexed abi.Arguments
	for _, input := range event.Inputs {
		if input.Indexed {
			indexed = append(indexed, input)
		}
	}
	if len(indexed) > 0 {
		if len(vLog.Topics) < len(indexed)+1 {
			return nil, fmt.Errorf("expect %d topics but got %d", len(indexed)+1, len(vLog.Topics))
		}
		if err := abi.ParseTopicsIntoMap(args, indexed, vLog.Topics[1:]); err != nil {
			return nil, err
		}
	}
	return args, nil
}
