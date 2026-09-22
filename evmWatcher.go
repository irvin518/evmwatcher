package evmwatcher

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sort"
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
	checkpointInterval    = 2 * time.Minute
	heartbeatInterval     = 30 * time.Second
	logChannelBuffer      = 4096
	eventChannelBuffer    = 4096
	resubscribeDelay      = 5 * time.Second
	maxResubscribeDelay   = 30 * time.Second
	rateLimitBackoff      = time.Second
	maxRateLimitBackoff   = 30 * time.Second
	defaultPollInterval   = 3 * time.Second
	filterLogsTimeout     = 30 * time.Second
	blockNumberTimeout    = 15 * time.Second
	streamCatchUpBuffer   = 4096
	deliveryRetryInitial  = time.Second
	deliveryRetryMax      = 30 * time.Second
	dedupeRetentionBlocks = uint64(128)
)

type subscriptionFailure struct {
	Generation uint64
	Err        error
}

type eventKey struct {
	BlockHash common.Hash
	TxHash    common.Hash
	Address   common.Address
	LogIndex  uint
	Removed   bool
}

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
	// chainName identifies the watched deployment for storage and callbacks. It
	// must be unique to the actual network (and contract set when deployments
	// must not share progress), for example "pharos-mainnet-1672".
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
	// subFailures is stable for the lifetime of a Start call. Subscription
	// specific Err channels are forwarded here with a generation so replacing
	// a subscription cannot leave eventLoop waiting on a stale/nil channel.
	subFailures   chan subscriptionFailure
	subGeneration uint64
	subscribeMu   sync.Mutex
	catchUpDoneCh chan struct{}

	// maxBlockRange is shrunk at runtime once the provider reports a cap
	// smaller than the configured one.
	maxBlockRange uint64

	requestInterval time.Duration
	rpcMu           sync.Mutex
	lastRequest     time.Time

	// confirmations delays delivery and watermark until a block is N blocks
	// behind the chain head. 0 keeps the live subscription path.
	confirmations   uint64
	pollInterval    time.Duration
	gapHealInterval time.Duration
	// latestHead is the freshest BlockNumber we have seen, used to clamp
	// watermarks and Stop flush to safeHead without an extra RPC.
	latestHead atomic.Uint64
	// lastStreamLogUnixNano records receipt of raw subscription traffic. A quiet
	// contract does not by itself imply a broken subscription.
	lastStreamLogUnixNano atomic.Int64

	// pendingEvents counts the events decoded but not yet handed to OnEvent,
	// deliveredBlock is the highest block an event has been delivered for.
	// Together they tell how far the delivery lags behind the scan cursor.
	pendingEvents  atomic.Int64
	deliveredBlock atomic.Uint64
	// reportedBlock is the watermark persisted through SetWatchedBlockNumber,
	// it never regresses. Guarded by reportMu, which is held across the storage
	// call so the writes cannot land out of order.
	reportMu      sync.Mutex
	reportedBlock uint64

	// lastBlock is the highest continuously scanned block (scannedThrough), not
	// the highest block observed on the live stream. pendingByBlock protects the
	// committed watermark from crossing an event that has not been delivered.
	progressMu       sync.Mutex
	pendingByBlock   map[uint64]int
	seenEvents       map[eventKey]uint64
	committedThrough atomic.Uint64

	reloadCh chan struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	// resubscribeMu guards the background resubscribe goroutine so that only
	// one rebuild runs at a time and eventLoop never blocks on it.
	resubscribeMu sync.Mutex
	resubscribing bool
	// backfillMu serializes concurrent backfill callers (startup goroutine,
	// checkpointLoop, confirmationLoop, resubscribe, gap heal).
	backfillMu sync.Mutex

	// skipGapThreshold skips historical catch-up when head-stored exceeds this
	// value (0 disables). maxCatchUpBlocks caps how many blocks one backfill
	// run may scan (0 = unlimited).
	skipGapThreshold uint64
	maxCatchUpBlocks uint64
	// catchUpTarget is the chain head startup/resubscribe backfill must reach
	// before live WSS delivery is enabled. 0 means catch-up is complete.
	catchUpTarget  atomic.Uint64
	catchingUp     atomic.Bool
	gapHealRunning atomic.Bool
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

// WithGapHealInterval sets how often zero-confirmation mode scans for logs the
// live subscription may have missed. It is independent from confirmation polling.
func WithGapHealInterval(d time.Duration) Option {
	return func(e *EVMWatcher) {
		if d > 0 {
			e.gapHealInterval = d
		}
	}
}

// WithSkipGapThreshold skips historical catch-up when head-stored exceeds this
// value and jumps the cursor to the current (safe) head. 0 disables skipping.
func WithSkipGapThreshold(blocks uint64) Option {
	return func(e *EVMWatcher) {
		e.skipGapThreshold = blocks
	}
}

// WithMaxCatchUpBlocks limits how many blocks a single backfill run may scan.
// 0 means unlimited.
func WithMaxCatchUpBlocks(blocks uint64) Option {
	return func(e *EVMWatcher) {
		e.maxCatchUpBlocks = blocks
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
		gapHealInterval: heartbeatInterval,
		reloadCh:        make(chan struct{}, 1),
		pendingByBlock:  make(map[uint64]int),
		seenEvents:      make(map[eventKey]uint64),
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
	e.subFailures = make(chan subscriptionFailure, 16)
	e.catchUpDoneCh = make(chan struct{}, 1)
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
	if err := validateStoredWatermark(e.chainName, stored, head, startAt); err != nil {
		e.release(cancel, client)
		return err
	}
	initialBlock := startAt
	if stored >= 0 {
		initialBlock = uint64(stored)
	}
	// A stopped watcher can be started again. Rebuild all in-memory delivery
	// state from the durable committed watermark so uncommitted events replay.
	e.mu.Lock()
	e.lastBlock = initialBlock
	e.mu.Unlock()
	e.progressMu.Lock()
	e.pendingByBlock = make(map[uint64]int)
	e.seenEvents = make(map[eventKey]uint64)
	e.progressMu.Unlock()
	e.pendingEvents.Store(0)
	e.catchUpTarget.Store(0)
	e.catchingUp.Store(false)
	e.gapHealRunning.Store(false)
	e.lastStreamLogUnixNano.Store(0)
	// Everything up to the starting point was delivered by the previous run.
	initial := e.getLastBlock()
	e.deliveredBlock.Store(initial)
	e.committedThrough.Store(initial)
	e.reportMu.Lock()
	e.reportedBlock = initial
	e.reportMu.Unlock()

	mode := "subscription"
	if e.confirmations > 0 {
		mode = "subscription+confirm"
	}
	e.logger.Infof("start watcher: head=%d stored=%d startAt=%d confirmations=%d mode=%s skip_gap=%d max_catch_up=%d",
		head, stored, e.getLastBlock(), e.confirmations, mode, e.skipGapThreshold, e.maxCatchUpBlocks)
	e.applyGapStrategy(head, stored)
	for _, target := range targets {
		for _, ev := range target.events {
			e.logger.Infof("watch event: contract=%s name=%s topic0=%s",
				target.address.Hex(), ev.Name, ev.ID.Hex())
		}
	}

	e.mu.Lock()
	e.started = true
	e.mu.Unlock()

	if err := e.subscribe(); err != nil {
		e.Stop()
		return err
	}

	// Read the catch-up target after the subscription is live. Logs arriving
	// during this query remain in logCh and are buffered by eventLoop.
	postSubscribeHead, err := e.blockNumber(ctx)
	if err != nil {
		e.Stop()
		return fmt.Errorf("failed to get block number after subscribing: %w", err)
	}
	catchTarget := e.catchUpHead(postSubscribeHead)
	if catchTarget > 0 && e.getLastBlock() < catchTarget {
		e.beginCatchUp(catchTarget)
	} else {
		e.setStreaming(true)
	}

	e.wg.Add(2)
	go e.eventLoop()
	go e.checkpointLoop()
	if e.catchingUp.Load() {
		e.startBackfill(catchTarget)
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
	e.subGeneration++
	e.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if sub != nil {
		sub.Unsubscribe()
	}
	e.wg.Wait()
	// Flush only the safe committed watermark. Queued/failed events remain
	// behind it and are replayed from storage on the next start.
	candidate := e.committedThrough.Load()
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

	if started {
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
	if started && remaining > 0 {
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
	buffered := make([]types.Log, 0, streamCatchUpBuffer)
	flushBuffered := func(finishCatchUp bool) bool {
		if e.catchingUp.Load() && !finishCatchUp {
			return false
		}
		if finishCatchUp {
			target := e.getCatchUpTarget()
			if target > 0 && e.getLastBlock() < target {
				return false
			}
		}
		if len(buffered) > 0 {
			sort.SliceStable(buffered, func(i, j int) bool {
				if buffered[i].BlockNumber != buffered[j].BlockNumber {
					return buffered[i].BlockNumber < buffered[j].BlockNumber
				}
				if buffered[i].TxIndex != buffered[j].TxIndex {
					return buffered[i].TxIndex < buffered[j].TxIndex
				}
				return buffered[i].Index < buffered[j].Index
			})
			for _, vLog := range buffered {
				e.dispatchLog(vLog, "subscription-buffer")
			}
			e.logger.Infof("catch-up stream buffer flushed: logs=%d", len(buffered))
			buffered = buffered[:0]
		}
		if !finishCatchUp {
			e.advanceCommittedThrough()
			return true
		}
		e.catchUpTarget.Store(0)
		e.catchingUp.Store(false)
		e.setStreaming(true)
		e.advanceCommittedThrough()
		e.logger.Infof("catch-up complete: scanned_through=%d committed_through=%d delivered=%d",
			e.getLastBlock(), e.committedThrough.Load(), e.deliveredBlock.Load())
		return true
	}
	for {
		select {
		case <-ctx.Done():
			return
		case vLog := <-e.logCh:
			e.lastStreamLogUnixNano.Store(time.Now().UnixNano())
			if e.catchingUp.Load() {
				if len(buffered) < streamCatchUpBuffer {
					buffered = append(buffered, vLog)
				} else {
					// Apply backpressure instead of dropping. If the provider's own
					// queue overflows, its error triggers subscription recovery.
					e.logger.Warnf("catch-up stream buffer full: capacity=%d block=%d; applying backpressure",
						streamCatchUpBuffer, vLog.BlockNumber)
					for e.catchingUp.Load() {
						select {
						case <-ctx.Done():
							return
						case <-e.catchUpDoneCh:
							flushBuffered(true)
						}
					}
					e.dispatchLog(vLog, "subscription-buffer")
				}
				continue
			}
			flushBuffered(false)
			if e.debug && len(vLog.Topics) > 0 {
				e.logger.Debugf("raw log: block=%d tx=%s topic0=%s",
					vLog.BlockNumber, vLog.TxHash.Hex(), vLog.Topics[0].Hex())
			}
			e.dispatchLog(vLog, "subscription")
		case failure := <-e.subFailures:
			if !e.isCurrentSubscription(failure.Generation) {
				e.logger.Debugf("ignore stale subscription failure: generation=%d current=%d err=%v",
					failure.Generation, e.currentSubscriptionGeneration(), failure.Err)
				continue
			}
			// The blocks missed from now on are unknown, freeze the progress
			// until the stream is back and the gap is scanned.
			e.setStreaming(false)
			e.beginCatchUp(e.latestHead.Load())
			e.logger.Errorf("log subscription broken: generation=%d err=%v", failure.Generation, failure.Err)
			e.startResubscribe()
		case <-e.catchUpDoneCh:
			flushBuffered(true)
		case <-e.reloadCh:
			e.setStreaming(false)
			e.beginCatchUp(e.latestHead.Load())
			if err := e.subscribe(); err != nil {
				e.logger.Errorf("failed to apply the new watcher list: %v", err)
				e.startResubscribe()
				continue
			}
			head, err := e.blockNumber(ctx)
			if err != nil {
				e.logger.Errorf("failed to get reload catch-up head: %v", err)
				e.startResubscribe()
				continue
			}
			target := e.catchUpHead(head)
			if target > e.getLastBlock() {
				e.beginCatchUp(target)
				e.startBackfill(target)
			} else {
				e.endCatchUp()
			}
		}
	}
}

// checkpointLoop keeps the RPC connection alive with periodic BlockNumber
// calls and heals gaps when the WSS filter stops pushing logs.
func (e *EVMWatcher) checkpointLoop() {
	defer e.wg.Done()

	ctx := e.context()
	hbInterval := e.gapHealInterval
	if e.confirmations > 0 {
		hbInterval = e.pollInterval
	}
	heartbeat := time.NewTicker(hbInterval)
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
			head, err := e.blockNumber(ctx)
			if err != nil {
				e.logger.Errorf("failed to get block number: %v", err)
				continue
			}
			if e.confirmations == 0 {
				e.maybeGapHeal(ctx, head)
			}
			e.logger.Infof("checkpoint tick: head=%d scanned_through=%d committed_through=%d delivered=%d pending=%d reported=%d streaming=%v catching_up=%v last_stream_log_unix_nano=%d",
				head, e.getLastBlock(), e.committedThrough.Load(), e.deliveredBlock.Load(), e.pendingEvents.Load(), e.getReportedBlock(), e.isStreaming(), e.catchingUp.Load(), e.lastStreamLogUnixNano.Load())
		}
	}
}

// catchUpHead returns the backfill target for catch-up: raw head when
// confirmations=0, safeHead(head) when confirmations>0.
func (e *EVMWatcher) catchUpHead(head uint64) uint64 {
	if e.confirmations == 0 {
		return head
	}
	return e.safeHead(head)
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
	if e.catchingUp.Load() {
		e.logger.Infof("heartbeat: catching_up head=%d lastBlock=%d target=%d",
			head, e.getLastBlock(), e.getCatchUpTarget())
		return
	}
	if e.confirmations > 0 {
		safe := e.safeHead(head)
		e.maybeGapHeal(ctx, head)
		e.reportWatermark(e.committedThrough.Load())
		e.logger.Infof("heartbeat: head=%d safeHead=%d scanned_through=%d committed_through=%d pending=%d reported=%d",
			head, safe, e.getLastBlock(), e.committedThrough.Load(), e.pendingEvents.Load(), e.getReportedBlock())
		return
	}
	// Live mode (confirmations=0): never push watermark past delivered without
	// a FilterLogs scan; heal gaps when chain head runs ahead of delivery.
	if head > e.getLastBlock() {
		e.maybeGapHeal(ctx, head)
		return
	}
	e.reportWatermark(e.committedThrough.Load())
	e.logger.Infof("heartbeat: head=%d delivered=%d reported=%d",
		head, e.deliveredBlock.Load(), e.getReportedBlock())
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

// validateStoredWatermark rejects progress that cannot belong to the current
// chain state. In confirmation mode the persisted watermark must also not be
// ahead of the current safe head, otherwise restarting could skip unconfirmed
// blocks after the confirmation policy changes.
func validateStoredWatermark(chainName string, stored int64, head, startAt uint64) error {
	if stored < 0 {
		return nil
	}
	watermark := uint64(stored)
	if watermark > head {
		return fmt.Errorf("stored watermark %d is ahead of chain head %d for chain %s",
			stored, head, chainName)
	}
	if watermark > startAt {
		return fmt.Errorf("stored watermark %d is ahead of safe head %d (chain head %d) for chain %s",
			stored, startAt, head, chainName)
	}
	return nil
}

// applyGapStrategy jumps over historical blocks when the stored gap exceeds
// skipGapThreshold.
func (e *EVMWatcher) applyGapStrategy(head uint64, stored int64) {
	if stored < 0 || e.skipGapThreshold == 0 {
		return
	}
	skipTo := e.catchUpHead(head)
	// Startup validation normally guarantees this. Keep the strategy safe on
	// its own as well: subtracting a watermark ahead of the target would wrap.
	if uint64(stored) >= skipTo {
		return
	}
	gap := skipTo - uint64(stored)
	if gap <= e.skipGapThreshold {
		return
	}
	e.logger.Warnf("skip gap: stored=%d head=%d gap=%d threshold=%d skip_to=%d",
		stored, head, gap, e.skipGapThreshold, skipTo)
	e.setLastBlock(skipTo)
	e.deliveredBlock.Store(skipTo)
	e.committedThrough.Store(skipTo)
	e.reportWatermark(skipTo)
}

// beginCatchUp marks startup/resubscribe backfill in progress. WSS logs are
// buffered by eventLoop until the scan and stream can be merged and deduplicated.
func (e *EVMWatcher) beginCatchUp(head uint64) {
	if head == 0 {
		return
	}
	for {
		current := e.getCatchUpTarget()
		if current >= head {
			break
		}
		if e.catchUpTarget.CompareAndSwap(current, head) {
			break
		}
	}
	e.catchingUp.Store(true)
	e.setStreaming(false)
}

func (e *EVMWatcher) getCatchUpTarget() uint64 {
	return e.catchUpTarget.Load()
}

// endCatchUp asks eventLoop to merge buffered WSS logs and complete the mode
// switch. eventLoop keeps catchingUp true until every buffered log is registered.
func (e *EVMWatcher) endCatchUp() {
	target := e.getCatchUpTarget()
	if target > 0 && e.getLastBlock() < target {
		return
	}
	select {
	case e.catchUpDoneCh <- struct{}{}:
	default:
	}
}

// effectiveBackfillHeadFrom caps the scan target for one backfill run starting at from.
func (e *EVMWatcher) effectiveBackfillHeadFrom(from, head uint64) uint64 {
	if from > head {
		return head
	}
	if e.maxCatchUpBlocks == 0 {
		return head
	}
	maxTo := from + e.maxCatchUpBlocks - 1
	if maxTo > head {
		return head
	}
	return maxTo
}

// maybeGapHeal scans [scannedThrough+1, safeHead] while buffering WSS logs.
// Safe to call from heartbeat/checkpoint; backfillMu serializes work.
func (e *EVMWatcher) maybeGapHeal(ctx context.Context, head uint64) {
	if e.catchingUp.Load() || !e.isStreaming() {
		return
	}
	target := e.catchUpHead(head)
	if target <= e.getLastBlock() {
		return
	}
	from := e.getLastBlock() + 1
	e.logger.Infof("gap heal: delivered=%d head=%d lastBlock=%d scan=[%d,%d]",
		e.deliveredBlock.Load(), head, e.getLastBlock(), from, e.effectiveBackfillHeadFrom(from, target))
	if !e.gapHealRunning.CompareAndSwap(false, true) {
		return
	}
	e.beginCatchUp(target)
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		defer e.gapHealRunning.Store(false)
		for {
			if err := e.runCatchUp(ctx, target); err != nil {
				if ctx.Err() != nil {
					return
				}
				e.logger.Errorf("gap heal failed [%d,%d]: %v", from, target, err)
				if !waitContext(ctx, time.Second) {
					return
				}
				continue
			}
			e.endCatchUp()
			return
		}
	}()
}

// startBackfill runs catch-up backfill in the background until the target head
// is reached, then switches to live mode.
func (e *EVMWatcher) startBackfill(target uint64) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		ctx := e.context()
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			head, err := e.blockNumber(ctx)
			if err != nil {
				e.logger.Errorf("catch-up: failed to get block number: %v", err)
				if !waitContext(ctx, time.Second) {
					return
				}
				continue
			}
			newTarget := e.catchUpHead(head)
			if newTarget == 0 {
				if !waitContext(ctx, e.pollInterval) {
					return
				}
				continue
			}
			if newTarget > target {
				target = newTarget
				e.beginCatchUp(target)
			}
			if e.getLastBlock() >= target {
				break
			}
			if err := e.backfill(ctx, target); err != nil {
				if ctx.Err() != nil {
					return
				}
				e.logger.Errorf("catch-up backfill failed up to %d: %v", target, err)
				if !waitContext(ctx, time.Second) {
					return
				}
				continue
			}
			if e.getLastBlock() >= target {
				break
			}
		}
		e.endCatchUp()
	}()
}

// runCatchUp blocks until backfill reaches target, used during resubscribe.
func (e *EVMWatcher) runCatchUp(ctx context.Context, target uint64) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		head, err := e.blockNumber(ctx)
		if err != nil {
			return err
		}
		newTarget := e.catchUpHead(head)
		if newTarget == 0 {
			if !waitContext(ctx, e.pollInterval) {
				return ctx.Err()
			}
			continue
		}
		if newTarget > target {
			target = newTarget
			e.beginCatchUp(target)
		}
		if e.getLastBlock() >= target {
			return nil
		}
		if err := e.backfill(ctx, target); err != nil {
			return err
		}
	}
}

func waitContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// backfill scans from the recorded progress up to head in batches.
func (e *EVMWatcher) backfill(ctx context.Context, head uint64) error {
	return e.backfillFrom(ctx, e.getLastBlock()+1, head)
}

// backfillFrom scans [from, head] in batches. The scan cursor is tracked locally
// and lastBlock advances only after a complete FilterLogs batch succeeds.
func (e *EVMWatcher) backfillFrom(ctx context.Context, from, head uint64) error {
	e.backfillMu.Lock()
	defer e.backfillMu.Unlock()

	if from > head {
		return nil
	}

	cursor := from - 1
	target := e.effectiveBackfillHeadFrom(from, head)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		nextFrom := cursor + 1
		if nextFrom > target {
			if target < head {
				e.logger.Infof("backfill partial: scanned_to=%d chain_head=%d max_catch_up=%d",
					cursor, head, e.maxCatchUpBlocks)
			}
			return nil
		}
		to := nextFrom + e.getMaxBlockRange() - 1
		if to > target {
			to = target
		}

		e.logger.Infof("backfill batch start: from=%d to=%d target_head=%d", nextFrom, to, target)

		logs, scanned, err := e.filterLogs(ctx, nextFrom, to)
		if err != nil {
			return err
		}
		sort.SliceStable(logs, func(i, j int) bool {
			if logs[i].BlockNumber != logs[j].BlockNumber {
				return logs[i].BlockNumber < logs[j].BlockNumber
			}
			if logs[i].TxIndex != logs[j].TxIndex {
				return logs[i].TxIndex < logs[j].TxIndex
			}
			return logs[i].Index < logs[j].Index
		})
		for _, vLog := range logs {
			e.dispatchLog(vLog, "backfill")
		}

		e.logger.Infof("backfill batch done: from=%d to=%d logs=%d scanned_to=%d", nextFrom, to, len(logs), scanned)
		cursor = scanned
		if scanned > e.getLastBlock() {
			e.setLastBlock(scanned)
		}
		e.reportScannedWatermark(scanned)
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
		case errors.Is(err, context.DeadlineExceeded) && to > from:
			origTo := to
			span := (origTo - from + 1) / 2
			if span == 0 {
				span = 1
			}
			e.setMaxBlockRange(span)
			to = from + span - 1
			e.logger.Warnf("filterLogs timed out on [%d, %d], shrink range to %d blocks", from, origTo, span)
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
	rpcCtx, cancel := context.WithTimeout(ctx, blockNumberTimeout)
	defer cancel()
	head, err := e.client().BlockNumber(rpcCtx)
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
	e.dispatchLog(vLog, "unknown")
}

func (e *EVMWatcher) dispatchLog(vLog types.Log, source string) {
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
	if !e.registerEvent(ev) {
		e.logger.Debugf("deduplicated: source=%s event=%s block=%d tx=%s log_index=%d removed=%v",
			source, event.Name, vLog.BlockNumber, vLog.TxHash.Hex(), vLog.Index, vLog.Removed)
		return
	}
	select {
	case e.eventCh <- ev:
		e.logger.Infof("dispatch: source=%s event=%s block=%d tx=%s log_index=%d removed=%v",
			source, event.Name, vLog.BlockNumber, vLog.TxHash.Hex(), vLog.Index, vLog.Removed)
	case <-e.context().Done():
		// Keep the event pending so the committed watermark cannot cross it.
		return
	}
}

func keyForEvent(event *Event) eventKey {
	return eventKey{
		BlockHash: event.BlockHash,
		TxHash:    event.TxHash,
		Address:   event.ContractAddress,
		LogIndex:  event.LogIndex,
		Removed:   event.Removed,
	}
}

func (e *EVMWatcher) registerEvent(event *Event) bool {
	key := keyForEvent(event)
	e.progressMu.Lock()
	if _, exists := e.seenEvents[key]; exists {
		e.progressMu.Unlock()
		return false
	}
	e.seenEvents[key] = event.BlockNumber
	e.pendingByBlock[event.BlockNumber]++
	e.progressMu.Unlock()
	e.pendingEvents.Add(1)
	return true
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
			return
		}
	}
}

func (e *EVMWatcher) notify(event *Event) {
	ctx := e.context()
	for e.confirmations > 0 && event.BlockNumber > e.safeHead(e.latestHead.Load()) {
		e.logger.Infof("held for confirmation: event=%s block=%d safeHead=%d",
			event.EventName, event.BlockNumber, e.safeHead(e.latestHead.Load()))
		select {
		case <-ctx.Done():
			return
		case <-time.After(e.pollInterval):
		}
	}
	e.deliverEvent(event)
}

func (e *EVMWatcher) deliverEvent(event *Event) {
	ctx := e.context()
	backoff := deliveryRetryInitial
	attempt := 0
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		attempt++
		err := e.event.OnEvent(e.chainName, event)
		if err == nil {
			break
		}
		e.logger.Errorf("failed to notify event %s of %s: attempt=%d retry_in=%s err=%v",
			event.EventName, event.ContractAddress.Hex(), attempt, backoff, err)
		e.logger.Errorf("delivery pending: block=%d tx=%s log_index=%d pending=%d",
			event.BlockNumber, event.TxHash.Hex(), event.LogIndex, e.pendingEvents.Load())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		if backoff < deliveryRetryMax {
			backoff *= 2
			if backoff > deliveryRetryMax {
				backoff = deliveryRetryMax
			}
		}
	}
	e.pendingEvents.Add(-1)
	e.logger.Infof("delivered: event=%s block=%d tx=%s",
		event.EventName, event.BlockNumber, event.TxHash.Hex())
	for current := e.deliveredBlock.Load(); event.BlockNumber > current; current = e.deliveredBlock.Load() {
		if e.deliveredBlock.CompareAndSwap(current, event.BlockNumber) {
			break
		}
	}
	e.progressMu.Lock()
	if count := e.pendingByBlock[event.BlockNumber]; count <= 1 {
		delete(e.pendingByBlock, event.BlockNumber)
	} else {
		e.pendingByBlock[event.BlockNumber] = count - 1
	}
	e.progressMu.Unlock()
	e.advanceCommittedThrough()
}

func (e *EVMWatcher) advanceCommittedThrough() {
	// During catch-up, eventLoop must first merge/register its buffered live
	// logs. Persisting the scan cursor before that merge recreates a loss window.
	if e.catchingUp.Load() {
		return
	}
	scanned := e.getLastBlock()
	candidate := scanned
	e.progressMu.Lock()
	for block, count := range e.pendingByBlock {
		if count > 0 && block <= candidate {
			if block == 0 {
				candidate = 0
			} else {
				candidate = block - 1
			}
		}
	}
	if candidate > e.committedThrough.Load() {
		e.committedThrough.Store(candidate)
	}
	if candidate > dedupeRetentionBlocks {
		pruneThrough := candidate - dedupeRetentionBlocks
		for key, block := range e.seenEvents {
			if block <= pruneThrough {
				delete(e.seenEvents, key)
			}
		}
	}
	e.progressMu.Unlock()
	e.reportWatermark(e.committedThrough.Load())
}

// reportScannedWatermark persists progress after a FilterLogs scan.
func (e *EVMWatcher) reportScannedWatermark(_ uint64) {
	e.advanceCommittedThrough()
}

// reportWatermark persists at most committedThrough. A failed/queued event keeps
// committedThrough below its block, so a crash cannot skip that event.
func (e *EVMWatcher) reportWatermark(candidate uint64) {
	committed := e.committedThrough.Load()
	if candidate > committed {
		candidate = committed
	}
	if e.confirmations > 0 {
		safe := e.safeHead(e.latestHead.Load())
		if safe == 0 {
			return
		}
		if candidate > safe {
			candidate = safe
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
	e.subscribeMu.Lock()
	defer e.subscribeMu.Unlock()

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
	subscribeCtx, cancel := context.WithTimeout(ctx, filterLogsTimeout)
	sub, err := client.SubscribeFilterLogs(subscribeCtx, query, logCh)
	cancel()
	if err != nil {
		e.logger.Errorf("subscription failed: %v", err)
		return fmt.Errorf("failed to subscribe logs: %w", err)
	}

	generation, old := e.installSubscription(sub)
	if old != nil {
		old.Unsubscribe()
	}
	e.forwardSubscriptionErrors(generation, sub)
	e.logger.Infof("subscription established: generation=%d", generation)
	return nil
}

func (e *EVMWatcher) installSubscription(sub ethereum.Subscription) (uint64, ethereum.Subscription) {
	e.mu.Lock()
	defer e.mu.Unlock()
	old := e.sub
	e.subGeneration++
	e.sub = sub
	return e.subGeneration, old
}

func (e *EVMWatcher) forwardSubscriptionErrors(generation uint64, sub ethereum.Subscription) {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		var err error
		select {
		case <-e.context().Done():
			return
		case err = <-sub.Err():
			if err == nil {
				err = errors.New("subscription error channel closed")
			}
		}
		failure := subscriptionFailure{Generation: generation, Err: err}
		select {
		case e.subFailures <- failure:
		case <-e.context().Done():
		}
	}()
}

func (e *EVMWatcher) isCurrentSubscription(generation uint64) bool {
	return generation == e.currentSubscriptionGeneration()
}

func (e *EVMWatcher) currentSubscriptionGeneration() uint64 {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.subGeneration
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
	delay := resubscribeDelay
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		if delay < maxResubscribeDelay {
			delay *= 2
			if delay > maxResubscribeDelay {
				delay = maxResubscribeDelay
			}
		}

		head, err := e.blockNumber(ctx)
		if err != nil {
			e.logger.Errorf("failed to get block number: %v", err)
			continue
		}
		catchTarget := e.catchUpHead(head)
		if catchTarget > 0 {
			e.beginCatchUp(catchTarget)
		}
		if err := e.subscribe(); err != nil {
			e.logger.Errorf("failed to resubscribe logs: %v", err)
			continue
		}
		if head, err := e.blockNumber(ctx); err != nil {
			e.logger.Errorf("failed to get block number after resubscribe: %v", err)
			continue
		} else {
			catchTarget = e.catchUpHead(head)
			if catchTarget > 0 && e.getLastBlock() < catchTarget {
				e.beginCatchUp(catchTarget)
				if err := e.runCatchUp(ctx, catchTarget); err != nil {
					e.logger.Errorf("failed to catch up after resubscribe: %v", err)
					continue
				}
			}
		}

		e.endCatchUp()
		e.logger.Infof("log subscription recovery ready: scanned_through=%d; waiting for buffered-log merge",
			e.getLastBlock())
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
