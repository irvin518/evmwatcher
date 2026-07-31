package evmwatcher

import (
	"context"
	"fmt"
	"log"
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
	logChannelBuffer   = 4096
	eventChannelBuffer = 4096
	resubscribeDelay   = 5 * time.Second
	// minimum gap between two delivery-driven watermark writes, keeps a busy
	// stream from turning into one storage write per block
	deliveryReportInterval = 5 * time.Second
	rateLimitBackoff       = time.Second
	maxRateLimitBackoff    = 30 * time.Second
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
		reloadCh:        make(chan struct{}, 1),
	}
	for _, option := range options {
		option(watcher)
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

	// Subscribe before catching up, the pushed logs are buffered in logCh so
	// that nothing is missed between the scan end and the stream start.
	if err := e.subscribe(); err != nil {
		e.release(cancel, client)
		return err
	}

	head, err := e.blockNumber(ctx)
	if err != nil {
		e.release(cancel, client)
		return fmt.Errorf("failed to get block number: %w", err)
	}

	stored, err := e.storage.GetWatchedBlockNumber(e.chainName)
	if err != nil {
		e.release(cancel, client)
		return fmt.Errorf("failed to get watched block number: %w", err)
	}
	if stored < 0 {
		// no progress recorded, watch from the current node head
		e.setLastBlock(head)
	} else {
		e.setLastBlock(uint64(stored))
	}
	// Everything up to the starting point was delivered by the previous run.
	initial := e.getLastBlock()
	e.deliveredBlock.Store(initial)
	e.reportMu.Lock()
	e.reportedBlock = initial
	e.reportMu.Unlock()

	// The node head is ahead of the recorded progress, scan the gap first.
	if err := e.backfill(ctx, head); err != nil {
		e.release(cancel, client)
		return err
	}

	e.mu.Lock()
	e.started = true
	e.streaming = true
	e.mu.Unlock()

	e.wg.Add(2)
	go e.eventLoop()
	go e.checkpointLoop()
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
	e.reportWatermark(e.getLastBlock())
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
	for {
		select {
		case <-ctx.Done():
			return
		case vLog := <-e.logCh:
			e.dispatch(vLog)
		case err := <-e.subError():
			// The blocks missed from now on are unknown, freeze the progress
			// until the stream is back and the gap is scanned.
			e.setStreaming(false)
			e.logf("log subscription broken: %v", err)
			e.resubscribe()
		case <-e.reloadCh:
			if err := e.subscribe(); err != nil {
				e.setStreaming(false)
				e.logf("failed to apply the new watcher list: %v", err)
				e.resubscribe()
			}
		}
	}
}

// checkpointLoop reports the node head to the caller periodically.
func (e *EVMWatcher) checkpointLoop() {
	defer e.wg.Done()

	ctx := e.context()
	ticker := time.NewTicker(checkpointInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// While the stream is down the covered range is unknown, reporting
			// the head would skip every missed block for good.
			if !e.isStreaming() {
				continue
			}
			head, err := e.blockNumber(ctx)
			if err != nil {
				e.logf("failed to get block number: %v", err)
				continue
			}
			e.setLastBlock(head)
			e.reportWatermark(head)
		}
	}
}

// backfill scans from the recorded progress up to head in batches.
func (e *EVMWatcher) backfill(ctx context.Context, head uint64) error {
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

		// The scanned range may be narrower than the requested one when the
		// provider rejects it.
		logs, scanned, err := e.filterLogs(ctx, from, to)
		if err != nil {
			return err
		}
		for _, vLog := range logs {
			e.dispatch(vLog)
		}

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
		logs, err := e.client().FilterLogs(ctx, query)
		if err == nil {
			return logs, to, nil
		}

		switch {
		case isRateLimitError(err):
			e.logf("rate limited on scanning [%d, %d], retry in %s", from, to, backoff)
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
			e.logf("block range rejected, shrink the range to %d blocks", span)
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
	return e.client().BlockNumber(ctx)
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
		return
	}
	// The node matches addresses and topics independently, which makes the
	// filter a cartesian product, so check the pair locally again.
	event, ok := target.events[vLog.Topics[0]]
	if !ok {
		return
	}

	args, err := decodeLog(target.parsed, event, vLog)
	if err != nil {
		e.logf("failed to decode event %s of %s: %v", event.Name, vLog.Address.Hex(), err)
		return
	}
	// Hand the event over to notifyLoop. Counted before sending so that the
	// event is part of the backlog even while the send blocks.
	e.pendingEvents.Add(1)
	select {
	case e.eventCh <- &Event{
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
	}:
	case <-e.context().Done():
		e.pendingEvents.Add(-1)
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
			// Deliver the already decoded events before leaving.
			for {
				select {
				case event := <-e.eventCh:
					e.notify(event)
				default:
					return
				}
			}
		}
	}
}

func (e *EVMWatcher) notify(event *Event) {
	err := e.event.OnEvent(e.chainName, event)
	e.pendingEvents.Add(-1)
	if err != nil {
		// Do not advance the watermark on failure, so a restart can rescan
		// this block and deliver the event again.
		e.logf("failed to notify event %s of %s: %v", event.EventName, event.ContractAddress.Hex(), err)
		return
	}
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
		e.logf("failed to save watched block number %d: %v", candidate, err)
		return
	}
	e.reportedBlock = candidate
}

func (e *EVMWatcher) subscribe() error {
	query := e.buildQuery()
	if len(query.Addresses) == 0 {
		return fmt.Errorf("no watcher data configured")
	}

	e.mu.RLock()
	client, ctx, logCh := e.ethClient, e.ctx, e.logCh
	e.mu.RUnlock()

	if err := e.waitRequestSlot(ctx); err != nil {
		return err
	}
	sub, err := client.SubscribeFilterLogs(ctx, query, logCh)
	if err != nil {
		return fmt.Errorf("failed to subscribe logs: %w", err)
	}

	e.mu.Lock()
	old := e.sub
	e.sub = sub
	e.mu.Unlock()
	if old != nil {
		old.Unsubscribe()
	}
	return nil
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

		if err := e.subscribe(); err != nil {
			e.logf("failed to resubscribe logs: %v", err)
			continue
		}
		head, err := e.blockNumber(ctx)
		if err != nil {
			e.logf("failed to get block number: %v", err)
			continue
		}
		// Subscribe first and scan afterwards, so the blocks produced during
		// the scan are already buffered in logCh.
		if err := e.backfill(ctx, head); err != nil {
			e.logf("failed to scan the missed blocks: %v", err)
			continue
		}

		e.setStreaming(true)
		e.logf("log subscription recovered at block %d", e.getLastBlock())
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

// logf prefixes the log with the chain name to tell the watchers apart.
func (e *EVMWatcher) logf(format string, args ...any) {
	log.Printf("evmwatcher: ["+e.chainName+"] "+format, args...)
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
