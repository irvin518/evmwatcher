package evmwatcher

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// ChainBackfillProgress identifies the last fully acknowledged block and the
// member set which must participate in the next scan.
type ChainBackfillProgress struct {
	Exists     bool
	Block      uint64
	Membership [32]byte
	// Manifest records members so that additions can be verified on restart.
	Manifest string
}

// ChainBackfillStorage is implemented by the caller. CompareAndSwap must be a
// durable atomic comparison of the complete old value (including Manifest) and
// write of the complete new value, including the uninitialized state. Return an
// error on mismatch.
type ChainBackfillStorage interface {
	LoadChainBackfill(context.Context, string) (ChainBackfillProgress, error)
	CompareAndSwapChainBackfill(context.Context, string, ChainBackfillProgress, ChainBackfillProgress) error
}

// ChainBackfillConfig describes a single chain and an immutable member set.
// StartBlock is inclusive and must be explicitly supplied for an empty store.
type ChainBackfillConfig struct {
	ChainID, RPCURL, ProgressKey string
	StartBlock                   *uint64
	Confirmations                uint64
	Interval                     time.Duration
	MaxBlockRange                uint64
	MaxAddresses                 int
	Storage                      ChainBackfillStorage
	Logger                       Logger
}
type chainBackfillMember struct {
	watcher *EVMWatcher
	targets map[common.Address]*watchTarget
	data    []WatcherData
}

// ChainBackfiller scans one chain for all registered watchers. The RPC client is
// dialed for each ScanOnce and closed after that pass; watcher WS clients stay
// independent. It never owns business storage or assumes an in-memory queue is
// a durable acknowledgement.
type ChainBackfiller struct {
	cfg         ChainBackfillConfig
	mu          sync.Mutex
	lifecycleMu sync.Mutex
	members     []chainBackfillMember
	sealed      bool
	running     bool
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	scanGate    chan struct{}
	errors      chan error
	dial        func(context.Context, string) (*ethclient.Client, error)
}

func NewChainBackfiller(cfg ChainBackfillConfig) (*ChainBackfiller, error) {
	id, ok := new(big.Int).SetString(cfg.ChainID, 10)
	if !ok || id.Sign() <= 0 || id.String() != cfg.ChainID || cfg.RPCURL == "" || cfg.ProgressKey == "" || cfg.StartBlock == nil || *cfg.StartBlock > math.MaxInt64 || cfg.Interval <= 0 || cfg.MaxBlockRange == 0 || cfg.MaxAddresses <= 0 || cfg.Storage == nil {
		return nil, errors.New("invalid chain backfill configuration")
	}
	start := *cfg.StartBlock
	cfg.StartBlock = &start
	if cfg.Logger == nil {
		cfg.Logger = defaultLogger(cfg.ChainID)
	}
	return &ChainBackfiller{cfg: cfg, errors: make(chan error, 16), dial: ethclient.DialContext, scanGate: make(chan struct{}, 1)}, nil
}

// RegisterWatcher attaches a watcher before either component starts. Targets are
// immutable during a run; on restart, additional watchers join at the next
// block after the last completed watermark.
func (c *ChainBackfiller) RegisterWatcher(w *EVMWatcher) error {
	if c == nil || w == nil {
		return errors.New("backfiller and watcher required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sealed {
		return errors.New("chain backfill membership already sealed")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.started || w.starting || w.chainBackfiller != nil || w.event == nil || w.chainName == "" {
		return errors.New("watcher must be unstarted, named and have an event handler")
	}
	if w.confirmations != c.cfg.Confirmations {
		return errors.New("watcher confirmation policy mismatch")
	}
	if w.skipGapThreshold != 0 || w.sharedLogProvider != nil || w.blockNumberProvider != nil {
		return errors.New("watcher gap skipping, shared query and shared head providers are incompatible with chain backfill")
	}
	for _, member := range c.members {
		if member.watcher.chainName == w.chainName {
			return errors.New("duplicate watcher identity")
		}
	}
	targets := make(map[common.Address]*watchTarget)
	data := cloneChainWatcherData(w.watcherData)
	for _, item := range data {
		target, err := newWatchTarget(item)
		if err != nil {
			return err
		}
		if _, ok := targets[target.address]; ok {
			return errors.New("duplicate watcher contract")
		}
		targets[target.address] = target
	}
	if len(targets) == 0 {
		return errors.New("watcher requires contracts")
	}
	w.watcherData = cloneChainWatcherData(data)
	w.chainBackfiller = c
	c.members = append(c.members, chainBackfillMember{watcher: w, targets: targets, data: data})
	return nil
}

// Errors reports asynchronous scan failures. It is buffered and may drop
// repeated errors; the configured logger also receives every failure.
func (c *ChainBackfiller) Errors() <-chan error { return c.errors }
func (c *ChainBackfiller) seal() ([]chainBackfillMember, [32]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.members) == 0 {
		return nil, [32]byte{}, errors.New("no registered watchers")
	}
	members := append([]chainBackfillMember(nil), c.members...)
	for _, m := range members {
		m.watcher.mu.RLock()
		started := m.watcher.started && !m.watcher.starting
		m.watcher.mu.RUnlock()
		if !started {
			return nil, [32]byte{}, fmt.Errorf("watcher %s is not started", m.watcher.chainName)
		}
	}
	c.sealed = true
	return members, chainMembership(c.cfg, members), nil
}
func cloneChainWatcherData(data []WatcherData) []WatcherData {
	out := make([]WatcherData, len(data))
	for i, item := range data {
		out[i] = item
		out[i].EventNames = append([]string(nil), item.EventNames...)
	}
	return out
}
func chainMembership(cfg ChainBackfillConfig, members []chainBackfillMember) [32]byte {
	parts := make([]string, 0, len(members))
	for _, member := range members {
		h := sha256.New()
		writeChainPart(h, member.watcher.chainName)
		data := append([]WatcherData(nil), member.data...)
		sort.Slice(data, func(i, j int) bool { return data[i].ContractAddress < data[j].ContractAddress })
		for _, item := range data {
			writeChainPart(h, common.HexToAddress(item.ContractAddress).Hex())
			writeChainPart(h, item.ContractABI)
			names := append([]string(nil), item.EventNames...)
			sort.Strings(names)
			for _, name := range names {
				writeChainPart(h, name)
			}
		}
		parts = append(parts, string(h.Sum(nil)))
	}
	sort.Strings(parts)
	h := sha256.New()
	writeChainPart(h, cfg.ChainID)
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], cfg.Confirmations)
	_, _ = h.Write(n[:])
	binary.BigEndian.PutUint64(n[:], *cfg.StartBlock)
	_, _ = h.Write(n[:])
	for _, part := range parts {
		writeChainPart(h, part)
	}
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}

// reconcileMembership allows additions only. The manifest is persisted with the
// watermark using the same CAS, so a restart never loses the old member set.
func (c *ChainBackfiller) reconcileMembership(ctx context.Context, progress ChainBackfillProgress, members []chainBackfillMember, membership [32]byte) (ChainBackfillProgress, string, error) {
	names := make([]string, 0, len(members))
	byName := make(map[string]chainBackfillMember, len(members))
	for _, m := range members {
		names = append(names, m.watcher.chainName)
		byName[m.watcher.chainName] = m
	}
	sort.Strings(names)
	encoded, err := json.Marshal(names)
	if err != nil {
		return progress, "", err
	}
	manifest := string(encoded)
	if !progress.Exists {
		return progress, manifest, nil
	}
	if progress.Manifest == "" {
		// Older records have no recoverable member list; migrate only if the
		// complete configured set is exactly the one originally recorded.
		if progress.Membership != membership {
			return progress, "", errors.New("legacy chain membership differs; restore original members before migration")
		}
	} else {
		var previous []string
		if err := json.Unmarshal([]byte(progress.Manifest), &previous); err != nil || len(previous) == 0 {
			return progress, "", errors.New("invalid chain membership manifest")
		}
		oldMembers := make([]chainBackfillMember, 0, len(previous))
		seen := make(map[string]bool, len(previous))
		for _, name := range previous {
			m, ok := byName[name]
			if !ok || seen[name] {
				return progress, "", fmt.Errorf("previous chain member %q missing or duplicated", name)
			}
			seen[name] = true
			oldMembers = append(oldMembers, m)
		}
		if chainMembership(c.cfg, oldMembers) != progress.Membership {
			return progress, "", errors.New("previous chain member configuration or scan policy changed")
		}
	}
	if progress.Membership != membership || progress.Manifest != manifest {
		next := progress
		next.Membership, next.Manifest = membership, manifest
		if err := c.cfg.Storage.CompareAndSwapChainBackfill(ctx, c.cfg.ProgressKey, progress, next); err != nil {
			return progress, "", fmt.Errorf("commit chain membership: %w", err)
		}
		progress = next
	}
	return progress, manifest, nil
}

type chainHashWriter interface{ Write([]byte) (int, error) }

func writeChainPart(h chainHashWriter, value string) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(value)))
	_, _ = h.Write(n[:])
	_, _ = h.Write([]byte(value))
}

// Start runs periodic scans until Stop or parent cancellation. Start watchers
// first, then start the backfiller. Errors never advance progress.
func (c *ChainBackfiller) Start(parent context.Context) error {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	if parent == nil {
		return errors.New("parent context is required")
	}
	if _, _, err := c.seal(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running {
		return errors.New("chain backfiller already running")
	}
	ctx, cancel := context.WithCancel(parent)
	c.cancel = cancel
	c.running = true
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		defer func() { c.mu.Lock(); c.running = false; c.mu.Unlock() }()
		ticker := time.NewTicker(c.cfg.Interval)
		defer ticker.Stop()
		for {
			if err := c.ScanOnce(ctx); err != nil && ctx.Err() == nil {
				c.cfg.Logger.Errorf("chain backfill: %v", err)
				select {
				case c.errors <- err:
				default:
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return nil
}
func (c *ChainBackfiller) Stop() {
	c.lifecycleMu.Lock()
	defer c.lifecycleMu.Unlock()
	c.mu.Lock()
	cancel := c.cancel
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	c.wg.Wait()
}

// ScanOnce performs at most one bounded block range. It is safe to call while
// Start is running; calls are serialized and always reload durable progress.
func (c *ChainBackfiller) ScanOnce(ctx context.Context) error {
	select {
	case c.scanGate <- struct{}{}:
		defer func() { <-c.scanGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	members, membership, err := c.seal()
	if err != nil {
		return err
	}
	progress, err := c.cfg.Storage.LoadChainBackfill(ctx, c.cfg.ProgressKey)
	if err != nil {
		return fmt.Errorf("load chain backfill progress: %w", err)
	}
	progress, manifest, err := c.reconcileMembership(ctx, progress, members, membership)
	if err != nil {
		return err
	}
	from := *c.cfg.StartBlock
	if progress.Exists {
		if progress.Block == math.MaxUint64 {
			return errors.New("chain progress overflow")
		}
		from = progress.Block + 1
	}
	client, err := c.dial(ctx, c.cfg.RPCURL)
	if err != nil {
		return fmt.Errorf("dial backfill RPC: %w", err)
	}
	defer client.Close()
	rpcCtx, cancel := context.WithTimeout(ctx, blockNumberTimeout)
	chainID, err := client.ChainID(rpcCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("read backfill chain ID: %w", err)
	}
	if chainID == nil || chainID.String() != c.cfg.ChainID {
		return fmt.Errorf("backfill RPC chain ID mismatch")
	}
	rpcCtx, cancel = context.WithTimeout(ctx, blockNumberTimeout)
	head, err := client.BlockNumber(rpcCtx)
	cancel()
	if err != nil {
		return fmt.Errorf("read backfill head: %w", err)
	}
	if progress.Exists && progress.Block > head {
		return errors.New("persisted chain backfill progress is ahead of chain head")
	}
	if head < c.cfg.Confirmations {
		return nil
	}
	safe := head - c.cfg.Confirmations
	if progress.Exists && progress.Block > safe {
		return errors.New("persisted chain backfill progress is ahead of confirmed head")
	}
	if from > safe {
		return nil
	}
	to := safe
	if safe-from >= c.cfg.MaxBlockRange {
		to = from + c.cfg.MaxBlockRange - 1
	}
	logs, scanned, err := c.filterRange(ctx, client, members, from, to)
	if err != nil {
		return err
	}
	sort.SliceStable(logs, func(i, j int) bool {
		a, b := logs[i], logs[j]
		if a.BlockNumber != b.BlockNumber {
			return a.BlockNumber < b.BlockNumber
		}
		if a.TxIndex != b.TxIndex {
			return a.TxIndex < b.TxIndex
		}
		return a.Index < b.Index
	})
	for _, log := range logs {
		for _, member := range members {
			target := member.targets[log.Address]
			if target == nil || len(log.Topics) == 0 {
				continue
			}
			if _, ok := target.events[log.Topics[0]]; !ok {
				continue
			}
			if err := member.watcher.deliverChainBackfill(ctx, log); err != nil {
				return fmt.Errorf("watcher %s block %d: %w", member.watcher.chainName, log.BlockNumber, err)
			}
		}
	}
	next := ChainBackfillProgress{Exists: true, Block: scanned, Membership: membership, Manifest: manifest}
	if err := c.cfg.Storage.CompareAndSwapChainBackfill(ctx, c.cfg.ProgressKey, progress, next); err != nil {
		return fmt.Errorf("commit chain backfill progress: %w", err)
	}
	// Advance only in-memory dedupe retention after the public watermark is
	// durable. Watcher-specific StorageInterface writes remain disabled.
	for _, member := range members {
		member.watcher.setLastBlock(scanned)
		member.watcher.advanceCommittedThrough()
	}
	return nil
}
func (c *ChainBackfiller) filterRange(ctx context.Context, client *ethclient.Client, members []chainBackfillMember, from, to uint64) ([]types.Log, uint64, error) {
	addressSet := make(map[common.Address]struct{})
	topicSet := make(map[common.Hash]struct{})
	for _, m := range members {
		for addr, target := range m.targets {
			addressSet[addr] = struct{}{}
			for topic := range target.events {
				topicSet[topic] = struct{}{}
			}
		}
	}
	addresses := make([]common.Address, 0, len(addressSet))
	for address := range addressSet {
		addresses = append(addresses, address)
	}
	sort.Slice(addresses, func(i, j int) bool { return string(addresses[i][:]) < string(addresses[j][:]) })
	topics := make([]common.Hash, 0, len(topicSet))
	for topic := range topicSet {
		topics = append(topics, topic)
	}
	for {
		all := make([]types.Log, 0)
		var scanErr error
		for start := 0; start < len(addresses); {
			end := start + min(c.cfg.MaxAddresses, len(addresses)-start)
			query := ethereum.FilterQuery{FromBlock: new(big.Int).SetUint64(from), ToBlock: new(big.Int).SetUint64(to), Addresses: addresses[start:end], Topics: [][]common.Hash{topics}}
			rpcCtx, cancel := context.WithTimeout(ctx, filterLogsTimeout)
			logs, err := client.FilterLogs(rpcCtx, query)
			cancel()
			if err != nil {
				scanErr = err
				break
			}
			// A JSON null result is not proof of an empty scan. A valid empty
			// getLogs response is an array and decodes to a non-nil slice.
			if logs == nil {
				return nil, 0, errors.New("getLogs returned null instead of an array")
			}
			for _, log := range logs {
				if log.BlockNumber < from || log.BlockNumber > to || !addressIn(query.Addresses, log.Address) || len(log.Topics) == 0 || !topicIn(topics, log.Topics[0]) {
					return nil, 0, errors.New("getLogs returned log outside requested range or filter")
				}
			}
			all = append(all, logs...)
			start = end
		}
		if scanErr == nil {
			return all, to, nil
		}
		if ctx.Err() != nil {
			return nil, 0, ctx.Err()
		}
		if to > from && (errors.Is(scanErr, context.DeadlineExceeded) || isBlockRangeError(scanErr) || isResultLimitError(scanErr)) {
			to = from + (to-from)/2
			continue
		}
		return nil, 0, fmt.Errorf("chain getLogs [%d,%d]: %w", from, to, scanErr)
	}
}

// deliverChainBackfill waits for the very same delivery used by WS. An event
// already queued or being retried is not considered acknowledged until OnEvent
// returns nil. Business handlers must durably accept before returning nil and
// deduplicate across restart or multiple processes.
func (e *EVMWatcher) deliverChainBackfill(ctx context.Context, log types.Log) error {
	e.mu.RLock()
	started, watcherCtx, eventCh := e.started, e.ctx, e.eventCh
	target := e.targets[log.Address]
	e.mu.RUnlock()
	if !started || watcherCtx == nil || eventCh == nil {
		return errors.New("watcher is not running")
	}
	if target == nil || len(log.Topics) == 0 {
		return errors.New("watcher target or topic missing")
	}
	event, ok := target.events[log.Topics[0]]
	if !ok {
		return errors.New("watcher event signature missing")
	}
	// Unlike the legacy dispatcher, public backfill must not advance on a
	// malformed event payload, including missing non-indexed data.
	if len(event.Inputs.NonIndexed()) > 0 {
		values := make(map[string]any)
		if err := target.parsed.UnpackIntoMap(values, event.Name, log.Data); err != nil {
			return fmt.Errorf("decode event: %w", err)
		}
	}
	args, err := decodeLog(target.parsed, event, log)
	if err != nil {
		return fmt.Errorf("decode event: %w", err)
	}
	ev := &Event{ContractAddress: log.Address, EventName: event.Name, BlockNumber: log.BlockNumber, BlockHash: log.BlockHash, TxHash: log.TxHash, TxIndex: log.TxIndex, LogIndex: log.Index, Args: args, Removed: log.Removed, Raw: log}
	// Prevent a stopped/restarted watcher from registering into its new dedupe
	// map while this call still holds the previous run's channel and context.
	e.mu.RLock()
	if !e.started || e.ctx != watcherCtx || e.eventCh != eventCh {
		e.mu.RUnlock()
		return errors.New("watcher changed while dispatching backfill")
	}
	added, done := e.registerEventWithAck(ev)
	e.mu.RUnlock()
	if added {
		select {
		case eventCh <- ev:
		case <-ctx.Done():
			e.abortChainBackfillEnqueue(ev, done, ctx.Err())
			return ctx.Err()
		case <-watcherCtx.Done():
			e.abortChainBackfillEnqueue(ev, done, watcherCtx.Err())
			return watcherCtx.Err()
		}
	}
	if done == nil {
		return nil
	}
	select {
	case <-done.done:
		return done.err
	case <-ctx.Done():
		return ctx.Err()
	case <-watcherCtx.Done():
		return watcherCtx.Err()
	}
}

func (e *EVMWatcher) abortChainBackfillEnqueue(event *Event, ack *eventDeliveryAck, err error) {
	key := keyForEvent(event)
	e.progressMu.Lock()
	defer e.progressMu.Unlock()
	if e.eventDone[key] != ack {
		return
	}
	delete(e.eventDone, key)
	delete(e.seenEvents, key)
	if count := e.pendingByBlock[event.BlockNumber]; count <= 1 {
		delete(e.pendingByBlock, event.BlockNumber)
	} else {
		e.pendingByBlock[event.BlockNumber] = count - 1
	}
	e.pendingEvents.Add(-1)
	ack.err = err
	close(ack.done)
}
