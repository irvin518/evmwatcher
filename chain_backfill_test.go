package evmwatcher

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

type chainTestStore struct {
	mu         sync.Mutex
	progress   ChainBackfillProgress
	commits    int
	failCommit bool
}

func (s *chainTestStore) LoadChainBackfill(context.Context, string) (ChainBackfillProgress, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.progress, nil
}
func (s *chainTestStore) CompareAndSwapChainBackfill(_ context.Context, _ string, old, next ChainBackfillProgress) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.progress != old {
		return errors.New("progress changed")
	}
	if s.failCommit {
		return errors.New("storage unavailable")
	}
	s.progress = next
	s.commits++
	return nil
}
func (s *chainTestStore) snapshot() (ChainBackfillProgress, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.progress, s.commits
}

type chainTestRPC struct {
	*headRPC
	mu2        sync.Mutex
	events     []types.Log
	getLogs    int
	failure    error
	failAt     int
	returnNull bool
}

func (s *chainTestRPC) ChainId(context.Context) (*hexutil.Big, error) {
	id := hexutil.Big(*big.NewInt(1672))
	return &id, nil
}
func (s *chainTestRPC) GetLogs(_ context.Context, q map[string]interface{}) ([]types.Log, error) {
	s.mu2.Lock()
	defer s.mu2.Unlock()
	s.getLogs++
	if s.returnNull {
		return nil, nil
	}
	if s.failAt > 0 && s.getLogs == s.failAt {
		return nil, errors.New("injected sub-batch failure")
	}
	if s.failure != nil {
		return nil, s.failure
	}
	from, err := hexutil.DecodeUint64(q["fromBlock"].(string))
	if err != nil {
		return nil, err
	}
	to, err := hexutil.DecodeUint64(q["toBlock"].(string))
	if err != nil {
		return nil, err
	}
	addresses := map[common.Address]bool{}
	switch v := q["address"].(type) {
	case []interface{}:
		for _, a := range v {
			addresses[common.HexToAddress(a.(string))] = true
		}
	case string:
		addresses[common.HexToAddress(v)] = true
	}
	out := []types.Log{}
	for _, event := range s.events {
		if addresses[event.Address] && event.BlockNumber >= from && event.BlockNumber <= to {
			out = append(out, event)
		}
	}
	return out, nil
}
func (s *chainTestRPC) count() int { s.mu2.Lock(); defer s.mu2.Unlock(); return s.getLogs }
func newChainTestWatcher(t *testing.T, server *rpc.Server, id, address string, handler EventInterface) *EVMWatcher {
	t.Helper()
	data := []WatcherData{{ContractAddress: address, ContractABI: `[{"anonymous":false,"inputs":[],"name":"Ping","type":"event"}]`}}
	w := NewEVMWatcher(id, "inproc", data, nil, handler, WithLogger(silentLogger{}))
	w.requestInterval = 0
	w.dialClient = func(string) (*ethclient.Client, error) { return ethclient.NewClient(rpc.DialInProc(server)), nil }
	return w
}
func newChainTestBackfiller(t *testing.T, server *rpc.Server, store ChainBackfillStorage, start uint64) *ChainBackfiller {
	t.Helper()
	scanner, err := NewChainBackfiller(ChainBackfillConfig{ChainID: "1672", RPCURL: "inproc", ProgressKey: "chain-1672", StartBlock: &start, Interval: time.Second, MaxBlockRange: 10, MaxAddresses: 10, Storage: store, Logger: silentLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	scanner.dial = func(context.Context, string) (*ethclient.Client, error) {
		return ethclient.NewClient(rpc.DialInProc(server)), nil
	}
	return scanner
}
func TestChainBackfillWaitsForBusinessAndDeduplicatesWS(t *testing.T) {
	a, b := common.HexToAddress("0x1"), common.HexToAddress("0x2")
	target, err := newWatchTarget(WatcherData{ContractAddress: a.Hex(), ContractABI: `[{"anonymous":false,"inputs":[],"name":"Ping","type":"event"}]`})
	if err != nil {
		t.Fatal(err)
	}
	var topic common.Hash
	for id := range target.events {
		topic = id
	}
	first := types.Log{Address: a, Topics: []common.Hash{topic}, BlockNumber: 10, BlockHash: common.HexToHash("0x10"), TxHash: common.HexToHash("0xa")}
	second := types.Log{Address: b, Topics: []common.Hash{topic}, BlockNumber: 10, BlockHash: common.HexToHash("0x10"), TxHash: common.HexToHash("0xb")}
	service := &chainTestRPC{headRPC: &headRPC{head: 10}, events: []types.Log{first, second}}
	server := rpc.NewServer()
	if err := server.RegisterName("eth", service); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	var mainCalls, vaultAttempts atomic.Int64
	var fail atomic.Bool
	fail.Store(true)
	main := newChainTestWatcher(t, server, "main", a.Hex(), eventHandlerFunc(func(string, *Event) error { mainCalls.Add(1); return nil }))
	vault := newChainTestWatcher(t, server, "vault", b.Hex(), eventHandlerFunc(func(string, *Event) error {
		vaultAttempts.Add(1)
		if fail.Load() {
			return errors.New("business failure")
		}
		return nil
	}))
	store := &chainTestStore{}
	scanner := newChainTestBackfiller(t, server, store, 10)
	if err := scanner.RegisterWatcher(main); err != nil {
		t.Fatal(err)
	}
	if err := scanner.RegisterWatcher(vault); err != nil {
		t.Fatal(err)
	}
	if err := main.Start(); err != nil {
		t.Fatal(err)
	}
	defer main.Stop()
	if err := vault.Start(); err != nil {
		t.Fatal(err)
	}
	defer vault.Stop()
	if service.count() != 0 {
		t.Fatal("watchers performed their own backfill")
	}
	// WS and public scan share the same pending/successful delivery record.
	main.dispatchLog(first, "subscription")
	vault.dispatchLog(second, "subscription")
	waitFor(t, func() bool { return mainCalls.Load() == 1 && vaultAttempts.Load() > 0 })
	done := make(chan error, 1)
	go func() { done <- scanner.ScanOnce(context.Background()) }()
	waitFor(t, func() bool { return vaultAttempts.Load() > 0 })
	if progress, commits := store.snapshot(); progress.Exists || commits != 0 {
		t.Fatal("progress advanced before business acknowledgement")
	}
	fail.Store(false)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("scan did not resume after business recovery")
	}
	if progress, commits := store.snapshot(); !progress.Exists || progress.Block != 10 || commits != 1 {
		t.Fatalf("progress=%+v commits=%d", progress, commits)
	}
	if mainCalls.Load() != 1 || service.count() != 1 {
		t.Fatalf("main calls=%d getLogs=%d", mainCalls.Load(), service.count())
	}
}

func TestChainBackfillCommitFailureAndRestart(t *testing.T) {
	address := common.HexToAddress("0x1")
	target, err := newWatchTarget(WatcherData{ContractAddress: address.Hex(), ContractABI: `[{"anonymous":false,"inputs":[],"name":"Ping","type":"event"}]`})
	if err != nil {
		t.Fatal(err)
	}
	var topic common.Hash
	for id := range target.events {
		topic = id
	}
	event10 := types.Log{Address: address, Topics: []common.Hash{topic}, BlockNumber: 10, BlockHash: common.HexToHash("0x10"), TxHash: common.HexToHash("0x10")}
	event11 := types.Log{Address: address, Topics: []common.Hash{topic}, BlockNumber: 11, BlockHash: common.HexToHash("0x11"), TxHash: common.HexToHash("0x11")}
	service := &chainTestRPC{headRPC: &headRPC{head: 10}, events: []types.Log{event10, event11}}
	server := rpc.NewServer()
	if err := server.RegisterName("eth", service); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	store := &chainTestStore{failCommit: true}
	var delivered atomic.Int64
	create := func() (*ChainBackfiller, *EVMWatcher) {
		w := newChainTestWatcher(t, server, "main", address.Hex(), eventHandlerFunc(func(string, *Event) error { delivered.Add(1); return nil }))
		scanner := newChainTestBackfiller(t, server, store, 10)
		if err := scanner.RegisterWatcher(w); err != nil {
			t.Fatal(err)
		}
		if err := w.Start(); err != nil {
			t.Fatal(err)
		}
		return scanner, w
	}
	scanner, w := create()
	if err := scanner.ScanOnce(context.Background()); err == nil {
		t.Fatal("storage failure accepted")
	}
	if progress, _ := store.snapshot(); progress.Exists {
		t.Fatal("failed commit advanced watermark")
	}
	store.mu.Lock()
	store.failCommit = false
	store.mu.Unlock()
	if err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if delivered.Load() != 1 {
		t.Fatalf("same event delivered %d times", delivered.Load())
	}
	w.Stop()
	service.headRPC.mu.Lock()
	service.head = 11
	service.headRPC.mu.Unlock()
	scanner2, w2 := create()
	defer w2.Stop()
	if err := scanner2.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if progress, commits := store.snapshot(); progress.Block != 11 || commits != 2 || delivered.Load() != 2 {
		t.Fatalf("progress=%+v commits=%d delivered=%d", progress, commits, delivered.Load())
	}
}

func TestChainBackfillSubbatchFailureKeepsProgress(t *testing.T) {
	service := &chainTestRPC{headRPC: &headRPC{head: 10}, failAt: 2}
	server := rpc.NewServer()
	if err := server.RegisterName("eth", service); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	store := &chainTestStore{}
	var deliveries atomic.Int64
	handler := eventHandlerFunc(func(string, *Event) error { deliveries.Add(1); return nil })
	a := newChainTestWatcher(t, server, "a", common.HexToAddress("0x1").Hex(), handler)
	b := newChainTestWatcher(t, server, "b", common.HexToAddress("0x2").Hex(), handler)
	scanner := newChainTestBackfiller(t, server, store, 10)
	scanner.cfg.MaxAddresses = 1
	for _, w := range []*EVMWatcher{a, b} {
		if err := scanner.RegisterWatcher(w); err != nil {
			t.Fatal(err)
		}
		if err := w.Start(); err != nil {
			t.Fatal(err)
		}
		defer w.Stop()
	}
	if err := scanner.ScanOnce(context.Background()); err == nil {
		t.Fatal("partial query failure accepted")
	}
	if progress, commits := store.snapshot(); progress.Exists || commits != 0 || deliveries.Load() != 0 {
		t.Fatal("partial query advanced or delivered")
	}
	if err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if progress, _ := store.snapshot(); !progress.Exists || progress.Block != 10 {
		t.Fatalf("retry progress=%+v", progress)
	}
}

func TestChainBackfillMembershipMismatchFailsClosed(t *testing.T) {
	service := &chainTestRPC{headRPC: &headRPC{head: 10}}
	server := rpc.NewServer()
	if err := server.RegisterName("eth", service); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	store := &chainTestStore{progress: ChainBackfillProgress{Exists: true, Block: 9, Membership: [32]byte{1}}}
	scanner := newChainTestBackfiller(t, server, store, 10)
	w := newChainTestWatcher(t, server, "changed", common.HexToAddress("0x1").Hex(), eventHandlerFunc(func(string, *Event) error { return nil }))
	if err := scanner.RegisterWatcher(w); err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	if err := scanner.ScanOnce(context.Background()); err == nil {
		t.Fatal("changed membership accepted")
	}
	if service.count() != 0 {
		t.Fatal("queried logs with incompatible membership")
	}
	if progress, commits := store.snapshot(); progress.Block != 9 || commits != 0 {
		t.Fatal("incompatible membership advanced")
	}
	other := newChainTestWatcher(t, server, "other", common.HexToAddress("0x2").Hex(), eventHandlerFunc(func(string, *Event) error { return nil }))
	if err := scanner.RegisterWatcher(other); err == nil {
		t.Fatal("allowed adding watcher after membership sealed")
	}
}

func TestChainBackfillMalformedDataFailsWithoutWatermark(t *testing.T) {
	address := common.HexToAddress("0x1")
	abi := `[{"anonymous":false,"inputs":[{"indexed":false,"name":"amount","type":"uint256"}],"name":"Paid","type":"event"}]`
	target, err := newWatchTarget(WatcherData{ContractAddress: address.Hex(), ContractABI: abi})
	if err != nil {
		t.Fatal(err)
	}
	var topic common.Hash
	for id := range target.events {
		topic = id
	}
	service := &chainTestRPC{headRPC: &headRPC{head: 10}, events: []types.Log{{Address: address, Topics: []common.Hash{topic}, BlockNumber: 10, BlockHash: common.HexToHash("0x10"), TxHash: common.HexToHash("0x1")}}}
	server := rpc.NewServer()
	if err := server.RegisterName("eth", service); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	store := &chainTestStore{}
	scanner := newChainTestBackfiller(t, server, store, 10)
	var deliveries atomic.Int64
	w := NewEVMWatcher("w", "inproc", []WatcherData{{ContractAddress: address.Hex(), ContractABI: abi}}, nil, eventHandlerFunc(func(string, *Event) error { deliveries.Add(1); return nil }), WithLogger(silentLogger{}))
	w.requestInterval = 0
	w.dialClient = func(string) (*ethclient.Client, error) { return ethclient.NewClient(rpc.DialInProc(server)), nil }
	if err := scanner.RegisterWatcher(w); err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	if err := scanner.ScanOnce(context.Background()); err == nil {
		t.Fatal("malformed event accepted")
	}
	if progress, commits := store.snapshot(); progress.Exists || commits != 0 || deliveries.Load() != 0 {
		t.Fatal("malformed event advanced or delivered")
	}
}

func TestChainBackfillCanceledBeforeEnqueueCanRetry(t *testing.T) {
	address := common.HexToAddress("0x1")
	target, err := newWatchTarget(WatcherData{ContractAddress: address.Hex(), ContractABI: `[{"anonymous":false,"inputs":[],"name":"Ping","type":"event"}]`})
	if err != nil {
		t.Fatal(err)
	}
	var topic common.Hash
	for id := range target.events {
		topic = id
	}
	log := types.Log{Address: address, Topics: []common.Hash{topic}, BlockNumber: 10, BlockHash: common.HexToHash("0x10"), TxHash: common.HexToHash("0x1")}
	var delivered atomic.Int64
	w, cancelWatcher := newUnitWatcher(&recordingStorage{}, eventHandlerFunc(func(string, *Event) error { delivered.Add(1); return nil }))
	defer func() { cancelWatcher(); w.wg.Wait() }()
	w.mu.Lock()
	w.started = true
	w.targets = map[common.Address]*watchTarget{address: target}
	w.eventCh = make(chan *Event)
	w.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := w.deliverChainBackfill(ctx, log); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancel error=%v", err)
	}
	w.progressMu.Lock()
	seen := len(w.seenEvents)
	pending := len(w.pendingByBlock)
	w.progressMu.Unlock()
	if seen != 0 || pending != 0 || w.pendingEvents.Load() != 0 {
		t.Fatal("canceled enqueue left a permanently pending event")
	}
	w.wg.Add(1)
	go w.notifyLoop()
	if err := w.deliverChainBackfill(context.Background(), log); err != nil {
		t.Fatal(err)
	}
	if delivered.Load() != 1 {
		t.Fatalf("deliveries=%d", delivered.Load())
	}
}

func TestChainBackfillPeriodicScansAndWatcherHeartbeatsRemainIndependent(t *testing.T) {
	address := common.HexToAddress("0x1")
	target, err := newWatchTarget(WatcherData{ContractAddress: address.Hex(), ContractABI: `[{"anonymous":false,"inputs":[],"name":"Ping","type":"event"}]`})
	if err != nil {
		t.Fatal(err)
	}
	var topic common.Hash
	for id := range target.events {
		topic = id
	}
	event := types.Log{Address: address, Topics: []common.Hash{topic}, BlockNumber: 11, BlockHash: common.HexToHash("0x11"), TxHash: common.HexToHash("0x11")}
	service := &chainTestRPC{headRPC: &headRPC{head: 10}, events: []types.Log{event}}
	server := rpc.NewServer()
	if err := server.RegisterName("eth", service); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	store := &chainTestStore{}
	scanner := newChainTestBackfiller(t, server, store, 10)
	scanner.cfg.Interval = 30 * time.Millisecond
	var delivered atomic.Int64
	w := newChainTestWatcher(t, server, "w", address.Hex(), eventHandlerFunc(func(string, *Event) error { delivered.Add(1); return nil }))
	w.gapHealInterval = 30 * time.Millisecond
	if err := scanner.RegisterWatcher(w); err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	if err := scanner.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer scanner.Stop()
	waitFor(t, func() bool { p, _ := store.snapshot(); return p.Exists && p.Block == 10 })
	service.headRPC.mu.Lock()
	service.head = 11
	service.headRPC.mu.Unlock()
	waitFor(t, func() bool { p, _ := store.snapshot(); return p.Block == 11 && delivered.Load() == 1 })
	scanner.Stop()
	if service.count() != 2 {
		t.Fatalf("expected exactly two public getLogs calls, got %d", service.count())
	}
	headCalls, _ := service.headRPC.counts()
	if headCalls < 3 {
		t.Fatalf("expected independent watcher heartbeat, head calls=%d", headCalls)
	}
}

func TestChainBackfillCanceledWhileBusinessPendingRetriesSafely(t *testing.T) {
	address := common.HexToAddress("0x1")
	target, err := newWatchTarget(WatcherData{ContractAddress: address.Hex(), ContractABI: `[{"anonymous":false,"inputs":[],"name":"Ping","type":"event"}]`})
	if err != nil {
		t.Fatal(err)
	}
	var topic common.Hash
	for id := range target.events {
		topic = id
	}
	log := types.Log{Address: address, Topics: []common.Hash{topic}, BlockNumber: 10, BlockHash: common.HexToHash("0x10"), TxHash: common.HexToHash("0x10")}
	service := &chainTestRPC{headRPC: &headRPC{head: 10}, events: []types.Log{log}}
	server := rpc.NewServer()
	if err := server.RegisterName("eth", service); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	store := &chainTestStore{}
	scanner := newChainTestBackfiller(t, server, store, 10)
	var attempts, success atomic.Int64
	var fail atomic.Bool
	fail.Store(true)
	w := newChainTestWatcher(t, server, "w", address.Hex(), eventHandlerFunc(func(string, *Event) error {
		attempts.Add(1)
		if fail.Load() {
			return errors.New("retry")
		}
		success.Add(1)
		return nil
	}))
	if err := scanner.RegisterWatcher(w); err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- scanner.ScanOnce(ctx) }()
	waitFor(t, func() bool { return attempts.Load() > 0 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error=%v", err)
	}
	if progress, _ := store.snapshot(); progress.Exists {
		t.Fatal("canceled scan advanced progress")
	}
	fail.Store(false)
	if err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if progress, _ := store.snapshot(); !progress.Exists || progress.Block != 10 || success.Load() != 1 {
		t.Fatalf("progress=%+v successes=%d", progress, success.Load())
	}
}

func TestChainBackfillRejectsWatcherOnDifferentChain(t *testing.T) {
	service := &chainTestRPC{headRPC: &headRPC{head: 10}}
	server := rpc.NewServer()
	if err := server.RegisterName("eth", service); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	start := uint64(10)
	scanner, err := NewChainBackfiller(ChainBackfillConfig{ChainID: "1", RPCURL: "inproc", ProgressKey: "wrong-chain", StartBlock: &start, Interval: time.Second, MaxBlockRange: 10, MaxAddresses: 1, Storage: &chainTestStore{}, Logger: silentLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	w := newChainTestWatcher(t, server, "w", common.HexToAddress("0x1").Hex(), eventHandlerFunc(func(string, *Event) error { return nil }))
	if err := scanner.RegisterWatcher(w); err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err == nil {
		w.Stop()
		t.Fatal("watcher from another chain started")
	}
	if service.count() != 0 {
		t.Fatal("queried logs for mismatched chain")
	}
}
func TestChainBackfillCanStartAtBlockZero(t *testing.T) {
	service := &chainTestRPC{headRPC: &headRPC{head: 0}}
	server := rpc.NewServer()
	if err := server.RegisterName("eth", service); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	store := &chainTestStore{}
	scanner := newChainTestBackfiller(t, server, store, 0)
	w := newChainTestWatcher(t, server, "w", common.HexToAddress("0x1").Hex(), eventHandlerFunc(func(string, *Event) error { return nil }))
	if err := scanner.RegisterWatcher(w); err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	if err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if progress, commits := store.snapshot(); !progress.Exists || progress.Block != 0 || commits != 1 {
		t.Fatalf("progress=%+v commits=%d", progress, commits)
	}
}

func TestChainBackfillNullResultDoesNotAdvance(t *testing.T) {
	service := &chainTestRPC{headRPC: &headRPC{head: 10}, returnNull: true}
	server := rpc.NewServer()
	if err := server.RegisterName("eth", service); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	store := &chainTestStore{}
	scanner := newChainTestBackfiller(t, server, store, 10)
	w := newChainTestWatcher(t, server, "w", common.HexToAddress("0x1").Hex(), eventHandlerFunc(func(string, *Event) error { return nil }))
	if err := scanner.RegisterWatcher(w); err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	if err := scanner.ScanOnce(context.Background()); err == nil {
		t.Fatal("null getLogs result accepted as empty array")
	}
	if progress, _ := store.snapshot(); progress.Exists {
		t.Fatal("null result advanced watermark")
	}
}

func TestChainBackfillDuringWatcherDisconnectAndReconnect(t *testing.T) {
	address := common.HexToAddress("0x1")
	target, err := newWatchTarget(WatcherData{ContractAddress: address.Hex(), ContractABI: `[{"anonymous":false,"inputs":[],"name":"Ping","type":"event"}]`})
	if err != nil {
		t.Fatal(err)
	}
	var topic common.Hash
	for id := range target.events {
		topic = id
	}
	service := &chainTestRPC{headRPC: &headRPC{head: 10}}
	server := rpc.NewServer()
	if err := server.RegisterName("eth", service); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	store := &chainTestStore{}
	scanner := newChainTestBackfiller(t, server, store, 10)
	var calls atomic.Int64
	w := newChainTestWatcher(t, server, "w", address.Hex(), eventHandlerFunc(func(string, *Event) error { calls.Add(1); return nil }))
	if err := scanner.RegisterWatcher(w); err != nil {
		t.Fatal(err)
	}
	if err := w.Start(); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()
	if err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	w.subFailures <- subscriptionFailure{Generation: w.currentSubscriptionGeneration(), Err: errors.New("disconnect")}
	waitFor(t, func() bool { return !w.isStreaming() })
	log := types.Log{Address: address, Topics: []common.Hash{topic}, BlockNumber: 11, BlockHash: common.HexToHash("0x11"), TxHash: common.HexToHash("0x11")}
	service.mu2.Lock()
	service.events = []types.Log{log}
	service.mu2.Unlock()
	service.headRPC.mu.Lock()
	service.head = 11
	service.headRPC.mu.Unlock()
	if err := scanner.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if progress, _ := store.snapshot(); progress.Block != 11 || calls.Load() != 1 {
		t.Fatalf("progress=%+v calls=%d", progress, calls.Load())
	}
	deadline := time.Now().Add(7 * time.Second)
	for !w.isStreaming() {
		if time.Now().After(deadline) {
			t.Fatal("watcher did not resubscribe")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if service.count() != 2 {
		t.Fatalf("watcher performed its own getLogs: calls=%d", service.count())
	}
}
