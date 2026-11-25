package evmwatcher

import (
	"context"
	"errors"
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

type logRPC struct {
	mu     sync.Mutex
	calls  int
	events []types.Log
	fail   error
}

func (s *logRPC) GetLogs(_ context.Context, q map[string]interface{}) ([]types.Log, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.fail != nil {
		return nil, s.fail
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
		for _, item := range v {
			addresses[common.HexToAddress(item.(string))] = true
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
func (s *logRPC) count() int { s.mu.Lock(); defer s.mu.Unlock(); return s.calls }
func TestSharedBackfillPreservesWatcherDeliveryAndProgress(t *testing.T) {
	abi := `[{"anonymous":false,"inputs":[],"name":"Ping","type":"event"}]`
	a, b := common.HexToAddress("0x1"), common.HexToAddress("0x2")
	ta, err := newWatchTarget(WatcherData{ContractAddress: a.Hex(), ContractABI: abi})
	if err != nil {
		t.Fatal(err)
	}
	tb, err := newWatchTarget(WatcherData{ContractAddress: b.Hex(), ContractABI: abi})
	if err != nil {
		t.Fatal(err)
	}
	var topic common.Hash
	for id := range ta.events {
		topic = id
	}
	first := types.Log{Address: a, Topics: []common.Hash{topic}, BlockNumber: 10, BlockHash: common.HexToHash("0x10"), TxHash: common.HexToHash("0xa")}
	second := types.Log{Address: b, Topics: []common.Hash{topic}, BlockNumber: 10, BlockHash: common.HexToHash("0x10"), TxHash: common.HexToHash("0xb")}
	service := &logRPC{events: []types.Log{first, second}}
	server := rpc.NewServer()
	if err := server.RegisterName("eth", service); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	sharedClient := ethclient.NewClient(rpc.DialInProc(server))
	defer sharedClient.Close()
	c1 := ethclient.NewClient(rpc.DialInProc(server))
	defer c1.Close()
	c2 := ethclient.NewClient(rpc.DialInProc(server))
	defer c2.Close()
	p, err := NewSharedLogProvider(sharedClient, SharedLogConfig{ChainID: "1672", RPCIdentity: "rpc", Window: 50 * time.Millisecond, MaxBlockRange: 10, MaxAddresses: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var fail atomic.Bool
	fail.Store(true)
	var firstAttempts, firstSuccess, secondSuccess atomic.Int64
	s1, s2 := &recordingStorage{}, &recordingStorage{}
	w1, cancel1 := newUnitWatcher(s1, eventHandlerFunc(func(string, *Event) error {
		firstAttempts.Add(1)
		if fail.Load() {
			return errors.New("injected handler failure")
		}
		firstSuccess.Add(1)
		return nil
	}))
	w2, cancel2 := newUnitWatcher(s2, eventHandlerFunc(func(string, *Event) error { secondSuccess.Add(1); return nil }))
	defer func() { cancel1(); cancel2(); w1.wg.Wait(); w2.wg.Wait() }()
	for _, w := range []*EVMWatcher{w1, w2} {
		w.requestInterval = 0
		WithSharedLogProvider(p, "1672", "rpc")(w)
		w.setLastBlock(9)
		w.wg.Add(1)
		go w.notifyLoop()
	}
	w1.ethClient = c1
	w2.ethClient = c2
	w1.targets = map[common.Address]*watchTarget{a: ta}
	w2.targets = map[common.Address]*watchTarget{b: tb}
	// A WS event is delivered immediately; the overlapping scan must deduplicate it.
	w1.dispatchLog(first, "subscription")
	waitFor(t, func() bool { return firstAttempts.Load() > 0 })
	done := make(chan error, 2)
	go func() { done <- w1.backfill(context.Background(), 10) }()
	go func() { done <- w2.backfill(context.Background(), 10) }()
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, func() bool { block, _ := s2.snapshot(); return block == 10 })
	if block, _ := s1.snapshot(); block >= 10 {
		t.Fatalf("failed watcher advanced to %d", block)
	}
	if service.count() != 1 {
		t.Fatalf("getLogs calls=%d", service.count())
	}
	if secondSuccess.Load() != 1 {
		t.Fatalf("second delivery=%d", secondSuccess.Load())
	}
	fail.Store(false)
	waitFor(t, func() bool { block, _ := s1.snapshot(); return block == 10 })
	if firstSuccess.Load() != 1 {
		t.Fatalf("first successful delivery=%d", firstSuccess.Load())
	}
}

type resumableStorage struct {
	recordingStorage
	initial int64
}

func (s *resumableStorage) GetWatchedBlockNumber(string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.setCalls > 0 {
		return int64(s.setBlock), nil
	}
	return s.initial, nil
}
func TestSharedProviderWatcherStartReconnectAndRestart(t *testing.T) {
	service := &headRPC{head: 100}
	server := rpc.NewServer()
	if err := server.RegisterName("eth", service); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	client := ethclient.NewClient(rpc.DialInProc(server))
	defer client.Close()
	p, err := NewSharedLogProvider(client, SharedLogConfig{ChainID: "1672", RPCIdentity: "rpc", Window: 100 * time.Millisecond, MaxBlockRange: 20, MaxAddresses: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	makeWatcher := func(name, address string, storage *resumableStorage) *EVMWatcher {
		data := []WatcherData{{ContractAddress: address, ContractABI: `[{"anonymous":false,"inputs":[],"name":"Ping","type":"event"}]`}}
		w := NewEVMWatcher(name, "rpc", data, storage, eventHandlerFunc(func(string, *Event) error { return nil }), WithLogger(silentLogger{}), WithSharedLogProvider(p, "1672", "rpc"))
		w.requestInterval = 0
		w.dialClient = func(string) (*ethclient.Client, error) { return ethclient.NewClient(rpc.DialInProc(server)), nil }
		return w
	}
	s1, s2 := &resumableStorage{initial: 90}, &resumableStorage{initial: 90}
	w1 := makeWatcher("main", "0x0000000000000000000000000000000000000001", s1)
	w2 := makeWatcher("vault", "0x0000000000000000000000000000000000000002", s2)
	if err := w1.Start(); err != nil {
		t.Fatal(err)
	}
	defer w1.Stop()
	if err := w2.Start(); err != nil {
		t.Fatal(err)
	}
	defer w2.Stop()
	waitFor(t, func() bool { a, _ := s1.snapshot(); b, _ := s2.snapshot(); return a == 100 && b == 100 })
	_, calls := service.counts()
	if calls != 1 {
		t.Fatalf("startup scans %d", calls)
	}
	service.mu.Lock()
	service.advanceOnSubscribe = true
	service.mu.Unlock()
	w1.subFailures <- subscriptionFailure{Generation: w1.currentSubscriptionGeneration(), Err: errors.New("injected disconnect")}
	deadline := time.Now().Add(8 * time.Second)
	for {
		a, _ := s1.snapshot()
		if a == 101 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("reconnect did not catch up")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if b, _ := s2.snapshot(); b != 100 {
		t.Fatalf("other watcher watermark changed to %d", b)
	}
	service.mu.Lock()
	service.advanceOnSubscribe = false
	service.mu.Unlock()
	w1.Stop()
	service.mu.Lock()
	before := service.logs
	service.mu.Unlock()
	if err := w1.Start(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return w1.isStreaming() })
	service.mu.Lock()
	after := service.logs
	subs := service.subscriptions
	service.mu.Unlock()
	if after != before || subs < 4 {
		t.Fatalf("restart scans %d -> %d, subscriptions %d", before, after, subs)
	}
}

func TestSharedBackfillRPCFailureDoesNotAdvanceEitherWatcher(t *testing.T) {
	abi := `[{"anonymous":false,"inputs":[],"name":"Ping","type":"event"}]`
	a, b := common.HexToAddress("0x1"), common.HexToAddress("0x2")
	ta, err := newWatchTarget(WatcherData{ContractAddress: a.Hex(), ContractABI: abi})
	if err != nil {
		t.Fatal(err)
	}
	tb, err := newWatchTarget(WatcherData{ContractAddress: b.Hex(), ContractABI: abi})
	if err != nil {
		t.Fatal(err)
	}
	service := &logRPC{fail: errors.New("upstream unavailable")}
	server := rpc.NewServer()
	if err := server.RegisterName("eth", service); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	shared := ethclient.NewClient(rpc.DialInProc(server))
	defer shared.Close()
	c1 := ethclient.NewClient(rpc.DialInProc(server))
	defer c1.Close()
	c2 := ethclient.NewClient(rpc.DialInProc(server))
	defer c2.Close()
	p, err := NewSharedLogProvider(shared, SharedLogConfig{ChainID: "1672", RPCIdentity: "rpc", Window: 50 * time.Millisecond, MaxBlockRange: 10, MaxAddresses: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	s1, s2 := &recordingStorage{}, &recordingStorage{}
	w1, cancel1 := newUnitWatcher(s1, nil)
	defer cancel1()
	w2, cancel2 := newUnitWatcher(s2, nil)
	defer cancel2()
	for _, w := range []*EVMWatcher{w1, w2} {
		w.requestInterval = 0
		WithSharedLogProvider(p, "1672", "rpc")(w)
		w.setLastBlock(9)
	}
	w1.ethClient = c1
	w2.ethClient = c2
	w1.targets = map[common.Address]*watchTarget{a: ta}
	w2.targets = map[common.Address]*watchTarget{b: tb}
	done := make(chan error, 2)
	go func() { done <- w1.backfill(context.Background(), 10) }()
	go func() { done <- w2.backfill(context.Background(), 10) }()
	for i := 0; i < 2; i++ {
		if err := <-done; err == nil {
			t.Fatal("failed shared RPC returned success")
		}
	}
	if w1.getLastBlock() != 9 || w2.getLastBlock() != 9 || service.count() != 1 {
		t.Fatalf("progress=%d/%d calls=%d", w1.getLastBlock(), w2.getLastBlock(), service.count())
	}
	if _, calls := s1.snapshot(); calls != 0 {
		t.Fatal("first watermark was written")
	}
	if _, calls := s2.snapshot(); calls != 0 {
		t.Fatal("second watermark was written")
	}
}
