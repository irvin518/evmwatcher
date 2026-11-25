package evmwatcher

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
)

type headRPC struct {
	subscriptions      int
	advanceOnSubscribe bool
	mu                 sync.Mutex
	calls              int
	head               uint64
	err                error
	gate               chan struct{}
	logs               int
	logErr             error
}

func (s *headRPC) BlockNumber(ctx context.Context) (hexutil.Uint64, error) {
	s.mu.Lock()
	s.calls++
	gate, head, err := s.gate, s.head, s.err
	s.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
	return hexutil.Uint64(head), err
}
func (s *headRPC) GetLogs(ctx context.Context, query map[string]interface{}) ([]types.Log, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logs++
	return []types.Log{}, s.logErr
}
func (s *headRPC) Logs(ctx context.Context, query map[string]interface{}) (*rpc.Subscription, error) {
	notifier, ok := rpc.NotifierFromContext(ctx)
	if !ok {
		return nil, errors.New("subscriptions unsupported")
	}
	s.mu.Lock()
	s.subscriptions++
	if s.advanceOnSubscribe {
		s.head++
	}
	s.mu.Unlock()
	return notifier.CreateSubscription(), nil
}

func (s *headRPC) counts() (int, int) { s.mu.Lock(); defer s.mu.Unlock(); return s.calls, s.logs }
func testHeadClient(t *testing.T, s *headRPC) *ethclient.Client {
	t.Helper()
	server := rpc.NewServer()
	if err := server.RegisterName("eth", s); err != nil {
		t.Fatal(err)
	}
	c := ethclient.NewClient(rpc.DialInProc(server))
	t.Cleanup(func() { c.Close(); server.Stop() })
	return c
}
func waitFor(t *testing.T, f func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !f() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestCachedBlockNumberCoalescesAndIsolatesCancellation(t *testing.T) {
	gate := make(chan struct{})
	service := &headRPC{head: 42, gate: gate}
	client := testHeadClient(t, service)
	p := NewCachedBlockNumberProvider(time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	canceled := make(chan error, 1)
	go func() { _, err := p.BlockNumber(ctx, client, "chain", "rpc", false); canceled <- err }()
	waitFor(t, func() bool { n, _ := service.counts(); return n == 1 })
	const count = 12
	results := make(chan error, count)
	for i := 0; i < count; i++ {
		go func() {
			h, err := p.BlockNumber(context.Background(), client, "chain", "rpc", false)
			if err == nil && h != 42 {
				err = errors.New("wrong head")
			}
			results <- err
		}()
	}
	waitFor(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.calls[headKey{"chain", "rpc"}].waiters == count+1
	})
	cancel()
	if err := <-canceled; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled call: %v", err)
	}
	close(gate)
	for i := 0; i < count; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if n, _ := service.counts(); n != 1 {
		t.Fatalf("RPC calls %d", n)
	}
	if _, err := p.BlockNumber(ctx, client, "chain", "rpc", false); !errors.Is(err, context.Canceled) {
		t.Fatalf("cached canceled call: %v", err)
	}
}

func TestCachedBlockNumberExpiryFailureAndKeys(t *testing.T) {
	service := &headRPC{head: 10}
	client := testHeadClient(t, service)
	p := NewCachedBlockNumberProvider(30 * time.Millisecond)
	ctx := context.Background()
	read := func(chain, url string) (uint64, error) { return p.BlockNumber(ctx, client, chain, url, false) }
	if h, e := read("a", "rpc"); e != nil || h != 10 {
		t.Fatalf("initial %d %v", h, e)
	}
	service.mu.Lock()
	service.head = 11
	service.mu.Unlock()
	if h, e := read("a", "rpc"); e != nil || h != 10 {
		t.Fatalf("cached %d %v", h, e)
	}
	if h, e := read("b", "rpc"); e != nil || h != 11 {
		t.Fatalf("other chain %d %v", h, e)
	}
	if h, e := read("a", "other"); e != nil || h != 11 {
		t.Fatalf("other endpoint %d %v", h, e)
	}
	time.Sleep(40 * time.Millisecond)
	service.mu.Lock()
	service.err = errors.New("rpc failed")
	service.mu.Unlock()
	if _, e := read("a", "rpc"); e == nil {
		t.Fatal("expired value returned on RPC failure")
	}
	service.mu.Lock()
	service.err = nil
	service.head = 12
	service.mu.Unlock()
	if h, e := read("a", "rpc"); e != nil || h != 12 {
		t.Fatalf("retry %d %v", h, e)
	}
	if n, _ := service.counts(); n != 5 {
		t.Fatalf("RPC calls %d, want 5", n)
	}
}

func TestSharedHeadKeepsWatcherBackfillAndWatermarksIndependent(t *testing.T) {
	service := &headRPC{head: 105}
	client := testHeadClient(t, service)
	p := NewCachedBlockNumberProvider(time.Minute)
	s1, s2 := &recordingStorage{}, &recordingStorage{}
	w1, cancel1 := newUnitWatcher(s1, nil)
	defer cancel1()
	w2, cancel2 := newUnitWatcher(s2, nil)
	w1.chainName = "pharos"
	w2.chainName = "chainwatcher:interest-vault:1672:address:"
	defer cancel2()
	for _, w := range []*EVMWatcher{w1, w2} {
		w.ethClient = client
		w.wssURL = "rpc"
		w.blockNumberProvider = p
		w.blockNumberChainID = "1672"
		w.blockNumberRPCIdentity = "rpc"
		w.requestInterval = 0
	}
	w1.setLastBlock(100)
	w2.setLastBlock(90)
	h1, e := w1.blockNumber(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	h2, e := w2.blockNumber(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if h1 != 105 || h2 != 105 {
		t.Fatalf("heads %d %d", h1, h2)
	}
	if b, n := s1.snapshot(); b != 0 || n != 0 {
		t.Fatal("head read advanced first watermark")
	}
	if b, n := s2.snapshot(); b != 0 || n != 0 {
		t.Fatal("head read advanced second watermark")
	}
	if err := w1.backfill(context.Background(), h1); err != nil {
		t.Fatal(err)
	}
	if b, _ := s1.snapshot(); b != 105 {
		t.Fatalf("first watermark %d", b)
	}
	if b, n := s2.snapshot(); b != 0 || n != 0 || w2.getLastBlock() != 90 {
		t.Fatal("first scan changed second watcher")
	}
	service.mu.Lock()
	service.logErr = errors.New("getLogs failed")
	service.mu.Unlock()
	if err := w2.backfill(context.Background(), h2); err == nil {
		t.Fatal("expected getLogs failure")
	}
	if b, n := s2.snapshot(); b != 0 || n != 0 || w2.getLastBlock() != 90 {
		t.Fatal("failed scan advanced second watcher")
	}
	service.mu.Lock()
	service.logErr = nil
	service.mu.Unlock()
	if err := w2.backfill(context.Background(), h2); err != nil {
		t.Fatal(err)
	}
	if b, _ := s2.snapshot(); b != 105 {
		t.Fatalf("second watermark %d", b)
	}
	if heads, logs := service.counts(); heads != 1 || logs != 3 {
		t.Fatalf("head calls %d, getLogs calls %d", heads, logs)
	}
}

func TestCanceledLastWaiterDoesNotCacheOrBlockRetry(t *testing.T) {
	gate := make(chan struct{})
	service := &headRPC{head: 9, gate: gate}
	client := testHeadClient(t, service)
	p := NewCachedBlockNumberProvider(time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := p.BlockNumber(ctx, client, "chain", "rpc", false); result <- err }()
	waitFor(t, func() bool { n, _ := service.counts(); return n == 1 })
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("error %v", err)
	}
	service.mu.Lock()
	service.gate = nil
	service.head = 10
	service.mu.Unlock()
	if head, err := p.BlockNumber(context.Background(), client, "chain", "rpc", false); err != nil || head != 10 {
		t.Fatalf("retry %d %v", head, err)
	}
	close(gate)
	if n, _ := service.counts(); n != 2 {
		t.Fatalf("calls %d", n)
	}
}

func TestWatcherProviderAndDefault(t *testing.T) {
	service := &headRPC{head: 25}
	client := testHeadClient(t, service)
	w, cancel := newUnitWatcher(&recordingStorage{}, nil)
	defer cancel()
	w.ethClient = client
	w.requestInterval = 0
	if h, err := w.blockNumber(context.Background()); err != nil || h != 25 {
		t.Fatalf("default %d %v", h, err)
	}
	type contextKey struct{}
	ctx := context.WithValue(context.Background(), contextKey{}, "value")
	expectedErr := errors.New("provider failure")
	WithBlockNumberIdentity("1672", "rpc")(w)
	WithBlockNumberProvider(BlockNumberProviderFunc(func(got context.Context, gotClient *ethclient.Client, chain, endpoint string, fresh bool) (uint64, error) {
		if got.Value(contextKey{}) != "value" || gotClient != client || chain != "1672" || endpoint != "rpc" || fresh {
			t.Error("provider received incorrect watcher context or client")
		}
		return 0, expectedErr
	}))(w)
	if _, err := w.blockNumber(ctx); !errors.Is(err, expectedErr) {
		t.Fatalf("provider error %v", err)
	}
	if got := w.latestHead.Load(); got != 25 {
		t.Fatalf("failed provider changed latest head to %d", got)
	}
}

type storedProgress struct {
	recordingStorage
	stored int64
}

func (s *storedProgress) GetWatchedBlockNumber(string) (int64, error) { return s.stored, nil }

func TestFreshHeadsAtStartupAndResubscribe(t *testing.T) {
	service := &headRPC{head: 100, advanceOnSubscribe: true}
	server := rpc.NewServer()
	if err := server.RegisterName("eth", service); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	endpoint := "in-process-rpc"
	client := ethclient.NewClient(rpc.DialInProc(server))
	defer client.Close()
	p := NewCachedBlockNumberProvider(time.Minute)
	if h, err := p.BlockNumber(context.Background(), client, "1672", "rpc", false); err != nil || h != 100 {
		t.Fatalf("prime cache %d %v", h, err)
	}
	service.mu.Lock()
	service.head = 101
	service.mu.Unlock()
	storage := &storedProgress{stored: 101}
	data := []WatcherData{{ContractAddress: "0x0000000000000000000000000000000000000001", ContractABI: `[{"anonymous":false,"inputs":[],"name":"Ping","type":"event"}]`}}
	w := NewEVMWatcher("interest-vault", endpoint, data, storage, eventHandlerFunc(func(string, *Event) error { return nil }), WithLogger(silentLogger{}), WithBlockNumberProvider(p), WithBlockNumberIdentity("1672", "rpc"))
	w.requestInterval = 0
	w.dialClient = func(string) (*ethclient.Client, error) { return ethclient.NewClient(rpc.DialInProc(server)), nil }
	if err := w.Start(); err != nil {
		t.Fatalf("start with stored 101 and cached 100: %v", err)
	}
	defer w.Stop()
	waitFor(t, func() bool { return w.getLastBlock() == 102 })
	waitFor(t, func() bool { b, _ := storage.snapshot(); return b == 102 })
	if b, _ := storage.snapshot(); b != 102 {
		t.Fatalf("post-subscription scan watermark %d", b)
	}
	// Populate the periodic cache with 102. Resubscription increments the head
	// to 103 and its post-subscription read must bypass this still-valid entry.
	if h, err := w.blockNumber(context.Background()); err != nil || h != 102 {
		t.Fatalf("periodic read %d %v", h, err)
	}
	if h, err := w.blockNumber(context.Background()); err != nil || h != 102 {
		t.Fatalf("cached periodic read %d %v", h, err)
	}
	before, _ := service.counts()
	done := make(chan struct{})
	go func() { w.resubscribe(); close(done) }()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("resubscribe timed out")
	}
	if got := w.getLastBlock(); got != 103 {
		t.Fatalf("resubscribe scanned through %d, want 103", got)
	}
	waitFor(t, func() bool { b, _ := storage.snapshot(); return b == 103 })
	if b, _ := storage.snapshot(); b != 103 {
		t.Fatalf("resubscribe watermark %d", b)
	}
	after, logs := service.counts()
	if after <= before || logs < 2 {
		t.Fatalf("head calls %d -> %d, getLogs %d", before, after, logs)
	}
	service.mu.Lock()
	subscriptions := service.subscriptions
	service.mu.Unlock()
	if subscriptions != 2 {
		t.Fatalf("subscriptions %d", subscriptions)
	}
}

func TestFreshReadDoesNotJoinOrCacheEarlierRequest(t *testing.T) {
	gate := make(chan struct{})
	service := &headRPC{head: 100, gate: gate}
	client := testHeadClient(t, service)
	p := NewCachedBlockNumberProvider(time.Minute)
	old := make(chan uint64, 1)
	go func() { h, _ := p.BlockNumber(context.Background(), client, "1672", "rpc", false); old <- h }()
	waitFor(t, func() bool { n, _ := service.counts(); return n == 1 })
	service.mu.Lock()
	service.head = 101
	service.gate = nil
	service.mu.Unlock()
	if h, err := p.BlockNumber(context.Background(), client, "1672", "rpc", true); err != nil || h != 101 {
		t.Fatalf("fresh read %d %v", h, err)
	}
	close(gate)
	if h := <-old; h != 100 {
		t.Fatalf("old request %d", h)
	}
	if h, err := p.BlockNumber(context.Background(), client, "1672", "rpc", false); err != nil || h != 101 {
		t.Fatalf("periodic read %d %v", h, err)
	}
	if _, err := p.BlockNumber(context.Background(), client, "", "rpc", false); err == nil {
		t.Fatal("missing chain ID accepted")
	}
	if _, err := p.BlockNumber(context.Background(), client, "1672", "", false); err == nil {
		t.Fatal("missing RPC identity accepted")
	}
}

func TestDifferentWatcherNamesSharePeriodicHeadAndKeepSubscriptions(t *testing.T) {
	service := &headRPC{head: 100}
	server := rpc.NewServer()
	if err := server.RegisterName("eth", service); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	p := NewCachedBlockNumberProvider(time.Minute)
	data := []WatcherData{{ContractAddress: "0x0000000000000000000000000000000000000001", ContractABI: `[{"anonymous":false,"inputs":[],"name":"Ping","type":"event"}]`}}
	s1 := &storedProgress{stored: 95}
	s2 := &storedProgress{stored: 90}
	makeWatcher := func(name string, storage *storedProgress) *EVMWatcher {
		w := NewEVMWatcher(name, "rpc", data, storage, eventHandlerFunc(func(string, *Event) error { return nil }), WithLogger(silentLogger{}), WithBlockNumberProvider(p), WithBlockNumberIdentity("1672", "rpc"))
		w.requestInterval = 0
		w.dialClient = func(string) (*ethclient.Client, error) { return ethclient.NewClient(rpc.DialInProc(server)), nil }
		return w
	}
	w1 := makeWatcher("pharos", s1)
	w2 := makeWatcher("chainwatcher:interest-vault:1672:address:", s2)
	if err := w1.Start(); err != nil {
		t.Fatal(err)
	}
	defer w1.Stop()
	if err := w2.Start(); err != nil {
		t.Fatal(err)
	}
	defer w2.Stop()
	waitFor(t, func() bool { a, _ := s1.snapshot(); b, _ := s2.snapshot(); return a == 100 && b == 100 })
	service.mu.Lock()
	subscriptions := service.subscriptions
	service.mu.Unlock()
	_, logs := service.counts()
	if subscriptions != 2 || logs != 2 {
		t.Fatalf("subscriptions %d getLogs %d", subscriptions, logs)
	}
	waitFor(t, func() bool { return !w1.catchingUp.Load() && !w2.catchingUp.Load() })
	p.mu.Lock()
	delete(p.cache, headKey{"1672", "rpc"})
	p.mu.Unlock()
	before, _ := service.counts()
	if _, err := w1.blockNumber(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := w2.blockNumber(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, _ := service.counts()
	if after-before != 1 {
		t.Fatalf("periodic head calls %d, want 1", after-before)
	}
}
