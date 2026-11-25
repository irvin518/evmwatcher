package evmwatcher

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type logClientFunc func(context.Context, ethereum.FilterQuery) ([]types.Log, error)

func (f logClientFunc) FilterLogs(ctx context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
	return f(ctx, q)
}
func logQuery(address common.Address, topic common.Hash, from, to uint64) ethereum.FilterQuery {
	return ethereum.FilterQuery{FromBlock: new(big.Int).SetUint64(from), ToBlock: new(big.Int).SetUint64(to), Addresses: []common.Address{address}, Topics: [][]common.Hash{{topic}}}
}
func TestSharedLogProviderMergesAndFiltersPairsAndRanges(t *testing.T) {
	a, b := common.HexToAddress("0x1"), common.HexToAddress("0x2")
	x, y := common.HexToHash("0x11"), common.HexToHash("0x22")
	var calls atomic.Int64
	client := logClientFunc(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
		calls.Add(1)
		if q.FromBlock.Uint64() != 10 || q.ToBlock.Uint64() != 18 || len(q.Addresses) != 2 {
			t.Errorf("merged query %+v", q)
		}
		return []types.Log{{Address: a, Topics: []common.Hash{x}, BlockNumber: 10}, {Address: a, Topics: []common.Hash{y}, BlockNumber: 12}, {Address: a, Topics: []common.Hash{x}, BlockNumber: 17}, {Address: b, Topics: []common.Hash{y}, BlockNumber: 18}, {Address: b, Topics: []common.Hash{x}, BlockNumber: 12}}, nil
	})
	p, err := newSharedLogProvider(client, SharedLogConfig{ChainID: "1672", RPCIdentity: "rpc", Window: 100 * time.Millisecond, MaxBlockRange: 20, MaxAddresses: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	out := make(chan sharedLogResponse, 2)
	run := func(q ethereum.FilterQuery) {
		logs, err := p.filterLogs(context.Background(), client, "1672", "rpc", 0, q)
		out <- sharedLogResponse{logs, err}
	}
	go run(logQuery(a, x, 10, 15))
	go run(logQuery(b, y, 12, 18))
	r1, r2 := <-out, <-out
	if r1.err != nil || r2.err != nil || calls.Load() != 1 {
		t.Fatalf("errors %v %v calls=%d", r1.err, r2.err, calls.Load())
	}
	results := map[common.Address][]types.Log{}
	for _, r := range []sharedLogResponse{r1, r2} {
		if len(r.logs) != 1 {
			t.Fatalf("unexpected logs %+v", r.logs)
		}
		results[r.logs[0].Address] = r.logs
	}
	if results[a][0].BlockNumber != 10 || results[b][0].BlockNumber != 18 {
		t.Fatal("wrong distribution")
	}
	stats := p.Stats()
	t.Logf("requests=%d getLogs=%d merged_batches=%d merged_requests=%d", stats.Requests, calls.Load(), stats.MergedBatches, stats.MergedRequests)
}
func TestSharedLogProviderIsolationAndMissedWindow(t *testing.T) {
	var calls atomic.Int64
	client := logClientFunc(func(context.Context, ethereum.FilterQuery) ([]types.Log, error) { calls.Add(1); return nil, nil })
	p, _ := newSharedLogProvider(client, SharedLogConfig{ChainID: "a", RPCIdentity: "rpc", Window: 20 * time.Millisecond, MaxBlockRange: 10, MaxAddresses: 5})
	defer p.Close()
	q := logQuery(common.HexToAddress("0x1"), common.HexToHash("0x1"), 1, 3)
	for _, group := range [][3]string{{"a", "rpc", "0"}, {"b", "rpc", "0"}, {"a", "other", "0"}, {"a", "rpc", "1"}} {
		g := group
		go func() {
			conf := uint64(0)
			if g[2] == "1" {
				conf = 1
			}
			_, _ = p.filterLogs(context.Background(), client, g[0], g[1], conf, q)
		}()
	}
	waitFor(t, func() bool { return calls.Load() == 4 })
	if _, err := p.filterLogs(context.Background(), client, "a", "rpc", 0, q); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 5 {
		t.Fatalf("calls=%d", calls.Load())
	}
}
func TestSharedLogProviderCancelOneAndAll(t *testing.T) {
	gate, started, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	client := logClientFunc(func(ctx context.Context, _ ethereum.FilterQuery) ([]types.Log, error) {
		once.Do(func() { close(started) })
		select {
		case <-gate:
			return nil, nil
		case <-ctx.Done():
			close(stopped)
			return nil, ctx.Err()
		}
	})
	p, _ := newSharedLogProvider(client, SharedLogConfig{ChainID: "1", RPCIdentity: "rpc", Window: 100 * time.Millisecond, MaxBlockRange: 10, MaxAddresses: 5})
	defer p.Close()
	a, b := context.WithCancel(context.Background())
	c, d := context.WithCancel(context.Background())
	q := logQuery(common.HexToAddress("0x1"), common.HexToHash("0x1"), 1, 2)
	results := make(chan error, 2)
	go func() { _, err := p.filterLogs(a, client, "1", "rpc", 0, q); results <- err }()
	go func() { _, err := p.filterLogs(c, client, "1", "rpc", 0, q); results <- err }()
	<-started
	b()
	if err := <-results; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-stopped:
		t.Fatal("one cancellation stopped the other watcher")
	default:
	}
	d()
	if err := <-results; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("shared RPC was not canceled")
	}
}
func TestSharedLogProviderFailureAndExplicitFallback(t *testing.T) {
	a, b := common.HexToAddress("0x1"), common.HexToAddress("0x2")
	topic := common.HexToHash("0x1")
	rpcErr := errors.New("429 rate limit")
	var combined, independent atomic.Int64
	shared := logClientFunc(func(context.Context, ethereum.FilterQuery) ([]types.Log, error) { combined.Add(1); return nil, rpcErr })
	solo := logClientFunc(func(context.Context, ethereum.FilterQuery) ([]types.Log, error) { independent.Add(1); return nil, nil })
	p, _ := newSharedLogProvider(shared, SharedLogConfig{ChainID: "1", RPCIdentity: "rpc", Window: 30 * time.Millisecond, MaxBlockRange: 10, MaxAddresses: 2})
	defer p.Close()
	runPair := func() []error {
		ch := make(chan error, 2)
		for _, addr := range []common.Address{a, b} {
			go func(address common.Address) {
				_, err := p.filterLogs(context.Background(), solo, "1", "rpc", 0, logQuery(address, topic, 1, 2))
				ch <- err
			}(addr)
		}
		return []error{<-ch, <-ch}
	}
	errs := runPair()
	if !errors.Is(errs[0], rpcErr) || !errors.Is(errs[1], rpcErr) || independent.Load() != 0 {
		t.Fatalf("errors=%v independent=%d", errs, independent.Load())
	}
	rpcErr = errors.New("address array not supported")
	errs = runPair()
	if errs[0] != nil || errs[1] != nil || combined.Load() != 2 || independent.Load() != 2 {
		t.Fatalf("fallback=%v combined=%d independent=%d", errs, combined.Load(), independent.Load())
	}
}

func TestSharedLogProviderNonoverlappingAndUnsupportedQueriesStayIndependent(t *testing.T) {
	var calls atomic.Int64
	client := logClientFunc(func(context.Context, ethereum.FilterQuery) ([]types.Log, error) { calls.Add(1); return nil, nil })
	p, _ := newSharedLogProvider(client, SharedLogConfig{ChainID: "1", RPCIdentity: "rpc", Window: 30 * time.Millisecond, MaxBlockRange: 20, MaxAddresses: 3})
	defer p.Close()
	a := common.HexToAddress("0x1")
	topic := common.HexToHash("0x1")
	done := make(chan error, 2)
	for _, q := range []ethereum.FilterQuery{logQuery(a, topic, 1, 4), logQuery(a, topic, 8, 10)} {
		go func(query ethereum.FilterQuery) {
			_, err := p.filterLogs(context.Background(), client, "1", "rpc", 0, query)
			done <- err
		}(q)
	}
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("nonoverlap calls=%d", calls.Load())
	}
	wildcard := logQuery(a, topic, 1, 4)
	wildcard.Topics = nil
	if _, err := p.filterLogs(context.Background(), client, "1", "rpc", 0, wildcard); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatalf("wildcard query calls=%d", calls.Load())
	}
}
func TestSharedLogProviderSyntheticCallReduction(t *testing.T) {
	var calls atomic.Int64
	client := logClientFunc(func(context.Context, ethereum.FilterQuery) ([]types.Log, error) { calls.Add(1); return nil, nil })
	p, _ := newSharedLogProvider(client, SharedLogConfig{ChainID: "1", RPCIdentity: "rpc", Window: 50 * time.Millisecond, MaxBlockRange: 20, MaxAddresses: 2})
	defer p.Close()
	topic := common.HexToHash("0x1")
	const rounds = 10
	var baselineCalls atomic.Int64
	baseline := logClientFunc(func(context.Context, ethereum.FilterQuery) ([]types.Log, error) {
		baselineCalls.Add(1)
		return nil, nil
	})
	for round := 0; round < rounds; round++ {
		for i := 1; i <= 2; i++ {
			address := common.BigToAddress(big.NewInt(int64(i)))
			if _, err := baseline.FilterLogs(context.Background(), logQuery(address, topic, uint64(round*10), uint64(round*10+9))); err != nil {
				t.Fatal(err)
			}
		}
	}
	for round := 0; round < rounds; round++ {
		done := make(chan error, 2)
		for i := 1; i <= 2; i++ {
			address := common.BigToAddress(big.NewInt(int64(i)))
			go func() {
				_, err := p.filterLogs(context.Background(), client, "1", "rpc", 0, logQuery(address, topic, uint64(round*10), uint64(round*10+9)))
				done <- err
			}()
		}
		for i := 0; i < 2; i++ {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
	}
	stats := p.Stats()
	if baselineCalls.Load() != 2*rounds || calls.Load() != rounds || stats.Requests != 2*rounds || stats.MergedRequests != 2*rounds || stats.MergedBatches != rounds {
		t.Fatalf("calls=%d stats=%+v", calls.Load(), stats)
	}
	t.Logf("synthetic: two same-range watcher queries per round; independent getLogs=%d shared getLogs=%d merged_requests=%d/%d (%.0f%%)", baselineCalls.Load(), calls.Load(), stats.MergedRequests, stats.Requests, 100*float64(stats.MergedRequests)/float64(stats.Requests))
}

func TestSharedLogProviderMismatchedIdentityNeverUsesSharedClient(t *testing.T) {
	var sharedCalls, ownCalls atomic.Int64
	shared := logClientFunc(func(context.Context, ethereum.FilterQuery) ([]types.Log, error) {
		sharedCalls.Add(1)
		return nil, errors.New("wrong backend")
	})
	own := logClientFunc(func(context.Context, ethereum.FilterQuery) ([]types.Log, error) { ownCalls.Add(1); return nil, nil })
	p, err := newSharedLogProvider(shared, SharedLogConfig{ChainID: "1", RPCIdentity: "rpc-a", Window: 30 * time.Millisecond, MaxBlockRange: 10, MaxAddresses: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	q := logQuery(common.HexToAddress("0x1"), common.HexToHash("0x1"), 1, 2)
	done := make(chan error, 4)
	for _, id := range [][2]string{{"2", "rpc-a"}, {"2", "rpc-a"}, {"1", "rpc-b"}, {"1", "rpc-b"}} {
		pair := id
		go func() { _, err := p.filterLogs(context.Background(), own, pair[0], pair[1], 0, q); done <- err }()
	}
	for i := 0; i < 4; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if sharedCalls.Load() != 0 || ownCalls.Load() != 4 {
		t.Fatalf("shared calls=%d independent calls=%d", sharedCalls.Load(), ownCalls.Load())
	}
}

func TestSharedLogProviderRespectsMergeLimits(t *testing.T) {
	cases := []struct {
		name          string
		maxAddresses  int
		maxRange      uint64
		first, second ethereum.FilterQuery
	}{
		{"address limit", 1, 10, logQuery(common.HexToAddress("0x1"), common.HexToHash("0x1"), 1, 3), logQuery(common.HexToAddress("0x2"), common.HexToHash("0x1"), 1, 3)},
		{"union range limit", 2, 5, logQuery(common.HexToAddress("0x1"), common.HexToHash("0x1"), 1, 3), logQuery(common.HexToAddress("0x2"), common.HexToHash("0x1"), 3, 6)},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int64
			client := logClientFunc(func(context.Context, ethereum.FilterQuery) ([]types.Log, error) { calls.Add(1); return nil, nil })
			p, err := newSharedLogProvider(client, SharedLogConfig{ChainID: "1", RPCIdentity: "rpc", Window: 30 * time.Millisecond, MaxBlockRange: tt.maxRange, MaxAddresses: tt.maxAddresses})
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			done := make(chan error, 2)
			for _, q := range []ethereum.FilterQuery{tt.first, tt.second} {
				go func(query ethereum.FilterQuery) {
					_, err := p.filterLogs(context.Background(), client, "1", "rpc", 0, query)
					done <- err
				}(q)
			}
			for i := 0; i < 2; i++ {
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			}
			if calls.Load() != 2 || p.Stats().MergedBatches != 0 {
				t.Fatalf("calls=%d stats=%+v", calls.Load(), p.Stats())
			}
		})
	}
}

func TestSharedLogProviderTimeoutReturnsErrorsAndStopsRPC(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	client := logClientFunc(func(ctx context.Context, _ ethereum.FilterQuery) ([]types.Log, error) {
		close(started)
		<-ctx.Done()
		close(stopped)
		return nil, ctx.Err()
	})
	p, err := newSharedLogProvider(client, SharedLogConfig{ChainID: "1", RPCIdentity: "rpc", Window: 100 * time.Millisecond, MaxBlockRange: 10, MaxAddresses: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	done := make(chan error, 2)
	for i := 1; i <= 2; i++ {
		address := common.BigToAddress(big.NewInt(int64(i)))
		go func() {
			_, err := p.filterLogs(ctx, client, "1", "rpc", 0, logQuery(address, common.HexToHash("0x1"), 1, 2))
			done <- err
		}()
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("shared request did not start")
	}
	for i := 0; i < 2; i++ {
		if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("timeout error=%v", err)
		}
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("timed-out request did not stop")
	}
}

func TestSharedLogProviderUnsupportedUnionPartialFallbackFailure(t *testing.T) {
	a, b := common.HexToAddress("0x1"), common.HexToAddress("0x2")
	topic := common.HexToHash("0x1")
	upstream := errors.New("independent RPC failed")
	shared := logClientFunc(func(context.Context, ethereum.FilterQuery) ([]types.Log, error) {
		return nil, errors.New("address array not supported")
	})
	own := logClientFunc(func(_ context.Context, q ethereum.FilterQuery) ([]types.Log, error) {
		if q.Addresses[0] == b {
			return nil, upstream
		}
		return []types.Log{{Address: a, Topics: []common.Hash{topic}, BlockNumber: 2}}, nil
	})
	p, err := newSharedLogProvider(shared, SharedLogConfig{ChainID: "1", RPCIdentity: "rpc", Window: 80 * time.Millisecond, MaxBlockRange: 10, MaxAddresses: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	out := make(chan sharedLogResponse, 2)
	for _, address := range []common.Address{a, b} {
		go func(addr common.Address) {
			logs, err := p.filterLogs(context.Background(), own, "1", "rpc", 0, logQuery(addr, topic, 1, 2))
			out <- sharedLogResponse{logs, err}
		}(address)
	}
	r1, r2 := <-out, <-out
	if r1.err == nil {
		r1, r2 = r2, r1
	}
	if !errors.Is(r1.err, upstream) || r2.err != nil || len(r2.logs) != 1 || r2.logs[0].Address != a {
		t.Fatalf("first=%+v second=%+v", r1, r2)
	}
}
