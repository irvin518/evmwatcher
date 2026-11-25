package evmwatcher

import (
	"context"
	"errors"
	"math/big"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// SharedLogConfig bounds merged requests to the limits of the shared RPC.
// Window adds at most this much wait to collect compatible requests.
type SharedLogConfig struct {
	ChainID       string
	RPCIdentity   string
	Window        time.Duration
	MaxBlockRange uint64
	MaxAddresses  int
}

// SharedLogStats counts logical merged attempts, not RPC billing units.
type SharedLogStats struct{ Requests, MergedBatches, MergedRequests, IndependentCalls uint64 }
type logFilterClient interface {
	FilterLogs(context.Context, ethereum.FilterQuery) ([]types.Log, error)
}
type sharedLogGroup struct {
	chainID, rpcIdentity string
	confirmations        uint64
}
type sharedLogRequest struct {
	ctx    context.Context
	query  ethereum.FilterQuery
	client logFilterClient
	result chan sharedLogResponse
}
type sharedLogResponse struct {
	logs []types.Log
	err  error
}
type sharedLogBatch struct {
	group     sharedLogGroup
	from, to  uint64
	addresses map[common.Address]struct{}
	requests  []*sharedLogRequest
}

// SharedLogProvider combines only compatible getLogs requests. It owns no
// subscriptions, event handling or watermarks. Its client is caller-owned and
// must remain open until Close returns.
type SharedLogProvider struct {
	client                                               logFilterClient
	config                                               SharedLogConfig
	ctx                                                  context.Context
	cancel                                               context.CancelFunc
	mu                                                   sync.Mutex
	closed                                               bool
	batches                                              map[*sharedLogBatch]struct{}
	wg                                                   sync.WaitGroup
	requests, mergedBatches, mergedRequests, independent atomic.Uint64
}

// NewSharedLogProvider creates an optional shared query provider. The client
// must refer to the same RPC identity supplied to each watcher using it.
func NewSharedLogProvider(client *ethclient.Client, config SharedLogConfig) (*SharedLogProvider, error) {
	if client == nil {
		return nil, errors.New("shared log RPC client is required")
	}
	return newSharedLogProvider(client, config)
}
func newSharedLogProvider(client logFilterClient, config SharedLogConfig) (*SharedLogProvider, error) {
	if client == nil || config.ChainID == "" || config.RPCIdentity == "" || config.Window < 0 || config.MaxBlockRange == 0 || config.MaxAddresses <= 0 {
		return nil, errors.New("invalid shared log configuration")
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &SharedLogProvider{client: client, config: config, ctx: ctx, cancel: cancel, batches: make(map[*sharedLogBatch]struct{})}, nil
}

// Close cancels outstanding work; it does not close the caller-owned client.
func (p *SharedLogProvider) Close() {
	if p == nil {
		return
	}
	p.mu.Lock()
	if !p.closed {
		p.closed = true
		p.cancel()
	}
	p.mu.Unlock()
	p.wg.Wait()
}
func (p *SharedLogProvider) Stats() SharedLogStats {
	if p == nil {
		return SharedLogStats{}
	}
	return SharedLogStats{Requests: p.requests.Load(), MergedBatches: p.mergedBatches.Load(), MergedRequests: p.mergedRequests.Load(), IndependentCalls: p.independent.Load()}
}

// FilterLogs uses the supplied watcher client for incompatible queries and the
// dedicated shared client for merged requests. Chain and RPC identities must
// accurately identify the network and endpoint; the provider cannot infer that
// two caller-supplied identity strings describe the same backend.
func (p *SharedLogProvider) FilterLogs(ctx context.Context, client *ethclient.Client, chainID, rpcIdentity string, confirmations uint64, query ethereum.FilterQuery) ([]types.Log, error) {
	if client == nil {
		return nil, errors.New("watcher RPC client is required")
	}
	return p.filterLogs(ctx, client, chainID, rpcIdentity, confirmations, query)
}
func (p *SharedLogProvider) filterLogs(ctx context.Context, client logFilterClient, chainID, rpcIdentity string, confirmations uint64, query ethereum.FilterQuery) ([]types.Log, error) {
	if p == nil || client == nil {
		return nil, errors.New("shared provider and watcher client are required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if chainID == "" || rpcIdentity == "" {
		return nil, errors.New("shared log chain ID and RPC identity are required")
	}
	p.requests.Add(1)
	// A provider is tied to exactly one RPC. Other identities must use the
	// caller's own client, even when several such requests arrive together.
	if chainID != p.config.ChainID || rpcIdentity != p.config.RPCIdentity {
		return p.independentQuery(ctx, client, query)
	}
	from, to, ok := mergeableLogQuery(query, p.config)
	if !ok {
		return p.independentQuery(ctx, client, query)
	}
	req := &sharedLogRequest{ctx: ctx, query: copyLogQuery(query), client: client, result: make(chan sharedLogResponse, 1)}
	group := sharedLogGroup{chainID, rpcIdentity, confirmations}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("shared log provider closed")
	}
	var batch *sharedLogBatch
	for candidate := range p.batches {
		if candidate.group != group || from > candidate.to || candidate.from > to {
			continue
		}
		low, high := min(from, candidate.from), max(to, candidate.to)
		if high-low >= p.config.MaxBlockRange {
			continue
		}
		count := len(candidate.addresses)
		for _, address := range req.query.Addresses {
			if _, exists := candidate.addresses[address]; !exists {
				count++
			}
		}
		if count > p.config.MaxAddresses {
			continue
		}
		batch = candidate
		break
	}
	if batch == nil {
		batch = &sharedLogBatch{group: group, from: from, to: to, addresses: make(map[common.Address]struct{})}
		p.batches[batch] = struct{}{}
		p.wg.Add(1)
		go p.runBatch(batch)
	} else {
		batch.from = min(batch.from, from)
		batch.to = max(batch.to, to)
	}
	for _, address := range req.query.Addresses {
		batch.addresses[address] = struct{}{}
	}
	batch.requests = append(batch.requests, req)
	p.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.ctx.Done():
		return nil, errors.New("shared log provider closed")
	case result := <-req.result:
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return result.logs, result.err
	}
}
func (p *SharedLogProvider) independentQuery(ctx context.Context, client logFilterClient, q ethereum.FilterQuery) ([]types.Log, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, errors.New("shared log provider closed")
	}
	p.wg.Add(1)
	p.mu.Unlock()
	defer p.wg.Done()
	p.independent.Add(1)
	callCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(p.ctx, cancel)
	defer func() { stop(); cancel() }()
	return client.FilterLogs(callCtx, q)
}
func mergeableLogQuery(q ethereum.FilterQuery, c SharedLogConfig) (uint64, uint64, bool) {
	if q.BlockHash != nil || q.FromBlock == nil || q.ToBlock == nil || !q.FromBlock.IsUint64() || !q.ToBlock.IsUint64() || len(q.Addresses) == 0 || len(q.Addresses) > c.MaxAddresses || len(q.Topics) != 1 || len(q.Topics[0]) == 0 {
		return 0, 0, false
	}
	from, to := q.FromBlock.Uint64(), q.ToBlock.Uint64()
	if from > to || to-from >= c.MaxBlockRange {
		return 0, 0, false
	}
	return from, to, true
}
func copyLogQuery(q ethereum.FilterQuery) ethereum.FilterQuery {
	copyQ := q
	if q.FromBlock != nil {
		copyQ.FromBlock = new(big.Int).Set(q.FromBlock)
	}
	if q.ToBlock != nil {
		copyQ.ToBlock = new(big.Int).Set(q.ToBlock)
	}
	copyQ.Addresses = append([]common.Address(nil), q.Addresses...)
	copyQ.Topics = make([][]common.Hash, len(q.Topics))
	for i := range q.Topics {
		copyQ.Topics[i] = append([]common.Hash(nil), q.Topics[i]...)
	}
	return copyQ
}
func (p *SharedLogProvider) runBatch(batch *sharedLogBatch) {
	defer p.wg.Done()
	if !waitContext(p.ctx, p.config.Window) {
		return
	}
	p.mu.Lock()
	delete(p.batches, batch)
	requests := append([]*sharedLogRequest(nil), batch.requests...)
	p.mu.Unlock()
	active := make([]*sharedLogRequest, 0, len(requests))
	for _, r := range requests {
		if r.ctx.Err() == nil {
			active = append(active, r)
		}
	}
	if len(active) == 0 {
		return
	}
	if len(active) == 1 {
		p.independent.Add(1)
		callCtx, cancel := context.WithCancel(active[0].ctx)
		stop := context.AfterFunc(p.ctx, cancel)
		logs, err := active[0].client.FilterLogs(callCtx, active[0].query)
		stop()
		cancel()
		active[0].result <- sharedLogResponse{logs, err}
		return
	}
	p.mergedBatches.Add(1)
	p.mergedRequests.Add(uint64(len(active)))
	query := mergedLogQuery(active)
	batchCtx, cancel := context.WithTimeout(p.ctx, filterLogsTimeout)
	var mu sync.Mutex
	remaining := len(active)
	stops := make([]func() bool, 0, len(active))
	for _, r := range active {
		stop := context.AfterFunc(r.ctx, func() {
			mu.Lock()
			remaining--
			if remaining == 0 {
				cancel()
			}
			mu.Unlock()
		})
		stops = append(stops, stop)
	}
	logs, err := p.client.FilterLogs(batchCtx, query)
	for _, stop := range stops {
		stop()
	}
	cancel()
	if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || isRateLimitError(err) || !isUnionFilterUnsupported(err)) {
		for _, r := range active {
			r.result <- sharedLogResponse{err: err}
		}
		return
	}
	if err != nil {
		// Providers may reject unions of addresses or ranges. Retry each original
		// query independently, and propagate its own failure to its watcher.
		var wg sync.WaitGroup
		for _, request := range active {
			r := request
			wg.Add(1)
			go func() {
				defer wg.Done()
				if r.ctx.Err() != nil {
					r.result <- sharedLogResponse{err: r.ctx.Err()}
					return
				}
				p.independent.Add(1)
				callCtx, callCancel := context.WithCancel(r.ctx)
				stop := context.AfterFunc(p.ctx, callCancel)
				defer func() { stop(); callCancel() }()
				items, e := r.client.FilterLogs(callCtx, r.query)
				r.result <- sharedLogResponse{logs: items, err: e}
			}()
		}
		wg.Wait()
		return
	}
	for _, log := range logs {
		if log.BlockNumber < query.FromBlock.Uint64() || log.BlockNumber > query.ToBlock.Uint64() || !addressIn(query.Addresses, log.Address) || len(log.Topics) == 0 || !topicIn(query.Topics[0], log.Topics[0]) {
			err = errors.New("shared getLogs returned a log outside its filter")
			break
		}
	}
	for _, r := range active {
		if err != nil {
			r.result <- sharedLogResponse{err: err}
			continue
		}
		items := make([]types.Log, 0)
		for _, log := range logs {
			if log.BlockNumber >= r.query.FromBlock.Uint64() && log.BlockNumber <= r.query.ToBlock.Uint64() && addressIn(r.query.Addresses, log.Address) && topicIn(r.query.Topics[0], log.Topics[0]) {
				items = append(items, log)
			}
		}
		r.result <- sharedLogResponse{logs: items}
	}
}
func mergedLogQuery(requests []*sharedLogRequest) ethereum.FilterQuery {
	from, to := requests[0].query.FromBlock.Uint64(), requests[0].query.ToBlock.Uint64()
	addresses := make(map[common.Address]struct{})
	topics := make(map[common.Hash]struct{})
	for _, r := range requests {
		from = min(from, r.query.FromBlock.Uint64())
		to = max(to, r.query.ToBlock.Uint64())
		for _, a := range r.query.Addresses {
			addresses[a] = struct{}{}
		}
		for _, t := range r.query.Topics[0] {
			topics[t] = struct{}{}
		}
	}
	addr := make([]common.Address, 0, len(addresses))
	for a := range addresses {
		addr = append(addr, a)
	}
	sort.Slice(addr, func(i, j int) bool { return string(addr[i][:]) < string(addr[j][:]) })
	ts := make([]common.Hash, 0, len(topics))
	for t := range topics {
		ts = append(ts, t)
	}
	sort.Slice(ts, func(i, j int) bool { return string(ts[i][:]) < string(ts[j][:]) })
	return ethereum.FilterQuery{FromBlock: new(big.Int).SetUint64(from), ToBlock: new(big.Int).SetUint64(to), Addresses: addr, Topics: [][]common.Hash{ts}}
}
func addressIn(values []common.Address, needle common.Address) bool {
	for _, v := range values {
		if v == needle {
			return true
		}
	}
	return false
}
func topicIn(values []common.Hash, needle common.Hash) bool {
	for _, v := range values {
		if v == needle {
			return true
		}
	}
	return false
}

// Fallback is deliberately limited to explicit unsupported address/topic
// union responses. Rate limits, timeouts and other RPC errors are returned to all
// affected watchers for their existing retry logic.
func isUnionFilterUnsupported(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return (strings.Contains(message, "address") || strings.Contains(message, "topic")) && (strings.Contains(message, "unsupported") || strings.Contains(message, "not supported") || strings.Contains(message, "too many") || strings.Contains(message, "limit") || strings.Contains(message, "array"))
}
