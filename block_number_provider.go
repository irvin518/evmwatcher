package evmwatcher

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
)

// BlockNumberProvider reads the chain head. Implementations must honor ctx and
// return an error on failure. A fresh read must start a new RPC after the call
// begins, without using a cached or already in-flight result.
type BlockNumberProvider interface {
	BlockNumber(ctx context.Context, client *ethclient.Client, chainID, rpcIdentity string, fresh bool) (uint64, error)
}

// BlockNumberProviderFunc adapts a function to BlockNumberProvider.
type BlockNumberProviderFunc func(context.Context, *ethclient.Client, string, string, bool) (uint64, error)

func (f BlockNumberProviderFunc) BlockNumber(ctx context.Context, client *ethclient.Client, chainID, rpcIdentity string, fresh bool) (uint64, error) {
	return f(ctx, client, chainID, rpcIdentity, fresh)
}

type headKey struct{ chain, endpoint string }
type headEntry struct {
	head    uint64
	expires time.Time
}
type headCall struct {
	done    chan struct{}
	cancel  context.CancelFunc
	waiters int
	head    uint64
	err     error
}

// CachedBlockNumberProvider shares successful head reads by explicit chain ID
// and RPC identity. Failed and canceled reads are never cached.
type CachedBlockNumberProvider struct {
	ttl   time.Duration
	mu    sync.Mutex
	cache map[headKey]headEntry
	calls map[headKey]*headCall
}

// NewCachedBlockNumberProvider creates a concurrency-safe shared provider.
// A non-positive TTL disables caching while retaining concurrent coalescing.
func NewCachedBlockNumberProvider(ttl time.Duration) *CachedBlockNumberProvider {
	return &CachedBlockNumberProvider{ttl: ttl, cache: make(map[headKey]headEntry), calls: make(map[headKey]*headCall)}
}

func (p *CachedBlockNumberProvider) BlockNumber(ctx context.Context, client *ethclient.Client, chainID, rpcIdentity string, fresh bool) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if chainID == "" || rpcIdentity == "" {
		return 0, errors.New("cached block number provider requires chain ID and RPC identity")
	}
	key := headKey{chainID, rpcIdentity}
	if fresh {
		p.mu.Lock()
		delete(p.cache, key)
		// A request started before this boundary cannot populate the cache.
		delete(p.calls, key)
		p.mu.Unlock()
		return client.BlockNumber(ctx)
	}
	p.mu.Lock()
	if entry, ok := p.cache[key]; ok {
		if time.Now().Before(entry.expires) {
			p.mu.Unlock()
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			return entry.head, nil
		}
		delete(p.cache, key)
	}
	call := p.calls[key]
	if call == nil {
		// The request remains alive while at least one caller is waiting. The
		// bounded context also prevents an abandoned RPC from running forever.
		rpcCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), blockNumberTimeout)
		call = &headCall{done: make(chan struct{}), cancel: cancel}
		p.calls[key] = call
		go p.fetch(rpcCtx, client, key, call)
	}
	call.waiters++
	p.mu.Unlock()
	select {
	case <-ctx.Done():
		p.mu.Lock()
		call.waiters--
		if call.waiters == 0 {
			if p.calls[key] == call {
				delete(p.calls, key)
			}
			call.cancel()
		}
		p.mu.Unlock()
		return 0, ctx.Err()
	case <-call.done:
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		return call.head, call.err
	}
}

func (p *CachedBlockNumberProvider) fetch(ctx context.Context, client *ethclient.Client, key headKey, call *headCall) {
	head, err := client.BlockNumber(ctx)
	p.mu.Lock()
	if p.calls[key] == call {
		delete(p.calls, key)
		if err == nil && ctx.Err() == nil && p.ttl > 0 {
			p.cache[key] = headEntry{head: head, expires: time.Now().Add(p.ttl)}
		}
	}
	call.head, call.err = head, err
	close(call.done)
	p.mu.Unlock()
	call.cancel()
}
