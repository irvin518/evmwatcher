package evmwatcher

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

func TestChainMembershipRestartAddition(t *testing.T) {
	service := &chainTestRPC{headRPC: &headRPC{head: 10}}
	server := rpc.NewServer()
	if err := server.RegisterName("eth", service); err != nil {
		t.Fatal(err)
	}
	defer server.Stop()
	store := &chainTestStore{}
	var newCount atomic.Int64
	handler := eventHandlerFunc(func(name string, e *Event) error {
		if name == "new" {
			if e.BlockNumber != 11 {
				t.Errorf("new member got block %d", e.BlockNumber)
			}
			newCount.Add(1)
		}
		return nil
	})
	create := func(names ...string) (*ChainBackfiller, []*EVMWatcher) {
		s := newChainTestBackfiller(t, server, store, 10)
		var watchers []*EVMWatcher
		for _, name := range names {
			address := common.HexToAddress("0x1")
			if name == "new" {
				address = common.HexToAddress("0x2")
			}
			w := newChainTestWatcher(t, server, name, address.Hex(), handler)
			if err := s.RegisterWatcher(w); err != nil {
				t.Fatal(err)
			}
			if err := w.Start(); err != nil {
				t.Fatal(err)
			}
			watchers = append(watchers, w)
		}
		return s, watchers
	}
	s, ws := create("old")
	if err := s.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, w := range ws {
		w.Stop()
	}
	before, _ := store.snapshot()
	target, err := newWatchTarget(WatcherData{ContractAddress: common.HexToAddress("0x2").Hex(), ContractABI: `[{"anonymous":false,"inputs":[],"name":"Ping","type":"event"}]`})
	if err != nil {
		t.Fatal(err)
	}
	var topic common.Hash
	for key := range target.events {
		topic = key
	}
	service.mu2.Lock()
	service.events = []types.Log{{Address: common.HexToAddress("0x2"), Topics: []common.Hash{topic}, BlockNumber: 10, BlockHash: common.HexToHash("0x10"), TxHash: common.HexToHash("0x1")}, {Address: common.HexToAddress("0x2"), Topics: []common.Hash{topic}, BlockNumber: 11, BlockHash: common.HexToHash("0x11"), TxHash: common.HexToHash("0x2")}}
	service.mu2.Unlock()
	service.headRPC.mu.Lock()
	service.head = 11
	service.headRPC.mu.Unlock()
	s, ws = create("old", "new")
	defer func() {
		for _, w := range ws {
			w.Stop()
		}
	}()
	store.mu.Lock()
	store.failCommit = true
	store.mu.Unlock()
	if err := s.ScanOnce(context.Background()); err == nil {
		t.Fatal("membership CAS failure accepted")
	}
	if got, _ := store.snapshot(); got != before {
		t.Fatal("failed CAS changed state")
	}
	store.mu.Lock()
	store.failCommit = false
	store.mu.Unlock()
	if err := s.ScanOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	after, _ := store.snapshot()
	if after.Block != 11 || after.Manifest != `["new","old"]` || newCount.Load() != 1 {
		t.Fatalf("unexpected state: %+v, deliveries=%d", after, newCount.Load())
	}
	for _, w := range ws {
		w.Stop()
	}
	ws = nil
	s, ws = create("new")
	if err := s.ScanOnce(context.Background()); err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("missing old member accepted: %v", err)
	}
}
