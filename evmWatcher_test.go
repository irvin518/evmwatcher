package evmwatcher

import (
	"context"
	"errors"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

type recordingStorage struct {
	mu       sync.Mutex
	setBlock uint64
	setCalls int
}

type failingStorage struct {
	recordingStorage
	err error
}

func (s *failingStorage) SetWatchedBlockNumber(_ string, blockNumber uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setBlock = blockNumber
	s.setCalls++
	return s.err
}

func (s *recordingStorage) GetWatchedBlockNumber(string) (int64, error) {
	return -1, nil
}

func (s *recordingStorage) SetWatchedBlockNumber(_ string, blockNumber uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setBlock = blockNumber
	s.setCalls++
	return nil
}

func (s *recordingStorage) snapshot() (uint64, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.setBlock, s.setCalls
}

type silentLogger struct{}

func (silentLogger) Debugf(string, ...any) {}
func (silentLogger) Infof(string, ...any)  {}
func (silentLogger) Warnf(string, ...any)  {}
func (silentLogger) Errorf(string, ...any) {}

type fakeSubscription struct {
	errCh chan error
	once  sync.Once
}

func newFakeSubscription() *fakeSubscription {
	return &fakeSubscription{errCh: make(chan error, 1)}
}

func (s *fakeSubscription) Err() <-chan error { return s.errCh }
func (s *fakeSubscription) Unsubscribe() {
	s.once.Do(func() { close(s.errCh) })
}

var _ ethereum.Subscription = (*fakeSubscription)(nil)

type eventHandlerFunc func(string, *Event) error

func (f eventHandlerFunc) OnEvent(chain string, event *Event) error { return f(chain, event) }

func newUnitWatcher(storage StorageInterface, handler EventInterface) (*EVMWatcher, context.CancelFunc) {
	w := NewEVMWatcher("test-chain", "ws://unused", nil, storage, handler, WithLogger(silentLogger{}))
	ctx, cancel := context.WithCancel(context.Background())
	w.ctx = ctx
	w.cancel = cancel
	w.logCh = make(chan types.Log, 32)
	w.eventCh = make(chan *Event, 32)
	w.subFailures = make(chan subscriptionFailure, 8)
	w.catchUpDoneCh = make(chan struct{}, 1)
	return w, cancel
}

func TestValidateStoredWatermark(t *testing.T) {
	tests := []struct {
		name    string
		stored  int64
		head    uint64
		startAt uint64
		wantErr string
	}{
		{name: "missing watermark", stored: -1, head: 100, startAt: 100},
		{name: "at head", stored: 100, head: 100, startAt: 100},
		{name: "below head", stored: 99, head: 100, startAt: 100},
		{name: "at safe head", stored: 88, head: 100, startAt: 88},
		{
			name:    "ahead of chain head",
			stored:  101,
			head:    100,
			startAt: 100,
			wantErr: "stored watermark 101 is ahead of chain head 100 for chain pharos-mainnet-1672",
		},
		{
			name:    "ahead of safe head",
			stored:  95,
			head:    100,
			startAt: 88,
			wantErr: "stored watermark 95 is ahead of safe head 88 (chain head 100) for chain pharos-mainnet-1672",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateStoredWatermark("pharos-mainnet-1672", tt.stored, tt.head, tt.startAt)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("validateStoredWatermark() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("validateStoredWatermark() error = %v, want %q", err, tt.wantErr)
			}
		})
	}
}

func TestApplyGapStrategyDoesNotUnderflow(t *testing.T) {
	watcher := &EVMWatcher{
		skipGapThreshold: 1,
		lastBlock:        70_670_610,
	}
	watcher.deliveredBlock.Store(70_670_610)
	watcher.reportedBlock = 70_670_610

	watcher.applyGapStrategy(17_829_084, 70_670_610)

	if got := watcher.getLastBlock(); got != 70_670_610 {
		t.Fatalf("lastBlock = %d, want %d", got, uint64(70_670_610))
	}
	if got := watcher.deliveredBlock.Load(); got != 70_670_610 {
		t.Fatalf("deliveredBlock = %d, want %d", got, uint64(70_670_610))
	}
	if got := watcher.getReportedBlock(); got != 70_670_610 {
		t.Fatalf("reportedBlock = %d, want %d", got, uint64(70_670_610))
	}
}

func TestApplyGapStrategyUsesSafeHead(t *testing.T) {
	storage := &recordingStorage{}
	watcher := NewEVMWatcher(
		"pharos-mainnet-1672",
		"ws://unused",
		nil,
		storage,
		nil,
		WithConfirmations(12),
		WithSkipGapThreshold(5),
	)
	watcher.latestHead.Store(100)
	watcher.reportedBlock = 80

	watcher.applyGapStrategy(100, 80)

	const want = uint64(88)
	if got := watcher.getLastBlock(); got != want {
		t.Fatalf("lastBlock = %d, want %d", got, want)
	}
	if got := watcher.deliveredBlock.Load(); got != want {
		t.Fatalf("deliveredBlock = %d, want %d", got, want)
	}
	if got := watcher.getReportedBlock(); got != want {
		t.Fatalf("reportedBlock = %d, want %d", got, want)
	}
	block, calls := storage.snapshot()
	if calls != 1 || block != want {
		t.Fatalf("storage writes = (%d, %d), want (1, %d)", calls, block, want)
	}
}

func TestSubscriptionFailureForwardingTracksCurrentGeneration(t *testing.T) {
	w, cancel := newUnitWatcher(&recordingStorage{}, eventHandlerFunc(func(string, *Event) error { return nil }))
	first := newFakeSubscription()
	firstGeneration, _ := w.installSubscription(first)
	w.forwardSubscriptionErrors(firstGeneration, first)

	second := newFakeSubscription()
	secondGeneration, old := w.installSubscription(second)
	w.forwardSubscriptionErrors(secondGeneration, second)
	old.Unsubscribe()

	select {
	case failure := <-w.subFailures:
		if failure.Generation != firstGeneration || w.isCurrentSubscription(failure.Generation) {
			t.Fatalf("old failure = generation %d current=%v", failure.Generation, w.isCurrentSubscription(failure.Generation))
		}
	case <-time.After(time.Second):
		t.Fatal("old subscription closure was not forwarded")
	}

	wantErr := errors.New("connection lost before first log")
	second.errCh <- wantErr
	select {
	case failure := <-w.subFailures:
		if failure.Generation != secondGeneration || !w.isCurrentSubscription(failure.Generation) {
			t.Fatalf("current failure = generation %d current=%v", failure.Generation, w.isCurrentSubscription(failure.Generation))
		}
		if !errors.Is(failure.Err, wantErr) {
			t.Fatalf("failure error = %v, want %v", failure.Err, wantErr)
		}
	case <-time.After(time.Second):
		t.Fatal("current subscription failure was not forwarded")
	}
	cancel()
	w.wg.Wait()
}

func TestCatchUpBuffersLogBeyondTargetAndDeliversIt(t *testing.T) {
	parsed, err := abi.JSON(strings.NewReader(`[{"anonymous":false,"inputs":[{"indexed":false,"name":"value","type":"uint256"}],"name":"Test","type":"event"}]`))
	if err != nil {
		t.Fatal(err)
	}
	contract := common.HexToAddress("0x1234")
	abiEvent := parsed.Events["Test"]
	data, err := abiEvent.Inputs.NonIndexed().Pack(big.NewInt(7))
	if err != nil {
		t.Fatal(err)
	}
	delivered := make(chan *Event, 1)
	w, cancel := newUnitWatcher(&recordingStorage{}, eventHandlerFunc(func(_ string, event *Event) error {
		delivered <- event
		return nil
	}))
	w.targets[contract] = &watchTarget{address: contract, parsed: parsed, events: map[common.Hash]abi.Event{abiEvent.ID: abiEvent}}
	w.lastBlock = 99
	w.committedThrough.Store(99)
	w.reportedBlock = 99
	w.beginCatchUp(100)
	w.wg.Add(2)
	go w.notifyLoop()
	go w.eventLoop()
	w.logCh <- types.Log{
		Address:     contract,
		Topics:      []common.Hash{abiEvent.ID},
		Data:        data,
		BlockNumber: 101,
		BlockHash:   common.HexToHash("0x101"),
		TxHash:      common.HexToHash("0xabc"),
		Index:       2,
	}
	select {
	case <-delivered:
		t.Fatal("log was delivered before catch-up completed")
	case <-time.After(30 * time.Millisecond):
	}
	w.setLastBlock(100)
	w.endCatchUp()
	select {
	case event := <-delivered:
		if event.BlockNumber != 101 {
			t.Fatalf("delivered block = %d, want 101", event.BlockNumber)
		}
	case <-time.After(time.Second):
		t.Fatal("buffered block 101 log was not delivered")
	}
	cancel()
	w.wg.Wait()
}

func TestDeliveryFailureDoesNotAdvanceCommitAndRetries(t *testing.T) {
	storage := &recordingStorage{}
	var calls atomic.Int32
	w, cancel := newUnitWatcher(storage, eventHandlerFunc(func(string, *Event) error {
		if calls.Add(1) == 1 {
			return errors.New("redis unavailable")
		}
		return nil
	}))
	w.lastBlock = 100
	w.committedThrough.Store(99)
	w.reportedBlock = 99
	event := &Event{BlockNumber: 100, BlockHash: common.HexToHash("0x100"), TxHash: common.HexToHash("0x1"), LogIndex: 1}
	if !w.registerEvent(event) {
		t.Fatal("event unexpectedly deduplicated")
	}
	w.advanceCommittedThrough()
	done := make(chan struct{})
	go func() {
		w.deliverEvent(event)
		close(done)
	}()

	deadline := time.After(time.Second)
	for calls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("first delivery attempt did not run")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if got := w.committedThrough.Load(); got != 99 {
		t.Fatalf("committedThrough after failure = %d, want 99", got)
	}
	if _, count := storage.snapshot(); count != 0 {
		t.Fatalf("storage writes after failure = %d, want 0", count)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("failed event was not retried successfully")
	}
	if got := w.committedThrough.Load(); got != 100 {
		t.Fatalf("committedThrough after retry = %d, want 100", got)
	}
	block, count := storage.snapshot()
	if count != 1 || block != 100 {
		t.Fatalf("storage writes after retry = (%d, %d), want (1, 100)", count, block)
	}
	cancel()
}

func TestAllEventsInBlockMustSucceedBeforeCommit(t *testing.T) {
	storage := &recordingStorage{}
	w, cancel := newUnitWatcher(storage, eventHandlerFunc(func(string, *Event) error { return nil }))
	defer cancel()
	w.lastBlock = 100
	w.committedThrough.Store(99)
	w.reportedBlock = 99
	first := &Event{BlockNumber: 100, TxHash: common.HexToHash("0x1"), LogIndex: 1}
	second := &Event{BlockNumber: 100, TxHash: common.HexToHash("0x2"), LogIndex: 2}
	w.registerEvent(first)
	w.registerEvent(second)
	w.deliverEvent(first)
	if got := w.committedThrough.Load(); got != 99 {
		t.Fatalf("committedThrough after one event = %d, want 99", got)
	}
	w.deliverEvent(second)
	if got := w.committedThrough.Load(); got != 100 {
		t.Fatalf("committedThrough after both events = %d, want 100", got)
	}
}

func TestDeliveryRetryStopsOnCancellation(t *testing.T) {
	var calls atomic.Int32
	w, cancel := newUnitWatcher(&recordingStorage{}, eventHandlerFunc(func(string, *Event) error {
		calls.Add(1)
		return errors.New("still unavailable")
	}))
	w.lastBlock = 100
	w.committedThrough.Store(99)
	w.reportedBlock = 99
	event := &Event{BlockNumber: 100, TxHash: common.HexToHash("0x1"), LogIndex: 1}
	w.registerEvent(event)
	done := make(chan struct{})
	go func() {
		w.deliverEvent(event)
		close(done)
	}()
	deadline := time.After(time.Second)
	for calls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("delivery attempt did not run")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("delivery retry did not stop after cancellation")
	}
	if got := w.committedThrough.Load(); got != 99 {
		t.Fatalf("committedThrough after cancelled retry = %d, want 99", got)
	}
}

func TestLiveHighBlockDoesNotAdvanceScannedThrough(t *testing.T) {
	w, cancel := newUnitWatcher(&recordingStorage{}, eventHandlerFunc(func(string, *Event) error { return nil }))
	defer cancel()
	w.lastBlock = 100
	w.committedThrough.Store(100)
	w.reportedBlock = 100
	event := &Event{BlockNumber: 105, TxHash: common.HexToHash("0x5"), LogIndex: 1}
	w.registerEvent(event)
	w.deliverEvent(event)
	if got := w.getLastBlock(); got != 100 {
		t.Fatalf("scannedThrough = %d, want 100", got)
	}
	if got := w.committedThrough.Load(); got != 100 {
		t.Fatalf("committedThrough = %d, want 100", got)
	}
}

func TestRemovedLogHasSeparateDedupeIdentity(t *testing.T) {
	w, cancel := newUnitWatcher(&recordingStorage{}, eventHandlerFunc(func(string, *Event) error { return nil }))
	defer cancel()
	base := &Event{BlockNumber: 10, BlockHash: common.HexToHash("0x10"), TxHash: common.HexToHash("0xa"), LogIndex: 3}
	removed := *base
	removed.Removed = true
	if !w.registerEvent(base) || !w.registerEvent(&removed) {
		t.Fatal("removed log was incorrectly deduplicated with the original log")
	}
}

func TestSubscriptionAndBackfillCopiesAreDeduplicated(t *testing.T) {
	w, cancel := newUnitWatcher(&recordingStorage{}, eventHandlerFunc(func(string, *Event) error { return nil }))
	defer cancel()
	event := &Event{BlockNumber: 10, BlockHash: common.HexToHash("0x10"), TxHash: common.HexToHash("0xa"), LogIndex: 3}
	copyOfEvent := *event
	if !w.registerEvent(event) {
		t.Fatal("first event unexpectedly deduplicated")
	}
	if w.registerEvent(&copyOfEvent) {
		t.Fatal("same event from another source was not deduplicated")
	}
}

func TestStorageFailureLeavesDurableProgressBehindForReplay(t *testing.T) {
	storage := &failingStorage{err: errors.New("storage unavailable")}
	w, cancel := newUnitWatcher(storage, eventHandlerFunc(func(string, *Event) error { return nil }))
	defer cancel()
	w.lastBlock = 100
	w.committedThrough.Store(99)
	w.reportedBlock = 99
	event := &Event{BlockNumber: 100, TxHash: common.HexToHash("0x1"), LogIndex: 1}
	w.registerEvent(event)
	w.deliverEvent(event)
	if got := w.committedThrough.Load(); got != 100 {
		t.Fatalf("in-memory committedThrough = %d, want 100", got)
	}
	if got := w.getReportedBlock(); got != 99 {
		t.Fatalf("durable reportedBlock = %d, want 99 so restart replays block 100", got)
	}
	block, calls := storage.snapshot()
	if calls != 1 || block != 100 {
		t.Fatalf("storage attempts = (%d, %d), want (1, 100)", calls, block)
	}
}

func TestGapHealIntervalIsIndependentFromPollInterval(t *testing.T) {
	w := NewEVMWatcher("test", "ws://unused", nil, nil, nil,
		WithPollInterval(7*time.Second), WithGapHealInterval(2*time.Second))
	if w.pollInterval != 7*time.Second || w.gapHealInterval != 2*time.Second {
		t.Fatalf("intervals = poll %s gap-heal %s", w.pollInterval, w.gapHealInterval)
	}
}
