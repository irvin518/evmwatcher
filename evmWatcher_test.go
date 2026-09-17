package evmwatcher

import (
	"strings"
	"testing"
)

type recordingStorage struct {
	setBlock uint64
	setCalls int
}

func (s *recordingStorage) GetWatchedBlockNumber(string) (int64, error) {
	return -1, nil
}

func (s *recordingStorage) SetWatchedBlockNumber(_ string, blockNumber uint64) error {
	s.setBlock = blockNumber
	s.setCalls++
	return nil
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
	if storage.setCalls != 1 || storage.setBlock != want {
		t.Fatalf("storage writes = (%d, %d), want (1, %d)", storage.setCalls, storage.setBlock, want)
	}
}
