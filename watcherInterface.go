package evmwatcher

type StorageInterface interface {
	// GetWatchedBlockNumber returns the last watched block number of the given
	// chain, -1 means there is no progress recorded yet.
	GetWatchedBlockNumber(chainName string) (int64, error)
	SetWatchedBlockNumber(chainName string, blockNumber uint64) error
}

// 监听到事件之后向外通知的接口
type EventInterface interface {
	// OnEvent is called with the chain the event belongs to, so that one
	// implementation can serve several chains.
	OnEvent(chainName string, event any) error
}
