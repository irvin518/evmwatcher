package evmwatcher

import "log"

// Logger is the minimal logging interface used inside evmwatcher.
// Callers can inject their own implementation via WithLogger.
type Logger interface {
	Debugf(format string, args ...any)
	Infof(format string, args ...any)
	Warnf(format string, args ...any)
	Errorf(format string, args ...any)
}

type stdLogger struct {
	prefix string
}

func defaultLogger(chainName string) Logger {
	return stdLogger{prefix: "evmwatcher: [" + chainName + "] "}
}

func (l stdLogger) Debugf(format string, args ...any) {
	log.Printf(l.prefix+format, args...)
}

func (l stdLogger) Infof(format string, args ...any) {
	log.Printf(l.prefix+format, args...)
}

func (l stdLogger) Warnf(format string, args ...any) {
	log.Printf(l.prefix+format, args...)
}

func (l stdLogger) Errorf(format string, args ...any) {
	log.Printf(l.prefix+format, args...)
}

// WithLogger injects a custom logger. nil is ignored.
func WithLogger(logger Logger) Option {
	return func(e *EVMWatcher) {
		if logger != nil {
			e.logger = logger
		}
	}
}

// WithDebug enables Debug-level logs such as raw subscription logs.
func WithDebug(enabled bool) Option {
	return func(e *EVMWatcher) {
		e.debug = enabled
	}
}
