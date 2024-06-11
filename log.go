package rocketmq_client

import (
	"fmt"
	"time"
)

type debugHandlerFunc func(msg string)

func debugLog(cfg *Config, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if cfg.DebugHandlerFunc != nil {
		cfg.DebugHandlerFunc(msg)
	}
	if !cfg.Debug {
		return
	}
	fmt.Printf("%s %10s %s\n", time.Now().Format("2006-01-02 15:04:05.000"), "DEBUG", msg)
	return
}
