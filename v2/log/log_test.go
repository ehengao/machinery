package log_test

import (
	"go.uber.org/zap"
	"testing"

	_log "github.com/ehengao/machinery/v2/log"
)

var log = _log.GetLogger()
var LogLevel = _log.LogLevel

func init() {
	LogLevel.SetLevel(zap.DebugLevel)
}

func TestDefaultLogger(t *testing.T) {
	defer log.Sync()
	log.Info("should not panic")
	log.Warn("should not panic")
	log.Error("should not panic")
	log.Named("should not panic")
	log.Debug("should not panic")
}
