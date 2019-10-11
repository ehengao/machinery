package retry

import (
	"fmt"
	"time"

	_log "github.com/ehengao/machinery/v2/log"
	"go.uber.org/zap"
)

var log = _log.GetLogger()

// Closure - a useful closure we can use when there is a problem
// connecting to the broker. It uses Fibonacci sequence to space out retry attempts
var Closure = func() func(chan int) {
	retryIn := 0
	fibonacci := Fibonacci()
	return func(stopChan chan int) {
		if retryIn > 0 {
			durationString := fmt.Sprintf("%vs", retryIn)
			duration, _ := time.ParseDuration(durationString)
			defer log.Sync()
			log.Warn("Retrying", zap.Int("seconds", retryIn))

			select {
			case <-stopChan:
				break
			case <-time.After(duration):
				break
			}
		}
		retryIn = fibonacci()
	}
}
