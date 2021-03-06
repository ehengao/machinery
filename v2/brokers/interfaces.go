package brokers

import (
	"github.com/ehengao/machinery/v2/config"
	"github.com/ehengao/machinery/v2/tasks"
)

// Interface - a common interface for all brokers
type Interface interface {
	GetConfig() *config.Config
	SetRegisteredTaskNames(names []string)
	IsTaskRegistered(name string) bool
	StartConsuming(consumerTag string, concurrency int, p TaskProcessor) (bool, error)
	StopConsuming()
	Publish(task *tasks.Signature) error
	GetPendingTasks(queue string) ([]*tasks.Signature, error)
}

// TaskProcessor - can process a delivered task
// This will probably always be a worker instance
type TaskProcessor interface {
	Process(signature *tasks.Signature) error
	ProcessBatch(parent *tasks.Signature, signatures []*tasks.Signature, pool chan struct{}) error
}
