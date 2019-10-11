package backends

import (
	"github.com/ehengao/machinery/v2/config"
)

// Backend represents a base backend structure
type Backend struct {
	cnf *config.Config
}

// New creates new Backend instance
func New(cnf *config.Config) Backend {
	return Backend{cnf: cnf}
}

// IsAMQP returns true if the backend is AMQP
func IsAMQP(b Interface) bool {
	return false
}
