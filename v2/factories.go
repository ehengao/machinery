package machinery

import (
	"errors"
	"fmt"
	neturl "net/url"
	"strconv"
	"strings"

	"github.com/ehengao/machinery/v2/backends"
	"github.com/ehengao/machinery/v2/brokers"
	"github.com/ehengao/machinery/v2/config"
)

// BrokerFactory creates a new object of brokers.Interface
// Currently only AMQP/S broker is supported
func BrokerFactory(cnf *config.Config) (brokers.Interface, error) {
	if strings.HasPrefix(cnf.Broker, "amqp://") {
		return brokers.NewAMQPBroker(cnf), nil
	}
	return nil, fmt.Errorf("Factory failed with broker URL: %v", cnf.Broker)
}

// BackendFactory creates a new object of backends.Interface
// Currently supported backends are AMQP/S and Memcache
func BackendFactory(cnf *config.Config) (backends.Interface, error) {
	if strings.HasPrefix(cnf.ResultBackend, "redis://") {
		redisHost, redisPassword, redisDB, err := ParseRedisURL(cnf.ResultBackend)
		if err != nil {
			return nil, err
		}

		if cnf.CeleryMessage > 0 {
			return backends.NewRedisCeleryBackend(cnf, redisHost, redisPassword, "", redisDB), nil
		}
	}
	return nil, fmt.Errorf("Factory failed with result backend: %v", cnf.ResultBackend)
}

// ParseRedisURL ...
func ParseRedisURL(url string) (host, password string, db int, err error) {
	// redis://pwd@host/db

	var u *neturl.URL
	u, err = neturl.Parse(url)
	if err != nil {
		return
	}
	if u.Scheme != "redis" {
		err = errors.New("No redis scheme found")
		return
	}

	if u.User != nil {
		var exists bool
		password, exists = u.User.Password()
		if !exists {
			password = u.User.Username()
		}
	}

	host = u.Host

	parts := strings.Split(u.Path, "/")
	if len(parts) == 1 {
		db = 0 //default redis db
	} else {
		db, err = strconv.Atoi(parts[1])
		if err != nil {
			db, err = 0, nil //ignore err here
		}
	}

	return
}

// ParseRedisSocketURL extracts Redis connection options from a URL with the
// redis+socket:// scheme. This scheme is not standard (or even de facto) and
// is used as a transitional mechanism until the the config package gains the
// proper facilities to support socket-based connections.
func ParseRedisSocketURL(url string) (path, password string, db int, err error) {
	parts := strings.Split(url, "redis+socket://")
	if parts[0] != "" {
		err = errors.New("No redis scheme found")
		return
	}

	// redis+socket://password@/path/to/file.soc:/db

	if len(parts) != 2 {
		err = fmt.Errorf("Redis socket connection string should be in format redis+socket://password@/path/to/file.sock:/db, instead got %s", url)
		return
	}

	remainder := parts[1]

	// Extract password if any
	parts = strings.SplitN(remainder, "@", 2)
	if len(parts) == 2 {
		password = parts[0]
		remainder = parts[1]
	} else {
		remainder = parts[0]
	}

	// Extract path
	parts = strings.SplitN(remainder, ":", 2)
	path = parts[0]
	if path == "" {
		err = fmt.Errorf("Redis socket connection string should be in format redis+socket://password@/path/to/file.sock:/db, instead got %s", url)
		return
	}
	if len(parts) == 2 {
		remainder = parts[1]
	}

	// Extract DB if any
	parts = strings.SplitN(remainder, "/", 2)
	if len(parts) == 2 {
		db, _ = strconv.Atoi(parts[1])
	}

	return
}
