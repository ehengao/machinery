package config

import (
	"time"

	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
)

// NewFromEnvironment creates a config object from environment variables
func NewFromEnvironment(keepReloading bool) (*Config, error) {
	cnf, err := fromEnvironment()
	if err != nil {
		return nil, err
	}

	defer log.Sync()
	log.Info("Successfully loaded config from the environment")

	if keepReloading {
		// Open a goroutine to watch remote changes forever
		go func() {
			for {
				// Delay after each request
				time.Sleep(reloadDelay)

				// Attempt to reload the config
				newCnf, newErr := fromEnvironment()
				defer log.Sync()
				if newErr != nil {
					log.Warn("Failed to reload config from the environment", zap.Error(newErr))
					continue
				}

				*cnf = *newCnf
			}
		}()
	}

	return cnf, nil
}

func fromEnvironment() (*Config, error) {
	loadedCnf, cnf := new(Config), new(Config)
	*cnf = *defaultCnf

	if err := envconfig.Process("", cnf); err != nil {
		return nil, err
	}
	if err := envconfig.Process("", loadedCnf); err != nil {
		return nil, err
	}

	if loadedCnf.AMQP == nil {
		cnf.AMQP = nil
	}

	return cnf, nil
}
