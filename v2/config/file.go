package config

import (
	"fmt"
	"os"
	"time"

	_log "github.com/ehengao/machinery/v2/log"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

var log = _log.GetLogger()

// NewFromYaml creates a config object from YAML file
func NewFromYaml(cnfPath string, keepReloading bool) (*Config, error) {
	cnf, err := fromFile(cnfPath)
	if err != nil {
		return nil, err
	}
	defer log.Sync()
	log.Info("Successfully loaded config from file", zap.String("cnfPath", cnfPath))

	if keepReloading {
		// Open a goroutine to watch remote changes forever
		go func() {
			for {
				// Delay after each request
				time.Sleep(reloadDelay)

				// Attempt to reload the config
				newCnf, newErr := fromFile(cnfPath)
				defer log.Sync()
				if newErr != nil {
					log.Warn("Failed to reload config from file", zap.String("cnfPath", cnfPath), zap.Error(newErr))
					continue
				}

				*cnf = *newCnf
			}
		}()
	}

	return cnf, nil
}

// ReadFromFile reads data from a file
func ReadFromFile(cnfPath string) ([]byte, error) {
	file, err := os.Open(cnfPath)

	// Config file not found
	if err != nil {
		return nil, fmt.Errorf("Open file error: %s", err)
	}

	// Config file found, let's try to read it
	data := make([]byte, 1000)
	count, err := file.Read(data)
	if err != nil {
		return nil, fmt.Errorf("Read from file error: %s", err)
	}

	return data[:count], nil
}

func fromFile(cnfPath string) (*Config, error) {
	loadedCnf, cnf := new(Config), new(Config)
	*cnf = *defaultCnf

	data, err := ReadFromFile(cnfPath)
	if err != nil {
		return nil, err
	}

	if err := yaml.Unmarshal(data, cnf); err != nil {
		return nil, fmt.Errorf("Unmarshal YAML error: %s", err)
	}
	if err := yaml.Unmarshal(data, loadedCnf); err != nil {
		return nil, fmt.Errorf("Unmarshal YAML error: %s", err)
	}
	if loadedCnf.AMQP == nil {
		cnf.AMQP = nil
	}

	return cnf, nil
}
