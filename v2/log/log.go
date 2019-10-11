package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

var logger = initialLogger()
var LogLevel = zap.NewAtomicLevelAt(zap.InfoLevel)
var LoggerConfig = DefaultConfig()

func init() {
	debug := os.Getenv("NIA_MACHINERY_DEBUG")
	if debug != "" {
		LogLevel.SetLevel(zap.DebugLevel)
	}
}

func DefaultConfig() *zap.Config {
	return &zap.Config{
		Level:             LogLevel,
		Development:       true,
		Encoding:          "json",
		EncoderConfig:     DefaultEncoderConfig(),
		OutputPaths:       []string{"stdout"},
		ErrorOutputPaths:  []string{"stderr"},
		DisableStacktrace: true,
	}
}

func DefaultEncoderConfig() zapcore.EncoderConfig {
	return zapcore.EncoderConfig{
		// Keys can be anything except the empty string.
		TimeKey:        "T",
		LevelKey:       "L",
		NameKey:        "N",
		CallerKey:      "C",
		MessageKey:     "M",
		StacktraceKey:  "S",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
}

func initialLogger() *zap.Logger {
	l, _ := LoggerConfig.Build()
	return l
}

func GetLogger() *zap.Logger {
	return logger
}
