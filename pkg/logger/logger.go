package logger

import (
	"go.uber.org/zap"
	"sync"
)

var (
	once   sync.Once
	sugar  *zap.SugaredLogger
)

// Init initializes the global zap logger
func Init(prod bool) {
	once.Do(func() {
		var logger *zap.Logger
		var err error

		if prod {
			logger, err = zap.NewProduction()
		} else {
			logger, err = zap.NewDevelopment()
		}
		if err != nil {
			panic(err)
		}
		sugar = logger.Sugar()
	})
}

// Get returns the global logger
func Get() *zap.SugaredLogger {
	if sugar == nil {
		Init(false) // default to dev
	}
	return sugar
}
