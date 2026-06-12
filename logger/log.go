package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var log *zap.Logger

func Init() {
	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{
		"stdout",
		"./logger/runtime.log",
	}

	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	l, err := cfg.Build()
	if err != nil {
		panic(err)
	}
	log = l
}

func Info(msg string, keysAndValues ...interface{}) {
	log.Sugar().Infow(msg, keysAndValues...)
}

func Sync() {
	log.Sync()
}
