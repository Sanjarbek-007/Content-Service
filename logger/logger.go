package logger

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
)

func NewLogger() (*zap.Logger, error) {
	logFile, err := os.Create("app.log")
	if err != nil {
		return nil, err
	}

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	encoder := zapcore.NewJSONEncoder(encoderConfig)
	fileWriteSyncer := zapcore.AddSync(logFile)
	consoleWriteSyncer := zapcore.AddSync(os.Stdout)
	multiWriteSyncer := zapcore.NewMultiWriteSyncer(fileWriteSyncer, consoleWriteSyncer)
	core := zapcore.NewCore(encoder, multiWriteSyncer, zapcore.DebugLevel)

	logger := zap.New(core)
	return logger, nil
}