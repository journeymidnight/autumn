package xlog

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	ZapLogger *zap.Logger
	Logger    *zap.SugaredLogger
)

func InitLog(outputPath []string, level zapcore.Level) {

	if Logger != nil {
		panic("InitLog called somewhere")
	}
	var err error
	cfg := zap.NewDevelopmentConfig()
	//cfg.OutputPaths = []string{fileName, os.Stdout.Name()}
	cfg.OutputPaths = outputPath

	cfg.Level.SetLevel(level)
	ZapLogger, err = cfg.Build()
	if err != nil {
		panic(err.Error())
	}
	Logger = ZapLogger.Sugar()
}
