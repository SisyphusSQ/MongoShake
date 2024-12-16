package log

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/natefinch/lumberjack"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/alibaba/MongoShake/v2/lib/timeutil"
)

var Logger *ZapLogger

var logLevelMap = map[string]zapcore.Level{
	"debug":  zapcore.DebugLevel,
	"info":   zapcore.InfoLevel,
	"warn":   zapcore.WarnLevel,
	"error":  zapcore.ErrorLevel,
	"dpanic": zapcore.DPanicLevel,
	"panic":  zapcore.PanicLevel,
	"fatal":  zapcore.FatalLevel,
}

// New zap logger
// verbose: where log goes to: 0 - file，1 - file+stdout，2 - stdout
func New(logLevel, logDir, logFile string, maxSize, maxBackup, maxAge, verbose int) error {
	if verbose != 0 {
		panic("with zap log, only use verbose zero")
	}

	level := preCheck(logLevel)

	logDir, err := filepath.Abs(logDir)
	if err != nil {
		panic(err)
	}

	if _, err = os.Stat(logDir); err != nil && os.IsNotExist(err) {
		if err = os.MkdirAll(logDir, os.ModeDir|os.ModePerm); err != nil {
			return fmt.Errorf("create log.dir[%v] failed[%v]", logDir, err)
		}
	}

	var fn string
	if strings.HasSuffix(logDir, "/") {
		fn = logDir + logFile
	} else {
		fn = logDir + "/" + logFile
	}
	if err = initRotate(fn); err != nil {
		panic(err)
	}

	lumberJackLogger := &lumberjack.Logger{
		Filename:   fn,
		MaxSize:    maxSize,
		MaxBackups: maxBackup,
		MaxAge:     maxAge,
		Compress:   true,
	}

	writeSyncer := zapcore.AddSync(lumberJackLogger)
	timeEncoder := zapcore.TimeEncoderOfLayout(timeutil.CSTLayout)
	cfg := zapcore.EncoderConfig{
		TimeKey:        "ts",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		FunctionKey:    zapcore.OmitKey,
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    customLevelEncoder,
		EncodeTime:     timeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   customCallerEncoder,
	}
	encoder := zapcore.NewConsoleEncoder(cfg)
	core := zapcore.NewCore(encoder, writeSyncer, level)
	logger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1)).Sugar()
	Logger = NewZapLogger(logger)

	return nil
}

func preCheck(level string) zapcore.Level {
	l, ok := logLevelMap[level]
	if !ok {
		panic(fmt.Sprintf("invalid log-level %s, should be [-1,5]", level))
	}

	return l
}

func customLevelEncoder(level zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	levelString := "[" + level.CapitalString() + "]"
	enc.AppendString(levelString)
}

func customCallerEncoder(caller zapcore.EntryCaller, enc zapcore.PrimitiveArrayEncoder) {
	if caller.Defined {
		enc.AppendString("[" + caller.TrimmedPath() + "]")
	} else {
		enc.AppendString("[undefined]")
	}
}

func initRotate(fn string) error {
	f, err := os.Lstat(fn)
	if f != nil {
		var (
			fname string
			num   = 100
		)

		for ; num >= 1; num-- {
			fname = fn + fmt.Sprintf(".%d", num)
			nfname := fn + fmt.Sprintf(".%d", num+1)
			_, err = os.Lstat(fname)
			if err == nil {
				_ = os.Rename(fname, nfname)
			}
		}

		// Rename the file to its newfound home
		err = os.Rename(fn, fname)
		if err != nil {
			return fmt.Errorf("Rotate: %s\n", err)
		}
	}

	if err != nil && strings.Contains(err.Error(), "no such file or directory") {
		return nil
	}
	return err
}

type ZapLogger struct {
	logger *zap.SugaredLogger
}

func NewZapLogger(logger *zap.SugaredLogger) *ZapLogger {
	return &ZapLogger{logger: logger}
}

// Printf formats according to a format specifier and writes to the logger.
func (l *ZapLogger) Printf(format string, v ...interface{}) {
	l.logger.Infof(format, v...)
}

// Print calls Printf with the default message format.
func (l *ZapLogger) Print(v ...interface{}) {
	l.logger.Info(v...)
}

// Println calls Print with a newline.
func (l *ZapLogger) Println(v ...interface{}) {
	l.logger.Info(v...)
}

// Fatal calls Print followed by a call to os.Exit(1).
func (l *ZapLogger) Fatal(v ...interface{}) {
	l.logger.Fatal(v...)
}

// Fatalf is equivalent to Printf followed by a call to os.Exit(1).
func (l *ZapLogger) Fatalf(format string, v ...interface{}) {
	l.logger.Fatalf(format, v...)
}

// Fatalln is equivalent to Fatal.
func (l *ZapLogger) Fatalln(v ...interface{}) {
	l.logger.Fatal(v...)
}

// Panic is equivalent to Print followed by a call to panic().
func (l *ZapLogger) Panic(v ...interface{}) {
	l.logger.Panic(v...)
}

// Panicf is equivalent to Printf followed by a call to panic().
func (l *ZapLogger) Panicf(format string, v ...interface{}) {
	l.logger.Panicf(format, v...)
}

func (l *ZapLogger) Debugf(format string, args ...interface{}) {
	l.logger.Debugf(format, args...)
}

func (l *ZapLogger) Infof(format string, args ...interface{}) {
	l.logger.Infof(format, args...)
}

func (l *ZapLogger) Warnf(format string, args ...interface{}) {
	l.logger.Warnf(format, args...)
}

func (l *ZapLogger) Errorf(format string, args ...interface{}) {
	l.logger.Errorf(format, args...)
}

func (l *ZapLogger) Debug(format string, args ...interface{}) {
	l.logger.Debugf(format, args...)
}

func (l *ZapLogger) Info(format string, args ...interface{}) {
	l.logger.Infof(format, args...)
}

func (l *ZapLogger) Warn(format string, args ...interface{}) {
	l.logger.Warnf(format, args...)
}

func (l *ZapLogger) Error(format string, args ...interface{}) {
	l.logger.Errorf(format, args...)
}

func (l *ZapLogger) Sync() {
	_ = l.logger.Sync()
}
