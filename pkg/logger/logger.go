package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

type Logger struct {
	mu        sync.Mutex
	level     LogLevel
	file      *os.File
	multi     io.Writer
	callDepth int
	flags     int
}

var (
	defaultLogger *Logger
	once          sync.Once
)

func NewLogger(level LogLevel, logFile string) (*Logger, error) {
	l := &Logger{
		level:     level,
		callDepth: 3,
		flags:     log.LstdFlags | log.Lmicroseconds,
	}

	writers := []io.Writer{os.Stdout}

	if logFile != "" {
		dir := filepath.Dir(logFile)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create log directory: %w", err)
		}

		f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file: %w", err)
		}
		l.file = f
		writers = append(writers, f)
	}

	l.multi = io.MultiWriter(writers...)
	return l, nil
}

func Default() *Logger {
	once.Do(func() {
		var err error
		defaultLogger, err = NewLogger(LevelInfo, "logs/gateway.log")
		if err != nil {
			defaultLogger, _ = NewLogger(LevelInfo, "")
		}
	})
	return defaultLogger
}

func (l *Logger) SetLevel(level LogLevel) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

func (l *Logger) log(level LogLevel, format string, args ...interface{}) {
	if level < l.level {
		return
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	levelStr := [...]string{"DEBUG", "INFO", "WARN", "ERROR", "FATAL"}[level]

	_, file, line, ok := runtime.Caller(l.callDepth)
	if !ok {
		file = "???"
		line = 0
	}
	file = filepath.Base(file)

	msg := fmt.Sprintf(format, args...)
	logMsg := fmt.Sprintf("%s [%s] %s:%d %s\n",
		now.Format("2006-01-02 15:04:05.000000"),
		levelStr,
		file,
		line,
		msg)

	l.multi.Write([]byte(logMsg))

	if level == LevelFatal {
		os.Exit(1)
	}
}

func (l *Logger) Debug(format string, args ...interface{}) {
	l.log(LevelDebug, format, args...)
}

func (l *Logger) Info(format string, args ...interface{}) {
	l.log(LevelInfo, format, args...)
}

func (l *Logger) Warn(format string, args ...interface{}) {
	l.log(LevelWarn, format, args...)
}

func (l *Logger) Error(format string, args ...interface{}) {
	l.log(LevelError, format, args...)
}

func (l *Logger) Fatal(format string, args ...interface{}) {
	l.log(LevelFatal, format, args...)
}

func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file != nil {
		return l.file.Close()
	}
	return nil
}
