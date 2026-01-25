package util

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
)

const (
	colorReset  = "\x1b[0m"
	colorRed    = "\x1b[31m"
	colorGreen  = "\x1b[32m"
	colorYellow = "\x1b[33m"
	colorBlue   = "\x1b[34m"
)

var (
	basicLogger  = log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)
	detailLogger = basicLogger
	detailCloser io.Closer
)

// InitLogging configures the detail logger to write to a file.
func InitLogging(logFile string) error {
	if detailCloser != nil {
		_ = detailCloser.Close()
		detailCloser = nil
	}
	if logFile == "" {
		detailLogger = basicLogger
		return nil
	}
	dir := filepath.Dir(logFile)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	detailLogger = log.New(f, "", log.LstdFlags|log.Lmicroseconds)
	detailCloser = f
	return nil
}

// CloseLogging closes the detail log file if configured.
func CloseLogging() {
	if detailCloser != nil {
		_ = detailCloser.Close()
		detailCloser = nil
	}
	detailLogger = basicLogger
}

// Infof logs an info message.
func Infof(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	basicLogger.Printf("%s %s", colorize(colorGreen, "INFO"), msg)
	if detailLogger != nil && detailLogger != basicLogger {
		detailLogger.Printf("INFO %s", msg)
	}
}

// Warnf logs a warning message.
func Warnf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	basicLogger.Printf("%s %s", colorize(colorYellow, "WARN"), msg)
	if detailLogger != nil && detailLogger != basicLogger {
		detailLogger.Printf("WARN %s", msg)
	}
}

// Errorf logs an error message.
func Errorf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	basicLogger.Printf("%s %s", colorize(colorRed, "ERROR"), msg)
	if detailLogger != nil && detailLogger != basicLogger {
		detailLogger.Printf("ERROR %s", msg)
	}
}

// Highlightf logs a highlighted message.
func Highlightf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	basicLogger.Printf("%s %s", colorize(colorBlue, "NOTE"), msg)
	if detailLogger != nil && detailLogger != basicLogger {
		detailLogger.Printf("NOTE %s", msg)
	}
}

// Detailf logs a message to the detail log only.
func Detailf(format string, args ...any) {
	if detailLogger == nil || detailLogger == basicLogger {
		return
	}
	detailLogger.Printf("INFO %s", fmt.Sprintf(format, args...))
}

func colorize(color, msg string) string {
	return color + msg + colorReset
}
