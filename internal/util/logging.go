package util

import (
	"fmt"
	"log"
)

const (
	colorReset  = "\x1b[0m"
	colorRed    = "\x1b[31m"
	colorGreen  = "\x1b[32m"
	colorYellow = "\x1b[33m"
	colorBlue   = "\x1b[34m"
)

// Infof logs an info message.
func Infof(format string, args ...any) {
	log.Printf("%s %s", colorize(colorGreen, "INFO"), fmt.Sprintf(format, args...))
}

// Warnf logs a warning message.
func Warnf(format string, args ...any) {
	log.Printf("%s %s", colorize(colorYellow, "WARN"), fmt.Sprintf(format, args...))
}

// Errorf logs an error message.
func Errorf(format string, args ...any) {
	log.Printf("%s %s", colorize(colorRed, "ERROR"), fmt.Sprintf(format, args...))
}

// Highlightf logs a highlighted message.
func Highlightf(format string, args ...any) {
	log.Printf("%s %s", colorize(colorBlue, "NOTE"), fmt.Sprintf(format, args...))
}

func colorize(color, msg string) string {
	return color + msg + colorReset
}
