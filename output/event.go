package output

import (
	"fmt"
	"strings"
	"time"
)

type OutputEvent struct {
	Source      string
	EventTime   time.Time
	EventType   string
	Name        string
	Level       Level
	EventId     string
	Entity      string
	SourceIP    string
	Body        map[string]interface{}
	Occurrences int
}

type Level int

func (l Level) String() string {
	switch l {
	case ErrorLevel:
		return "error"
	case WarnLevel:
		return "warn"
	case InfoLevel:
		return "info"
	}

	return "unknown"
}

func ParseLevel(lvl string) (Level, error) {
	switch strings.ToLower(lvl) {
	case "error":
		return ErrorLevel, nil
	case "warn", "warning":
		return WarnLevel, nil
	case "info":
		return InfoLevel, nil
	}

	var l Level
	return l, fmt.Errorf("not a valid Level: %q", lvl)
}

const (
	ErrorLevel Level = iota
	WarnLevel
	InfoLevel
)
