package logger

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Level uint8

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

func (level Level) String() string {
	switch level {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	case LevelFatal:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

type Field struct {
	Key   string
	Value any
}

type Entry struct {
	Timestamp time.Time      `json:"ts"`
	Level     Level          `json:"-"`
	Component string         `json:"component"`
	Event     string         `json:"event"`
	Fields    map[string]any `json:"fields,omitempty"`
}

var (
	mu sync.Mutex

	level = LevelInfo

	component string
	instance  string

	file *os.File

	initialized bool
)

func Init(name string, logLevel Level, logDirectory string) error {
	mu.Lock()
	defer mu.Unlock()

	if initialized {
		return nil
	}

	logDirectory = filepath.Join("logs", logDirectory)
	if err := os.MkdirAll(logDirectory, 0755); err != nil {
		return err
	}

	component = name
	level = logLevel
	instance = time.Now().Format("20060102-150405.000000000")

	logFile, err := os.OpenFile(
		filepath.Join(logDirectory, fmt.Sprintf("%s-%s.jsonl", component, instance)),
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return err
	}

	file = logFile
	initialized = true

	return nil
}

func Close() error {
	mu.Lock()
	defer mu.Unlock()

	if !initialized {
		return nil
	}

	err := file.Close()

	file = nil
	component = ""
	initialized = false

	return err
}

func Instance() string {
	mu.Lock()
	defer mu.Unlock()

	return instance
}

func Debug(event string, fields ...Field) {
	logEntry(LevelDebug, event, fields...)
}

func Info(event string, fields ...Field) {
	logEntry(LevelInfo, event, fields...)
}

func Warn(event string, fields ...Field) {
	logEntry(LevelWarn, event, fields...)
}

func Error(event string, fields ...Field) {
	logEntry(LevelError, event, fields...)
}

func Fatal(event string, fields ...Field) {
	logEntry(LevelFatal, event, fields...)
}

func logEntry(logLevel Level, event string, fields ...Field) {
	mu.Lock()
	defer mu.Unlock()

	if !initialized || !enabled(logLevel) {
		return
	}

	entry := &Entry{
		Timestamp: time.Now().UTC(),
		Level:     logLevel,
		Component: component,
		Event:     event,
	}

	if len(fields) > 0 {
		entry.Fields = make(map[string]any, len(fields))
		for _, field := range fields {
			if _, exists := entry.Fields[field.Key]; exists {
				panic("duplicate logger field: " + field.Key)
			}
			entry.Fields[field.Key] = field.Value
		}
	}

	write(entry)

	if logLevel == LevelFatal {
		_ = file.Close()
		os.Exit(1)
	}
}

func enabled(logLevel Level) bool {
	return logLevel >= level
}

func write(entry *Entry) {
	if !initialized {
		return
	}

	writeTerminal(entry)
	writeFile(entry)
}

func writeTerminal(entry *Entry) {
	var builder strings.Builder
	builder.Grow(32 + len(entry.Event) + len(entry.Fields)*20)

	level := entry.Level.String()

	builder.WriteByte('[')
	builder.WriteString(level)

	if padding := 5 - len(level); padding > 0 {
		builder.WriteString(strings.Repeat(" ", padding))
	}

	builder.WriteString("] ")
	builder.WriteString(entry.Event)

	for key, value := range entry.Fields {
		builder.WriteByte(' ')
		builder.WriteString(key)
		builder.WriteByte('=')
		fmt.Fprint(&builder, value)
	}

	builder.WriteByte('\n')

	if _, err := os.Stdout.WriteString(builder.String()); err != nil {
		// ignore logging errors
	}
}

func writeFile(entry *Entry) {
	record := struct {
		Timestamp time.Time      `json:"ts"`
		Level     string         `json:"level"`
		Component string         `json:"component"`
		Event     string         `json:"event"`
		Fields    map[string]any `json:"fields,omitempty"`
	}{
		Timestamp: entry.Timestamp,
		Level:     entry.Level.String(),
		Component: entry.Component,
		Event:     entry.Event,
		Fields:    entry.Fields,
	}

	data, err := json.Marshal(record)
	if err != nil {
		return
	}

	data = append(data, '\n')

	if _, err := file.Write(data); err != nil {
		// ignore logging errors
	}
}

func Str(key, value string) Field {
	return Field{
		Key:   key,
		Value: value,
	}
}

func Int(key string, value int) Field {
	return Field{
		Key:   key,
		Value: value,
	}
}

func Uint32(key string, value uint32) Field {
	return Field{
		Key:   key,
		Value: value,
	}
}

func Uint64(key string, value uint64) Field {
	return Field{
		Key:   key,
		Value: value,
	}
}

func Bool(key string, value bool) Field {
	return Field{
		Key:   key,
		Value: value,
	}
}

func Err(err error) Field {
	return Field{
		Key:   "error",
		Value: err,
	}
}
