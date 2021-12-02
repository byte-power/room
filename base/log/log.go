package log

import (
	"bytepower_room/utility"
)

type Logger struct {
	outpers []Output
}

type Output interface {
	Level() Level
	LogModuleAndPairs(l Level, subject string, pairs []LogPair)
}

func NewLogger(outpers ...Output) *Logger {
	return &Logger{outpers: outpers}
}

func MakeConsoleOutput(name string, fmt LocalFormat, level Level, stream ConsoleStream) Output {
	writer := newZapConsoleWriter(stream.stream())
	return newZapLogger(name, fmt, level, writer)
}

func MakeFileOutput(name string, fmt LocalFormat, level Level, location string, rotation FileRotation) Output {
	writer := newZapFileWriter(location, rotation)
	return newZapLogger(name, fmt, level, writer)
}

func MakeTCPOutput(name string, fmt LocalFormat, level Level, config utility.TCPWriterConfig) Output {
	w := utility.NewTCPWriter(config)
	writer := newZapWriter(w)
	return newZapLogger(name, fmt, level, writer)
}

func (l *Logger) Debugm(subject string, values map[string]interface{}) {
	l.logPairs(LevelDebug, subject, convertStrMapToLogPairs(values))
}
func (l *Logger) Infom(subject string, values map[string]interface{}) {
	l.logPairs(LevelInfo, subject, convertStrMapToLogPairs(values))
}
func (l *Logger) Warnm(subject string, values map[string]interface{}) {
	l.logPairs(LevelWarn, subject, convertStrMapToLogPairs(values))
}
func (l *Logger) Errorm(subject string, values map[string]interface{}) {
	l.logPairs(LevelError, subject, convertStrMapToLogPairs(values))
}

func (l *Logger) Logm(level Level, subject string, values map[string]interface{}) {
	l.logPairs(level, subject, convertStrMapToLogPairs(values))
}

func (l *Logger) logPairs(level Level, subject string, pairs []LogPair) {
	for _, it := range l.outpers {
		if level >= it.Level() {
			it.LogModuleAndPairs(level, subject, pairs)
		}
	}
}
func (l *Logger) Debug(subject string, pairs ...LogPair) {
	l.logPairs(LevelDebug, subject, pairs)
}
func (l *Logger) Info(subject string, pairs ...LogPair) {
	l.logPairs(LevelInfo, subject, pairs)
}
func (l *Logger) Warn(subject string, pairs ...LogPair) {
	l.logPairs(LevelWarn, subject, pairs)
}
func (l *Logger) Error(subject string, pairs ...LogPair) {
	l.logPairs(LevelError, subject, pairs)
}

func (l *Logger) Log(level Level, subject string, pairs ...LogPair) {
	l.logPairs(level, subject, pairs)
}

type LogPair struct {
	key   string
	value interface{}
}

func String(k, v string) LogPair {
	return LogPair{key: k, value: v}
}

func Int(k string, v int) LogPair {
	return LogPair{key: k, value: v}
}

func Int32(k string, v int32) LogPair {
	return LogPair{key: k, value: v}
}

func Int64(k string, v int64) LogPair {
	return LogPair{key: k, value: v}
}

func Error(err error) LogPair {
	logPair := LogPair{key: "error"}
	if err == nil {
		logPair.value = "error_is_nil"
	} else {
		logPair.value = err.Error()
	}
	return logPair
}

func Stack(stack []byte) LogPair {
	return LogPair{key: "stack", value: utility.BytesToString(stack)}
}

func convertStrMapToLogPairs(values map[string]interface{}) []LogPair {
	pairs := make([]LogPair, 0, len(values))
	for key, value := range values {
		pairs = append(pairs, LogPair{key: key, value: value})
	}
	return pairs
}
