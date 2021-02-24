package base

import (
	"bytepower_room/base/log"
	"bytepower_room/utility"
	"errors"
	"fmt"
	"os"
	"strings"
)

func parseLogger(appName, name string, cfg utility.StrMap) (*log.Logger, error) {
	var outputs []log.Output
	for k, v := range cfg {
		vs := utility.AnyToAnyMap(v)
		if vs == nil && v != nil {
			return nil, fmt.Errorf("'log.%v' should be map", k)
		}
		format := parseFormat(vs)
		level := parseLevel(vs["level"])
		var output log.Output
		switch k {
		case "console":
			output = parseConsoleLogger(name, format, level, vs)
		case "file":
			output = parseFileLogger(name, format, level, vs)
		case "tcp":
			output, _ = parseTCPLogger(fmt.Sprintf("%s.%s", appName, name), format, level, vs)
		}
		if output == nil {
			return nil, fmt.Errorf("'log.%v' unknown output type", k)
		}
		outputs = append(outputs, output)
	}
	return log.NewLogger(outputs...), nil
}

func parseConsoleLogger(name string, fmt log.LocalFormat, level log.Level, cfg utility.AnyMap) log.Output {
	stream := log.MakeConsoleStream(utility.AnyToString(cfg["stream"]))
	return log.MakeConsoleOutput(name, fmt, level, stream)
}

func parseFileLogger(name string, fmt log.LocalFormat, level log.Level, cfg utility.AnyMap) log.Output {
	location := utility.AnyToString(cfg["location"])
	if strings.Contains(location, "{pid}") {
		location = strings.Replace(location, "{pid}", utility.AnyToString(os.Getpid()), 1)
	}
	rotation := parseFileRotation(utility.AnyToAnyMap(cfg["rotation"]))
	return log.MakeFileOutput(name, fmt, level, location, rotation)
}

func parseTCPLogger(name string, fmt log.LocalFormat, level log.Level, cfg utility.AnyMap) (log.Output, error) {
	if name == "" {
		return nil, errors.New("TCPWriter name must be sepecified in config.")
	}
	config := utility.DefaultTCPWriterConfig()
	if it, ok := cfg["dsn"].(string); ok {
		if it == "" {
			return nil, errors.New("TCPWriter dsn must be specified in config.")
		}
		config.DSN = it
	}
	config.Async = utility.AnyToBool(cfg["async"])
	if it := int(utility.AnyToInt64(cfg["buffer_limit"])); it > 0 {
		config.BufferLimit = it
	}
	return log.MakeTCPOutput(name, fmt, level, config), nil
}

func parseFormat(cfg utility.AnyMap) log.LocalFormat {
	msgFMT := parseMessageFormat(cfg["format"])
	fmt := log.MakeLocalFormat(msgFMT)
	if keys := utility.AnyToAnyMap(cfg["keys"]); keys != nil {
		fmt.CallerKey = utility.AnyToString(keys["caller"])
		fmt.TimeKey = utility.AnyToString(keys["time"])
		fmt.MessageKey = utility.AnyToString(keys["message"])
		fmt.LevelKey = utility.AnyToString(keys["level"])
		fmt.NameKey = utility.AnyToString(keys["name"])
	}
	if timeFMT, ok := cfg["time_format"].(string); ok {
		fmt.TimeFormat = log.MakeTimeFormat(timeFMT)
	}
	return fmt
}

func parseMessageFormat(v interface{}) log.MessageFormat {
	name, _ := v.(string)
	return log.MakeMessageFormat(name)
}

func parseLevel(v interface{}) log.Level {
	name, _ := v.(string)
	return log.MakeLevelWithName(name)
}

func parseFileRotation(cfg utility.AnyMap) log.FileRotation {
	return log.FileRotation{
		MaxSize:      int(utility.AnyToInt64(cfg["max_size"])),
		Compress:     utility.AnyToBool(cfg["compress"]),
		MaxAge:       int(utility.AnyToInt64(cfg["max_age"])),
		MaxBackups:   int(utility.AnyToInt64(cfg["max_backups"])),
		LocalTime:    utility.AnyToBool(cfg["localtime"]),
		RotateOnTime: utility.AnyToBool(cfg["rotate_on_time"]),
		RotatePeriod: utility.AnyToString(cfg["rotate_period"]),
		RotateAfter:  int(utility.AnyToInt64(cfg["rotate_after"])),
	}
}
