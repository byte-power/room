package log

import (
	"io"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

const callerSkip = 3

var _ Output = (*zapOutput)(nil)

type zapOutput struct {
	level  Level
	output *zap.SugaredLogger
}

func (o *zapOutput) Level() Level {
	return o.level
}

func (o *zapOutput) Log(l Level, msg string, argPairs []interface{}) {
	switch l {
	case LevelDebug:
		o.output.Debugw(msg, argPairs...)
	case LevelInfo:
		o.output.Infow(msg, argPairs...)
	case LevelWarn:
		o.output.Warnw(msg, argPairs...)
	case LevelError:
		o.output.Errorw(msg, argPairs...)
	case LevelFatal:
		o.output.Fatalw(msg, argPairs...)
	}
}

func (o *zapOutput) LogModuleAndPairs(l Level, subject string, pairs []LogPair) {
	argPairs := make([]interface{}, 0, len(pairs)*2)
	for i := range pairs {
		argPairs = append(argPairs, pairs[i].key, pairs[i].value)
	}
	switch l {
	case LevelDebug:
		o.output.Debugw(subject, argPairs...)
	case LevelInfo:
		o.output.Infow(subject, argPairs...)
	case LevelWarn:
		o.output.Warnw(subject, argPairs...)
	case LevelError:
		o.output.Errorw(subject, argPairs...)
	case LevelFatal:
		o.output.Fatalw(subject, argPairs...)
	}
}

func (o *zapOutput) LogPlainMessage(l Level, args []interface{}) {
	switch l {
	case LevelDebug:
		o.output.Debug(args...)
	case LevelInfo:
		o.output.Info(args...)
	case LevelWarn:
		o.output.Warn(args...)
	case LevelError:
		o.output.Error(args...)
	case LevelFatal:
		o.output.Fatal(args...)
	}
}

func (o *zapOutput) LogFormatted(l Level, format string, args []interface{}) {
	switch l {
	case LevelDebug:
		o.output.Debugf(format, args...)
	case LevelInfo:
		o.output.Infof(format, args...)
	case LevelWarn:
		o.output.Warnf(format, args...)
	case LevelError:
		o.output.Errorf(format, args...)
	case LevelFatal:
		o.output.Fatalf(format, args...)
	}
}

func newZapConsoleWriter(stream zapcore.WriteSyncer) zapcore.WriteSyncer {
	return zapcore.Lock(stream)
}

func newZapFileWriter(location string, rotation FileRotation) zapcore.WriteSyncer {
	fileLogger := lumberjack.Logger{
		Filename:   location,
		MaxSize:    rotation.MaxSize,
		Compress:   rotation.Compress,
		MaxAge:     rotation.MaxAge,
		MaxBackups: rotation.MaxBackups,
	}
	if rotation.RotateOnTime {
		go func() {
			interval := parseRotationPeriod(rotation.RotatePeriod, rotation.RotateAfter)
			for {
				<-time.After(interval)
				fileLogger.Rotate()
			}
		}()
	}
	return zapcore.AddSync(&fileLogger)
}

func newZapWriter(w io.Writer) zapcore.WriteSyncer {
	return zapcore.AddSync(w)
}

func newZapLogger(name string, fmt LocalFormat, level Level, writer zapcore.WriteSyncer) *zapOutput {
	encoder := makeZapEncoder(fmt.Format.isJSON(), makeZapEncoderConfig(fmt))
	core := zapcore.NewCore(encoder, writer, makeZapLevel(level))
	output := zap.New(core,
		zap.AddCallerSkip(callerSkip),
		zap.AddCaller(),
	).Sugar()
	if name != "" {
		output = output.Named(name)
	}
	return &zapOutput{level: level, output: output}
}

func makeZapEncoderConfig(f LocalFormat) zapcore.EncoderConfig {
	cfg := zap.NewProductionEncoderConfig()
	cfg.CallerKey = f.CallerKey
	cfg.LevelKey = f.LevelKey
	cfg.MessageKey = f.MessageKey
	cfg.NameKey = f.NameKey
	cfg.TimeKey = f.TimeKey
	if f.TimeFormat == "" {
		cfg.EncodeTime = zapcore.RFC3339TimeEncoder
	} else {
		cfg.EncodeTime.UnmarshalText([]byte(f.TimeFormat))
	}
	return cfg
}

func makeZapEncoder(isJSON bool, encoderConfig zapcore.EncoderConfig) zapcore.Encoder {
	if isJSON {
		return zapcore.NewJSONEncoder(encoderConfig)
	}
	return zapcore.NewConsoleEncoder(encoderConfig)
}

func makeZapLevel(l Level) zapcore.Level {
	switch l {
	case LevelInfo:
		return zapcore.InfoLevel
	case LevelWarn:
		return zapcore.WarnLevel
	case LevelDebug:
		return zapcore.DebugLevel
	case LevelError:
		return zapcore.ErrorLevel
	case LevelFatal:
		return zapcore.FatalLevel
	default:
		return zapcore.InfoLevel
	}
}

func parseRotationPeriod(period string, n int) time.Duration {
	switch strings.ToLower(period) {
	case "day", "daily", "d":
		return time.Hour * 24 * time.Duration(n)
	case "hour", "hourly", "h":
		return time.Hour * time.Duration(n)
	case "minute", "m":
		return time.Minute * time.Duration(n)
	case "second", "s":
		return time.Second * time.Duration(n)
	default:
		return time.Hour * 24
	}
}
