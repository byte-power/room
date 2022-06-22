package base

import (
	"bytepower_room/base/opentelemetry"
	"context"
	"errors"
	"runtime"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const (
	otelLibraryNameRoomService = "github.com/byte-power/room"
)

const (
	otelExporterTypeJaeger = "jaeger"
	otelExporterTypeOTLP   = "otlp"
	otelExporterTypeStdout = "stdout"
)

type OtelConfig struct {
	ServiceName    string         `yaml:"service_name"`
	ServiceVersion string         `yaml:"service_version"`
	Exporter       ExporterConfig `yaml:"exporter"`
}

type ExporterConfig struct {
	Type   string `yaml:"type"`
	Jaeger struct {
		Endpoint string `yaml:"endpoint"`
	} `yaml:"jaeger"`
}

func (o OtelConfig) check() error {
	if o.ServiceName == "" {
		return errors.New("service_name should not be empty")
	}

	switch o.Exporter.Type {
	case otelExporterTypeJaeger:
		if o.Exporter.Jaeger.Endpoint == "" {
			return errors.New("jaeger endpoint should not be empty")
		}
	default:
		return errors.New("exporter type not supported")
	}

	return nil
}

func NewOtelClientWithConfig(ctx context.Context, config OtelConfig) (*opentelemetry.Otel, error) {
	if err := config.check(); err != nil {
		return nil, err
	}
	var exp sdktrace.SpanExporter
	var err error
	switch config.Exporter.Type {
	case otelExporterTypeJaeger:
		exp, err = opentelemetry.MakeJaegerExporter(ctx, config.Exporter.Jaeger.Endpoint)
	default:
		return nil, errors.New("exporter type not supported")
	}
	if err != nil {
		return nil, err
	}
	return opentelemetry.NewOtelClient(ctx, config.ServiceName, config.ServiceVersion, &exp)
}

func GetTracer() trace.Tracer {
	return otel.Tracer(otelLibraryNameRoomService)
}

// MakeCodeAttributes return a slice of attributes contain code info for the caller
func MakeCodeAttributes() []attribute.KeyValue {
	pc, file, line, _ := runtime.Caller(1)
	fn := runtime.FuncForPC(pc).Name()
	if ind := strings.LastIndexByte(fn, '/'); ind != -1 {
		fn = fn[ind+1:]
	}
	return []attribute.KeyValue{
		attribute.String("code.function", fn),
		attribute.String("code.filepath", file),
		attribute.Int("code.lineno", line),
	}
}
