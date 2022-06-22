package opentelemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

const LibName = "opentelemetry"

type Otel struct {
	serviceName    string
	serviceVersion string

	tracerProvider *sdktrace.TracerProvider
	exporter       *sdktrace.SpanExporter
	resource       *resource.Resource
}

func NewOtelClient(ctx context.Context, serviceName, serviceVersion string, exporter *sdktrace.SpanExporter) (*Otel, error) {
	var err error
	o := Otel{serviceName: serviceName, serviceVersion: serviceVersion, exporter: exporter}
	o.tracerProvider = o.makeTraceProvider(ctx, exporter)
	otel.SetTracerProvider(o.tracerProvider)
	return &o, err
}

func (o *Otel) Shutdown(ctx context.Context) error {
	return o.tracerProvider.Shutdown(ctx)
}

func (o *Otel) makeTraceProvider(ctx context.Context, exp *sdktrace.SpanExporter) *sdktrace.TracerProvider {
	r, _ := resource.New(
		ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(o.serviceName),
			semconv.ServiceVersionKey.String(o.serviceVersion),
		),
	)
	o.resource = r
	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(*exp),
		sdktrace.WithResource(r),
	)
}
