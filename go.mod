module bytepower_room

go 1.14

require (
	github.com/byte-power/gorich v1.0.5
	github.com/go-pg/pg/extra/pgotel/v10 v10.10.6
	github.com/go-pg/pg/v10 v10.10.6
	github.com/go-redis/redis/extra/redisotel/v8 v8.11.5
	github.com/go-redis/redis/v8 v8.11.5
	github.com/gogf/greuse v1.1.0
	github.com/json-iterator/go v1.1.10
	github.com/patrickmn/go-cache v2.1.0+incompatible
	github.com/pkg/errors v0.9.1 // indirect
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.7.1
	github.com/tidwall/redcon v1.4.4
	go.opentelemetry.io/otel v1.7.0
	go.opentelemetry.io/otel/exporters/jaeger v1.7.0
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.7.0
	go.opentelemetry.io/otel/sdk v1.7.0
	go.opentelemetry.io/otel/trace v1.7.0
	go.uber.org/ratelimit v0.2.0
	go.uber.org/zap v1.16.0
	gopkg.in/alexcesaro/statsd.v2 v2.0.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v2 v2.4.0
)

replace github.com/go-redis/redis/v8 v8.11.5 => ../redis

replace github.com/go-redis/redis/extra/redisotel/v8 v8.11.5 => ../redis/extra/redisotel

replace github.com/vmihailenco/bufpool v0.1.11 => github.com/byte-power/bufpool v0.1.13

replace github.com/tidwall/redcon v1.4.4 => github.com/byte-power/redcon v1.4.4
