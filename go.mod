module bytepower_room

go 1.14

require (
	github.com/byte-power/gorich v1.0.5
	github.com/go-pg/pg/v10 v10.7.3
	github.com/go-redis/redis/v8 v8.4.3
	github.com/gogf/greuse v1.1.0
	github.com/json-iterator/go v1.1.10
	github.com/pkg/errors v0.9.1 // indirect
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.6.1
	github.com/tidwall/redcon v1.4.0
	go.uber.org/ratelimit v0.2.0
	go.uber.org/zap v1.16.0
	golang.org/x/sys v0.0.0-20210119212857-b64e53b001e4 // indirect
	golang.org/x/tools v0.0.0-20201022035929-9cf592e881e9 // indirect
	gopkg.in/alexcesaro/statsd.v2 v2.0.0
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v2 v2.3.0
)

replace github.com/go-redis/redis/v8 v8.4.3 => github.com/byte-power/redis/v8 v8.4.3

replace github.com/vmihailenco/bufpool v0.1.11 => github.com/byte-power/bufpool v0.1.13
