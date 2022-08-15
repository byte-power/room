package base

import (
	"bytepower_room/base/log"
	"fmt"
	"time"

	redisotel "github.com/go-redis/redis/extra/redisotel/v8"
	"github.com/go-redis/redis/v8"
)

type RedisClusterConfig struct {
	Addrs      []string         `yaml:"addrs"`
	Connection connectionConfig `yaml:",inline"`
}

func (config RedisClusterConfig) check() error {
	if len(config.Addrs) == 0 {
		return fmt.Errorf("adds should not be empty")
	}
	for _, addr := range config.Addrs {
		if addr == "" {
			return fmt.Errorf("address in adds should not be empty")
		}
	}
	if err := config.Connection.check(); err != nil {
		return err
	}

	return nil
}

type connectionConfig struct {
	PoolSize int `yaml:"pool_size"`

	DialTimeoutMS     int `yaml:"dial_timeout_ms"`
	ReadTimeoutMS     int `yaml:"read_timeout_ms"`
	WriteTimeoutMS    int `yaml:"write_timeout_ms"`
	IdleTimeoutSecond int `yaml:"idle_timeout_second"`
	PoolTimeoutMS     int `yaml:"pool_timeout_ms"`

	MaxRetries                int `yaml:"max_retries"`
	MaxConnAgeSeconds         int `yaml:"max_conn_age_second"`
	MinIdleConns              int `yaml:"min_idle_conns"`
	MinRetryBackoffMS         int `yaml:"min_retry_backoff_ms"`
	MaxRetryBackoffMS         int `yaml:"max_retry_backoff_ms"`
	IdleCheckFrequencySeconds int `yaml:"idle_check_frequency_second"`
}

func (config connectionConfig) check() error {
	if v := config.PoolSize; v <= 0 {
		return fmt.Errorf("pool_size=%d, it should be > 0", v)
	}
	if v := config.DialTimeoutMS; v < 0 {
		return fmt.Errorf("dial_timeout_ms=%d, it should be >= 0", v)
	}
	if v := config.ReadTimeoutMS; v < 0 {
		return fmt.Errorf("read_timeout_ms=%d, it should be >= 0", v)
	}
	if v := config.WriteTimeoutMS; v < 0 {
		return fmt.Errorf("write_timeout_ms=%d, it should be >= 0", v)
	}
	if v := config.IdleTimeoutSecond; v < -1 {
		return fmt.Errorf("idle_timeout_second=%d, it should be >= -1", v)
	}
	if v := config.PoolTimeoutMS; v < 0 {
		return fmt.Errorf("pool_timeout_ms=%d, it should be >= 0", v)
	}
	if v := config.MaxRetries; v < 0 {
		return fmt.Errorf("max_retries=%d, it should be >= 0", v)
	}
	if v := config.MaxConnAgeSeconds; v < 0 {
		return fmt.Errorf("max_conn_age_second=%d, it should be >= 0", v)
	}
	if v := config.MinIdleConns; v < 0 {
		return fmt.Errorf("min_idle_conns=%d, it should be >= 0", v)
	}
	if v := config.MinRetryBackoffMS; v < -1 {
		return fmt.Errorf("min_retry_backoff_ms=%d, it should be >= -1", v)
	}
	if v := config.MaxRetryBackoffMS; v < -1 {
		return fmt.Errorf("max_retry_backoff_ms=%d, it should be >= -1", v)
	}
	if config.MinRetryBackoffMS > config.MaxRetryBackoffMS {
		return fmt.Errorf(
			"min_retry_backoff_ms=%d, max_retry_backoff_ms=%d, min_retry_backoff_ms shoule be less than or equal to max_retry_backoff_ms",
			config.MinRetryBackoffMS, config.MaxRetryBackoffMS)
	}
	if v := config.IdleCheckFrequencySeconds; v < -1 {
		return fmt.Errorf("idle_check_frequency_seconds=%d, it should be >= -1", v)
	}
	return nil
}

func NewRedisClusterFromConfig(config RedisClusterConfig, logger *log.Logger, metric *MetricClient) (*redis.ClusterClient, error) {
	opt, err := initRedisOption(config)
	if err != nil {
		return nil, err
	}
	opt.NewClient = func(opt *redis.Options) *redis.Client {
		node := redis.NewClient(opt)
		node.AddHook(redisotel.NewTracingHook())
		return node
	}
	client := redis.NewClusterClient(opt)
	redisHook := newRedisRecordHook(metric, logger)
	client.AddHook(redisHook)
	client.AddHook(redisotel.NewTracingHook())
	return client, nil
}

func initRedisOption(config RedisClusterConfig) (*redis.ClusterOptions, error) {
	if err := config.check(); err != nil {
		return nil, err
	}
	opt := &redis.ClusterOptions{
		Addrs:        config.Addrs,
		ReadTimeout:  time.Duration(config.Connection.ReadTimeoutMS) * time.Millisecond,
		WriteTimeout: time.Duration(config.Connection.WriteTimeoutMS) * time.Millisecond,
		DialTimeout:  time.Duration(config.Connection.DialTimeoutMS) * time.Millisecond,
		MinIdleConns: config.Connection.MinIdleConns,
		PoolTimeout:  time.Duration(config.Connection.PoolTimeoutMS) * time.Millisecond,
		PoolSize:     config.Connection.PoolSize,
		MaxRetries:   config.Connection.MaxRetries,
		MaxConnAge:   time.Duration(config.Connection.MaxConnAgeSeconds) * time.Second,
	}
	if config.Connection.IdleTimeoutSecond == -1 {
		opt.IdleTimeout = -1
	} else {
		opt.IdleTimeout = time.Duration(config.Connection.IdleTimeoutSecond) * time.Second
	}
	if config.Connection.MinRetryBackoffMS == -1 {
		opt.MinRetryBackoff = -1
	} else {
		opt.MinRetryBackoff = time.Duration(config.Connection.MinRetryBackoffMS) * time.Millisecond
	}
	if config.Connection.MaxRetryBackoffMS == -1 {
		opt.MaxRetryBackoff = -1
	} else {
		opt.MaxRetryBackoff = time.Duration(config.Connection.MaxRetryBackoffMS) * time.Millisecond
	}
	if config.Connection.IdleCheckFrequencySeconds == -1 {
		opt.IdleCheckFrequency = -1
	} else {
		opt.IdleCheckFrequency = time.Duration(config.Connection.IdleCheckFrequencySeconds) * time.Second
	}
	return opt, nil
}
