package base

import (
	"bytepower_room/base/log"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

func NewRedisClusterFromConfig(config RedisClusterConfig, logger *log.Logger) (*redis.ClusterClient, error) {
	opt, err := initRedisOption(config)
	if err != nil {
		return nil, err
	}
	client := redis.NewClusterClient(opt)
	logger.Info("initialize redis cluster client", log.String("options", fmt.Sprintf("%+v", *opt)))
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
