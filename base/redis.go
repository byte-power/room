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
		ReadTimeout:  time.Duration(config.ReadTimeoutMS) * time.Millisecond,
		WriteTimeout: time.Duration(config.WriteTimeoutMS) * time.Millisecond,
		DialTimeout:  time.Duration(config.DialTimeoutMS) * time.Millisecond,
		MinIdleConns: config.MinIdleConns,
		PoolTimeout:  time.Duration(config.PoolTimeoutMS) * time.Millisecond,
		PoolSize:     config.PoolSize,
		MaxRetries:   config.MaxRetries,
		MaxConnAge:   time.Duration(config.MaxConnAgeSeconds) * time.Second,
	}
	if config.IdleTimeoutMS == -1 {
		opt.IdleTimeout = -1
	} else {
		opt.IdleTimeout = time.Duration(config.IdleTimeoutMS) * time.Millisecond
	}
	if config.MinRetryBackoffMS == -1 {
		opt.MinRetryBackoff = -1
	} else {
		opt.MinRetryBackoff = time.Duration(config.MinRetryBackoffMS) * time.Millisecond
	}
	if config.MaxRetryBackoffMS == -1 {
		opt.MaxRetryBackoff = -1
	} else {
		opt.MaxRetryBackoff = time.Duration(config.MaxRetryBackoffMS) * time.Millisecond
	}
	if config.IdleCheckFrequencySeconds == -1 {
		opt.IdleCheckFrequency = -1
	} else {
		opt.IdleCheckFrequency = time.Duration(config.IdleCheckFrequencySeconds) * time.Second
	}
	return opt, nil
}
