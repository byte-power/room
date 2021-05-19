package service

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"time"
)

const (
	metricLoadKeyError               = "error.loadkey"
	metricLoadKeyRetryError          = "error.loadkey.retry"
	metricLoadKeyFromDBError         = "error.loadkey.db"
	metricLoadKeyFromDBNotFoundError = "error.loadkey.db.not_found"
	metricLoadKeyIntoRedisError      = "error.loadkey.redis"

	metricLoadKeySuccess                  = "loadkey.success"
	metricLoadKeySuccessDuration          = "loadkey.duration"
	metricLoadKeyFromDBSuccess            = "loadkey.db.success"
	metricLoadKeyIntoRedisSuccess         = "loadkey.redis.success"
	metricLoadKeyIntoRedisSuccessDuration = "loadkey.redis.success.duration"
)

func recordLoadKeyError(logger *log.Logger, metric *base.MetricClient, hashTag string, err error, duration time.Duration, count int) {
	logger.Error(
		metricLoadKeyError,
		log.String("hash_tag", hashTag),
		log.Int("count", count),
		log.Error(err),
		log.String("duration", duration.String()))
	metric.MetricIncrease(metricLoadKeyError)
}

func recordLoadKeyRetryError(logger *log.Logger, metric *base.MetricClient, hashTag string, err error, times, count int) {
	logger.Error(
		metricLoadKeyRetryError,
		log.String("hash_tag", hashTag),
		log.Int("count", count),
		log.Int("load_times", times),
		log.Error(err))
	metric.MetricIncrease(metricLoadKeyRetryError)
}

func recordLoadKeySuccess(logger *log.Logger, metric *base.MetricClient, hashTag string, duration time.Duration, count int) {
	logger.Info(
		metricLoadKeySuccess,
		log.String("hash_tag", hashTag),
		log.Int("count", count),
		log.String("duration", duration.String()))
	metric.MetricIncrease(metricLoadKeySuccess)
	metric.MetricTimeDuration(metricLoadKeySuccessDuration, duration)
}

func recordLoadDBSuccess(logger *log.Logger, hashTag string, duration time.Duration) {
	logger.Info(
		metricLoadKeyFromDBSuccess,
		log.String("hash_tag", hashTag),
		log.String("duration", duration.String()),
	)
}

func recordLoadDBError(logger *log.Logger, hashTag string, duration time.Duration, err error) {
	logger.Error(
		metricLoadKeyFromDBError,
		log.String("hash_tag", hashTag),
		log.String("duration", duration.String()),
		log.Error(err),
	)
}

func recordLoadDBRecordNotFound(logger *log.Logger, metric *base.MetricClient, hashTag string) {
	logger.Error(
		metricLoadKeyFromDBNotFoundError,
		log.String("hash_tag", hashTag),
	)
	metric.MetricIncrease(metricLoadKeyFromDBNotFoundError)
}

func recordLoadIntoRedisError(logger *log.Logger, metric *base.MetricClient, hashTag string, duration time.Duration, count int, err error) {
	logger.Error(
		metricLoadKeyIntoRedisError,
		log.String("hash_tag", hashTag),
		log.Int("count", count),
		log.Error(err),
		log.String("duration", duration.String()),
	)
	metric.MetricIncrease(metricLoadKeyIntoRedisError)
}

func recordLoadIntoRedisSuccess(logger *log.Logger, metric *base.MetricClient, hashTag string, duration time.Duration, count int) {
	logger.Info(
		metricLoadKeyIntoRedisSuccess,
		log.String("hash_tag", hashTag),
		log.Int("count", count),
		log.String("duration", duration.String()),
	)
	metric.MetricIncrease(metricLoadKeyIntoRedisSuccess)
	metric.MetricTimeDuration(metricLoadKeyIntoRedisSuccessDuration, duration)
}
