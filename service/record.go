package service

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"fmt"
	"time"
)

const (
	metricLoadKeyError                = "error.loadkey"
	metricLoadKeyRetryError           = "error.loadkey.retry"
	metricLoadKeyFromDBError          = "error.loadkey.db"
	metricLoadKeyIntoRedisError       = "error.loadkey.redis"
	metricLoadKeyCheckNeedToLoadError = "error.loadkey.check_need_to_load"

	metricLoadKeySuccess                  = "loadkey.success"
	metricLoadKeySuccessDuration          = "loadkey.duration"
	metricLoadKeySuccessNoKeys            = "loadkey.success.nokeys"
	metricLoadKeySuccessNoKeysDuration    = "loadkey.nokeys.duration"
	metricLoadKeyFromDBSuccess            = "loadkey.db.success"
	metricLoadKeyFromDBNotFound           = "loadkey.db.not_found"
	metricLoadKeyFromDBNotFoundDuration   = "loadkey.db.not_found.duration"
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

func recordLoadKeyCheckNeedToLoadError(logger *log.Logger, metric *base.MetricClient, hashTag string, err error) {
	logger.Error(
		metricLoadKeyCheckNeedToLoadError,
		log.String("hash_tag", hashTag),
		log.Error(err),
	)
	metric.MetricIncrease(metricLoadKeyCheckNeedToLoadError)
}

func recordLoadKeySuccess(logger *log.Logger, metric *base.MetricClient, hashTag string, duration time.Duration, count int) {
	if count > 0 {
		logger.Info(
			metricLoadKeySuccess,
			log.String("hash_tag", hashTag),
			log.Int("count", count),
			log.String("duration", duration.String()))
		metric.MetricIncrease(metricLoadKeySuccess)
		metric.MetricTimeDuration(metricLoadKeySuccessDuration, duration)
	} else {
		metric.MetricIncrease(metricLoadKeySuccessNoKeys)
		metric.MetricTimeDuration(metricLoadKeySuccessNoKeysDuration, duration)
	}
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

func recordLoadDBRecordNotFound(metric *base.MetricClient, hashTag string, duration time.Duration) {
	metric.MetricIncrease(metricLoadKeyFromDBNotFound)
	metric.MetricTimeDuration(metricLoadKeyFromDBNotFoundDuration, duration)
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

func recordTaskError(logger *log.Logger, metric *base.MetricClient, taskName string, err error, reason string, ctxInfo map[string]string) {
	recordTaskErrorLog(logger, taskName, err, reason, ctxInfo)
	recordTaskErrorMetric(metric, taskName, reason)
}

func recordTaskErrorLog(logger *log.Logger, taskName string, err error, reason string, ctxInfo map[string]string) {
	logPairs := make([]log.LogPair, 0)
	logPairs = append(logPairs, log.String("task", taskName))
	if reason != "" {
		logPairs = append(logPairs, log.String("reason", reason))
	}
	for key, value := range ctxInfo {
		logPairs = append(logPairs, log.String(key, value))
	}
	if err != nil {
		logPairs = append(logPairs, log.Error(err))
	}
	logger.Error("task error", logPairs...)
}

func recordTaskErrorMetric(metric *base.MetricClient, taskName string, reasons ...string) {
	metricName := fmt.Sprintf("%s.error", taskName)
	metric.MetricIncrease(metricName)
	for _, reason := range reasons {
		errorMetricName := fmt.Sprintf("%s.%s", metricName, reason)
		metric.MetricIncrease(errorMetricName)
	}
}

func recordTaskSuccessInfo(logger *log.Logger, metric *base.MetricClient, taskName string, info string, count int) {
	metricName := fmt.Sprintf("%s.success.%s", taskName, info)
	logger.Info(metricName, log.Int("count", count))
	metric.MetricCount(metricName, count)
}

func recordTaskSuccess(logger *log.Logger, metric *base.MetricClient, taskName string, d time.Duration) {
	recordTaskSuccessLog(logger, taskName, d)
	recordTaskSuccessMetric(metric, taskName, d)
}

func recordTaskSuccessLog(logger *log.Logger, taskName string, d time.Duration) {
	logger.Info(
		"task success",
		log.String("task", taskName),
		log.String("duration", d.String()),
	)
}

func recordTaskSuccessMetric(metric *base.MetricClient, taskName string, d time.Duration) {
	metricName := fmt.Sprintf("%s.success", taskName)
	metric.MetricIncrease(metricName)
	if d != time.Duration(0) {
		durationMetricName := fmt.Sprintf("%s.duration", metricName)
		metric.MetricTimeDuration(durationMetricName, d)
	}
}

func logTaskStart(logger *log.Logger, taskName string, startTime time.Time, pairs ...log.LogPair) {
	logPairs := []log.LogPair{
		log.String("task", taskName),
		log.String("start_time", startTime.String()),
	}
	for _, pair := range pairs {
		logPairs = append(logPairs, pair)
	}
	logger.Info(
		"start task",
		logPairs...,
	)
}
