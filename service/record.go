package service

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"fmt"
	"time"
)

const (
	metricLoadKeyError               = "error.loadkey"
	metricLoadKeyRetryError          = "error.loadkey.retry"
	metricLoadKeyFromDBError         = "error.loadkey.db"
	metricLoadKeyFromDBNotFoundError = "error.loadkey.db.not_found"
	metricLoadKeyIntoRedisError      = "error.loadkey.redis"

	metricAddEventToDBRetryError = "error.add_event_to_db.retry"

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

func recordTaskErrorV2(logger *log.Logger, metric *base.MetricClient, taskName string, err error, reason string, ctxInfo map[string]string) {
	recordTaskErrorLog2(logger, taskName, err, reason, ctxInfo)
	recordTaskErrorMetric2(metric, taskName, reason)
}

func recordTaskErrorLog2(logger *log.Logger, taskName string, err error, reason string, ctxInfo map[string]string) {
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

func recordTaskErrorMetric2(metric *base.MetricClient, taskName string, reasons ...string) {
	metricName := fmt.Sprintf("%s.error", taskName)
	metric.MetricIncrease(metricName)
	for _, reason := range reasons {
		errorMetricName := fmt.Sprintf("%s.%s", metricName, reason)
		metric.MetricIncrease(errorMetricName)
	}
}

func recordTaskSuccessV2(logger *log.Logger, metric *base.MetricClient, taskName string, d time.Duration) {
	recordTaskSuccessLogV2(logger, taskName, d)
	recordTaskSuccessMetricV2(metric, taskName, d)
}

func recordTaskSuccessLogV2(logger *log.Logger, taskName string, d time.Duration) {
	logger.Info(
		"task success",
		log.String("task", taskName),
		log.String("duration", d.String()),
	)
}

func recordTaskSuccessMetricV2(metric *base.MetricClient, taskName string, d time.Duration) {
	metricName := fmt.Sprintf("%s.success", taskName)
	metric.MetricIncrease(metricName)
	if d != time.Duration(0) {
		durationMetricName := fmt.Sprintf("%s.duration", metricName)
		metric.MetricTimeDuration(durationMetricName, d)
	}
}

func recordTaskSuccessInfo(logger *log.Logger, metric *base.MetricClient, taskName string, info string, count int) {
	metricName := fmt.Sprintf("%s.success.%s", taskName, info)
	logger.Info(metricName, log.Int("count", count))
	metric.MetricCount(metricName, count)
}
