package base

import (
	"bytepower_room/base/log"
	"context"
	"errors"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
)

var redisCluster *redis.ClusterClient
var dbCluster *DBCluster
var writtenRecordDBCluster *DBCluster
var accessedRecordDBCluster *DBCluster
var eventService *EventService
var metricService *MetricClient
var taskMetricService *MetricClient
var loggers map[string]*log.Logger
var serverConfig Config
var json = jsoniter.ConfigCompatibleWithStandardLibrary

func InitServices(configPath string) error {
	config, err := NewConfigFromFile(configPath)
	if err != nil {
		return err
	}
	serverConfig = config

	loggers = make(map[string]*log.Logger, len(config.Log))
	if !areAllRequiredLoggersConfigured(config.Log) {
		return errors.New("not all required loggers are configured")
	}
	for name, value := range config.Log {
		logger, err := parseLogger(config.Name, name, value)
		if err != nil {
			return err
		}
		loggers[name] = logger
	}

	// Init Metric.
	metric, err := InitMetric(config.Metric)
	if err != nil {
		return err
	}
	metricService = metric

	rdsCluster, err := NewRedisClusterFromConfig(config.RedisCluster)
	if err != nil {
		return err
	}
	redisHook := newRedisRecordHook(
		metricService, GetServerLogger(),
		redisCommandDurationMetricKey,
		redisPipelineDurationMetricKey)

	rdsCluster.AddHook(redisHook)
	redisCluster = rdsCluster

	databaseCluster, err := NewDBClusterFromConfig(config.DBCluster, GetServerLogger())
	if err != nil {
		return err
	}
	dbCluster = databaseCluster

	event, err := NewEventService(config.EventService, loggers["server"])
	if err != nil {
		return nil
	}
	eventService = event
	d, err := time.ParseDuration(serverConfig.LoadKey.RawRetryInterval)
	if err != nil {
		return err
	}
	serverConfig.LoadKey.retryInterval = d

	d, err = time.ParseDuration(serverConfig.LoadKey.RawLoadTimeout)
	if err != nil {
		return err
	}
	serverConfig.LoadKey.loadTimeout = d
	return nil
}

func areAllRequiredLoggersConfigured(loggers map[string]map[string]interface{}) bool {
	requiredLoggerNames := []string{"server", "task"}
	for _, name := range requiredLoggerNames {
		if _, ok := loggers[name]; !ok {
			return false
		}
	}
	return true
}

func InitSyncService(configPath string) error {
	if err := InitServices(configPath); err != nil {
		return err
	}
	syncServiceConfig := GetServerConfig().SyncService
	metric, err := InitMetric(syncServiceConfig.Metric)
	if err != nil {
		return err
	}
	taskMetricService = metric

	writtenRecordCluster, err := NewDBClusterFromConfig(syncServiceConfig.WrittenRecordDBCluster, GetServerLogger())
	if err != nil {
		return err
	}
	writtenRecordDBCluster = writtenRecordCluster
	accessedRecordCluster, err := NewDBClusterFromConfig(syncServiceConfig.AccessedRecordDBCluster, GetServerLogger())
	if err != nil {
		return err
	}
	accessedRecordDBCluster = accessedRecordCluster
	rawInactiveDuration := syncServiceConfig.CleanKeyTask.RawInactiveDuration
	duration, err := time.ParseDuration(rawInactiveDuration)
	if err != nil {
		return err
	}
	serverConfig.SyncService.CleanKeyTask.InactiveDuration = duration
	return nil
}

func GetRedisCluster() *redis.ClusterClient {
	return redisCluster
}

func GetDBCluster() *DBCluster {
	return dbCluster
}

func GetWrittenRecordDBCluster() *DBCluster {
	return writtenRecordDBCluster
}

func GetAccessedRecordDBCluster() *DBCluster {
	return accessedRecordDBCluster
}

func GetEventService() *EventService {
	return eventService
}

func GetMetricService() *MetricClient {
	return metricService
}

func GetServerLogger() *log.Logger {
	return loggers["server"]
}

func GetTaskLogger() *log.Logger {
	return loggers["task"]
}

func GetTaskMetricService() *MetricClient {
	return taskMetricService
}

func GetServerConfig() Config {
	return serverConfig
}

func StopServices() {
	eventService.Stop()
}

const (
	redisCommandStartTimeContextKey  = "command_start_time"
	redisPipelineStartTimeContextKey = "pipeline_start_time"
	redisCommandDurationMetricKey    = "redis.command.duration"
	redisPipelineDurationMetricKey   = "redis.pipeline.duration"
)

type redisRecordHook struct {
	metricClient      *MetricClient
	logger            *log.Logger
	commandMetricKey  string
	pipelineMetricKey string
}

func newRedisRecordHook(metricClient *MetricClient, logger *log.Logger, commandMetricKey, pipelineMetricKey string) redisRecordHook {
	return redisRecordHook{
		metricClient:      metricClient,
		logger:            logger,
		commandMetricKey:  commandMetricKey,
		pipelineMetricKey: pipelineMetricKey,
	}
}

func (hook redisRecordHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	return context.WithValue(ctx, redisCommandStartTimeContextKey, time.Now()), nil
}

func (hook redisRecordHook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	if startTime, ok := ctx.Value(redisCommandStartTimeContextKey).(time.Time); ok {
		duration := time.Since(startTime)
		hook.logger.Info(
			"redis command latency",
			log.String("command", cmd.String()),
			log.String("duration", duration.String()),
		)
		metricService.MetricTimeDuration(hook.commandMetricKey, duration)
	}
	return nil
}

func (hook redisRecordHook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	return context.WithValue(ctx, redisPipelineStartTimeContextKey, time.Now()), nil
}

func (hook redisRecordHook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	if startTime, ok := ctx.Value(redisPipelineStartTimeContextKey).(time.Time); ok {
		var sb strings.Builder
		sb.WriteString("[")
		for _, cmd := range cmds {
			sb.WriteString(cmd.String())
			sb.WriteString(" ")
		}
		sb.WriteString("]")
		duration := time.Since(startTime)
		hook.logger.Info(
			"redis pipeline latency",
			log.String("commands", sb.String()),
			log.String("duration", duration.String()),
		)
		metricService.MetricTimeDuration(hook.pipelineMetricKey, duration)
	}
	return nil
}
