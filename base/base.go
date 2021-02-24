package base

import (
	"bytepower_room/base/log"
	"errors"
	"time"

	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
)

var redisCluster *redis.ClusterClient
var dbCluster *DBCluster
var writtenRecordDBCluster *DBCluster
var accessedRecordDBCluster *DBCluster
var eventService *EventService
var metricService MetricClient
var taskMetricService MetricClient
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

	rdsCluster, err := NewRedisClusterFromConfig(config.RedisCluster)
	if err != nil {
		return err
	}
	redisCluster = rdsCluster

	databaseCluster, err := NewDBClusterFromConfig(config.DBCluster, GetServerLogger())
	if err != nil {
		return err
	}
	dbCluster = databaseCluster

	// Init Metric.
	metric, err := InitMetric(config.Metric)
	if err != nil {
		return err
	}
	metricService = metric

	event, err := NewEventService(config.EventService, loggers["server"])
	if err != nil {
		return nil
	}
	eventService = event
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

func GetMetricService() MetricClient {
	return metricService
}

func GetServerLogger() *log.Logger {
	return loggers["server"]
}

func GetTaskLogger() *log.Logger {
	return loggers["task"]
}

func GetTaskMetricService() MetricClient {
	return taskMetricService
}

func GetServerConfig() Config {
	return serverConfig
}

func StopServices() {
	eventService.Stop()
}
