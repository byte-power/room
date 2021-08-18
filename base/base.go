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
var serviceDBCluster *DBCluster
var taskDBCluster *DBCluster
var collectEventDBCluster *DBCluster
var hashTagEventService *HashTagEventService
var metricService *MetricClient
var taskMetricService *MetricClient
var collectEventMetricService *MetricClient
var loggers map[string]*log.Logger
var serverConfig Config
var json = jsoniter.ConfigCompatibleWithStandardLibrary

func initBasicDependencies(configPath string) error {
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

	rdsCluster, err := NewRedisClusterFromConfig(config.RedisCluster, GetServerLogger())
	if err != nil {
		return err
	}
	redisHook := newRedisRecordHook(metricService, GetServerLogger())

	rdsCluster.AddHook(redisHook)
	redisCluster = rdsCluster

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

func InitRoomService(configPath string) error {
	if err := initBasicDependencies(configPath); err != nil {
		return err
	}
	logger := GetServerLogger()

	databaseCluster, err := NewDBClusterFromConfig(serverConfig.ServiceDBCluster)
	if err != nil {
		return err
	}
	queryHook := dbLogger{logger: logger, metricClient: GetMetricService(), durationMetricKey: serviceDBQueryDurationMetricKey}
	databaseCluster.AddQueryHook(queryHook)
	logger.Info("init room service database cluster", log.String("cluster", databaseCluster.String()))
	serviceDBCluster = databaseCluster

	hashTagEventSrv, err := NewHashTagEventService(serverConfig.HashTagEventService, loggers["server"], metricService)
	if err != nil {
		return err
	}
	hashTagEventService = hashTagEventSrv

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
	if err := initBasicDependencies(configPath); err != nil {
		return err
	}
	logger := GetTaskLogger()
	syncServiceConfig := GetServerConfig().SyncService
	metric, err := InitMetric(syncServiceConfig.Metric)
	if err != nil {
		return err
	}
	taskMetricService = metric

	databaseCluster, err := NewDBClusterFromConfig(serverConfig.TaskDBCluster)
	if err != nil {
		return err
	}
	queryHook := dbLogger{logger: logger, metricClient: GetTaskMetricService(), durationMetricKey: taskDBQueryDurationMetricKey}
	databaseCluster.AddQueryHook(queryHook)
	logger.Info("init task database cluster", log.String("cluster", databaseCluster.String()))
	taskDBCluster = databaseCluster

	rawNoWrittenDuration := syncServiceConfig.SyncKeyTaskV2.RawNoWrittenDuration
	duration, err := time.ParseDuration(rawNoWrittenDuration)
	if err != nil {
		return err
	}
	serverConfig.SyncService.SyncKeyTaskV2.NoWrittenDuration = duration

	rawInactiveDuration := syncServiceConfig.CleanKeyTaskV2.RawInactiveDuration
	duration, err = time.ParseDuration(rawInactiveDuration)
	if err != nil {
		return err
	}
	serverConfig.SyncService.CleanKeyTaskV2.InactiveDuration = duration
	return nil
}

func InitCollectEventService(configPath string) error {
	if err := initBasicDependencies(configPath); err != nil {
		return err
	}
	logger := GetCollectEventLogger()
	metricConfig := GetServerConfig().CollectEventServiceMetric
	metric, err := InitMetric(metricConfig)
	if err != nil {
		return err
	}
	collectEventMetricService = metric

	databaseCluster, err := NewDBClusterFromConfig(serverConfig.CollectEventDBCluster)
	if err != nil {
		return err
	}
	queryHook := dbLogger{
		logger:            logger,
		metricClient:      GetCollectEventMetricService(),
		durationMetricKey: collectEventDBQueryDurationMetricKey}
	databaseCluster.AddQueryHook(queryHook)
	logger.Info("init collect event service database cluster", log.String("cluster", databaseCluster.String()))
	collectEventDBCluster = databaseCluster
	return nil
}

func GetRedisCluster() *redis.ClusterClient {
	return redisCluster
}

func GetServiceDBCluster() *DBCluster {
	return serviceDBCluster
}

func GetTaskDBCluster() *DBCluster {
	return taskDBCluster
}

func GetCollectEventDBCluster() *DBCluster {
	return collectEventDBCluster
}

func GetHashTagEventService() *HashTagEventService {
	return hashTagEventService
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

func GetCollectEventLogger() *log.Logger {
	return loggers["collect_event"]
}

func GetTaskMetricService() *MetricClient {
	return taskMetricService
}

func GetCollectEventMetricService() *MetricClient {
	return collectEventMetricService
}

func GetServerConfig() Config {
	return serverConfig
}

func StartServices() {
	hashTagEventService.Run()
}

func StopServices() {
	hashTagEventService.Stop()
}

const (
	redisCommandStartTimeContextKey  = "command_start_time"
	redisPipelineStartTimeContextKey = "pipeline_start_time"
	redisCommandDurationMetricKey    = "redis.command.duration"
	redisPipelineDurationMetricKey   = "redis.pipeline.duration"
)

type redisRecordHook struct {
	metricClient *MetricClient
	logger       *log.Logger
}

func newRedisRecordHook(metricClient *MetricClient, logger *log.Logger) redisRecordHook {
	return redisRecordHook{metricClient: metricClient, logger: logger}
}

func (hook redisRecordHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	return context.WithValue(ctx, redisCommandStartTimeContextKey, time.Now()), nil
}

func (hook redisRecordHook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	if startTime, ok := ctx.Value(redisCommandStartTimeContextKey).(time.Time); ok {
		duration := time.Since(startTime)
		hook.logger.Debug(
			"execute redis command",
			log.String("command", cmd.String()),
			log.String("duration", duration.String()),
		)
		hook.metricClient.MetricTimeDuration(redisCommandDurationMetricKey, duration)
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
		hook.logger.Debug(
			"execute redis pipeline",
			log.String("commands", sb.String()),
			log.String("duration", duration.String()),
		)
		hook.metricClient.MetricTimeDuration(redisPipelineDurationMetricKey, duration)
	}
	return nil
}

type Dependency struct {
	Redis  *redis.ClusterClient
	DB     *DBCluster
	Logger *log.Logger
	Metric *MetricClient
}

var (
	ErrDepRedisNull  = errors.New("redis service is null")
	ErrDepDBNull     = errors.New("db service is null")
	ErrDepLoggerNull = errors.New("logger is null")
	ErrDepMetricNull = errors.New("metric service is null")
)

func (dep Dependency) Check() error {
	if dep.Redis == nil {
		return ErrDepRedisNull
	}
	if dep.DB == nil {
		return ErrDepDBNull
	}
	if dep.Logger == nil {
		return ErrDepLoggerNull
	}
	if dep.Metric == nil {
		return ErrDepMetricNull
	}
	return nil
}

func GetServerDependency() Dependency {
	return Dependency{
		Redis:  GetRedisCluster(),
		DB:     GetServiceDBCluster(),
		Logger: GetServerLogger(),
		Metric: GetMetricService(),
	}
}

func GetTaskDependency() Dependency {
	return Dependency{
		Redis:  GetRedisCluster(),
		DB:     GetTaskDBCluster(),
		Logger: GetTaskLogger(),
		Metric: GetTaskMetricService(),
	}
}

func GetCollectEventDependency() Dependency {
	return Dependency{
		Redis:  GetRedisCluster(),
		DB:     GetCollectEventDBCluster(),
		Logger: GetCollectEventLogger(),
		Metric: GetCollectEventMetricService(),
	}
}
