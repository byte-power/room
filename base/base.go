package base

import (
	"bytepower_room/base/log"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
)

var serverRedisCluster *redis.ClusterClient
var taskRedisCluster *redis.ClusterClient
var collectEventRedisCluster *redis.ClusterClient

var serverDBCluster *DBCluster
var taskDBCluster *DBCluster
var collectEventDBCluster *DBCluster

var hashTagEventService *HashTagEventService

var serverMetricService *MetricClient
var taskMetricService *MetricClient
var collectEventMetricService *MetricClient

var serverLogger *log.Logger
var collectEventLogger *log.Logger
var taskLogger *log.Logger

var serverConfig *RoomServerConfig
var taskConfig *RoomTaskConfig
var collectEventConfig *RoomCollectEventConfig

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func initService(
	loggerName string, logConfig map[string]interface{},
	metricConfig MetricConfig, metricKey string,
	redisClusterConfig RedisClusterConfig,
	dbClusterConfig DBClusterConfig,
) (*log.Logger, *MetricClient, *redis.ClusterClient, *DBCluster, error) {

	logger, err := parseLogger(loggerName, logConfig)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("init_logger.%w", err)
	}
	metric, err := InitMetric(metricConfig)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("init_metric.%w", err)
	}
	rdsCluster, err := NewRedisClusterFromConfig(redisClusterConfig, logger, metric)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("init_redis.%w", err)
	}
	databaseCluster, err := NewDBClusterFromConfig(dbClusterConfig, logger, metric, metricKey)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("init_db.%w", err)
	}
	return logger, metric, rdsCluster, databaseCluster, nil
}

func InitRoomServer(configPath string) error {
	config, err := newConfigFromFile(configPath)
	if err != nil {
		return err
	}

	serverConfig = &config.Server

	if err = serverConfig.init(); err != nil {
		return err
	}

	serverLogger, serverMetricService, serverRedisCluster, serverDBCluster, err = initService(
		"room.server", serverConfig.Log, serverConfig.Metric,
		serviceDBQueryDurationMetricKey, serverConfig.RedisCluster,
		serverConfig.DB)
	if err != nil {
		return err
	}

	hashTagEventService, err = NewHashTagEventService(&serverConfig.HashTagEventService, serverLogger, serverMetricService)
	if err != nil {
		return err
	}

	serverLogger.Info(
		"init room server service",
		log.String("config", fmt.Sprintf("%+v", serverConfig)),
		log.String("redis", fmt.Sprintf("%+v", *serverRedisCluster.Options())),
		log.String("database", serverDBCluster.String()),
	)

	return nil
}

func InitRoomTask(configPath string) error {
	config, err := newConfigFromFile(configPath)
	if err != nil {
		return err
	}

	taskConfig = &config.Task
	if err = taskConfig.init(); err != nil {
		return err
	}

	taskLogger, taskMetricService, taskRedisCluster, taskDBCluster, err = initService(
		"room.task", taskConfig.Log,
		taskConfig.Metric, taskDBQueryDurationMetricKey,
		taskConfig.RedisCluster, taskConfig.DB)
	if err != nil {
		return err
	}

	taskLogger.Info(
		"init room task",
		log.String("config", fmt.Sprintf("%+v", taskConfig)),
		log.String("redis", fmt.Sprintf("%+v", *taskRedisCluster.Options())),
		log.String("database", taskDBCluster.String()),
	)

	return nil
}

func InitCollectEvent(configPath string) error {
	config, err := newConfigFromFile(configPath)
	if err != nil {
		return err
	}

	collectEventConfig = &config.CollectEvent
	if err = collectEventConfig.init(); err != nil {
		return err
	}

	collectEventLogger, collectEventMetricService, collectEventRedisCluster, collectEventDBCluster, err = initService(
		"room.collect_event", collectEventConfig.Log,
		collectEventConfig.Metric, collectEventDBQueryDurationMetricKey,
		collectEventConfig.RedisCluster, collectEventConfig.DB)
	if err != nil {
		return err
	}

	collectEventLogger.Info(
		"init room collect event service",
		log.String("config", fmt.Sprintf("%+v", collectEventConfig)),
		log.String("redis", fmt.Sprintf("%+v", *collectEventRedisCluster.Options())),
		log.String("database", collectEventDBCluster.String()),
	)

	return nil
}

func GetHashTagEventService() *HashTagEventService {
	return hashTagEventService
}

func GetServerConfig() *RoomServerConfig {
	return serverConfig
}

func GetTaskConfig() *RoomTaskConfig {
	return taskConfig
}

func GetCollectEventConfig() *RoomCollectEventConfig {
	return collectEventConfig
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
			redisCommandDurationMetricKey,
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
			redisPipelineDurationMetricKey,
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
		Redis:  serverRedisCluster,
		DB:     serverDBCluster,
		Logger: serverLogger,
		Metric: serverMetricService,
	}
}

func GetTaskDependency() Dependency {
	return Dependency{
		Redis:  taskRedisCluster,
		DB:     taskDBCluster,
		Logger: taskLogger,
		Metric: taskMetricService,
	}
}

func GetCollectEventDependency() Dependency {
	return Dependency{
		Redis:  collectEventRedisCluster,
		DB:     collectEventDBCluster,
		Logger: collectEventLogger,
		Metric: collectEventMetricService,
	}
}
