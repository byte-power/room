package base

import (
	"bytepower_room/base/log"
	"bytepower_room/base/opentelemetry"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	"github.com/patrickmn/go-cache"
)

var serverDependency Dependency
var taskDependency Dependency
var collectEventDependency CollectEventDependency

var hashTagEventService *HashTagEventService
var hashTagLoadedCache *cache.Cache

var serverConfig *RoomServerConfig
var taskConfig *RoomTaskConfig
var collectEventConfig *RoomCollectEventConfig

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func initService(
	loggerName string, logConfig map[string]interface{},
	metricConfig MetricConfig, dbClusterConfig DBClusterConfig,
) (*log.Logger, *MetricClient, *DBCluster, error) {

	logger, err := parseLogger(loggerName, logConfig)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("init_logger.%w", err)
	}
	metric, err := InitMetric(metricConfig)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("init_metric.%w", err)
	}
	databaseCluster, err := NewDBClusterFromConfig(dbClusterConfig, logger, metric)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("init_db.%w", err)
	}
	return logger, metric, databaseCluster, nil
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

	logger, metric, dbCluster, err := initService(
		"room.server", serverConfig.Log,
		serverConfig.Metric, serverConfig.DB)
	if err != nil {
		return err
	}

	redisCluster, err := NewRedisClusterFromConfig(serverConfig.RedisCluster, logger, metric)
	if err != nil {
		return fmt.Errorf("init_redis.%w", err)
	}

	otelClient, err := NewOtelClientWithConfig(context.Background(), serverConfig.Otel)
	if err != nil {
		return fmt.Errorf("init_opentelemetry.%w", err)
	}

	serverDependency = Dependency{
		Redis:  redisCluster,
		DB:     dbCluster,
		Logger: logger,
		Metric: metric,
		Otel:   otelClient,
	}

	hashTagEventService, err = NewHashTagEventService(&serverConfig.HashTagEventService, logger, metric)
	if err != nil {
		return err
	}

	hashTagLoadedCache = cache.New(serverConfig.LoadKey.GetCacheDuration(), serverConfig.LoadKey.GetCacheCheckInterval())

	logger.Info(
		"init room server service",
		log.String("config", fmt.Sprintf("%+v", serverConfig)),
		log.String("redis", fmt.Sprintf("%+v", *redisCluster.Options())),
		log.String("database", dbCluster.String()),
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

	logger, metric, dbCluster, err := initService(
		"room.task", taskConfig.Log,
		taskConfig.Metric, taskConfig.DB)
	if err != nil {
		return err
	}

	redisCluster, err := NewRedisClusterFromConfig(taskConfig.RedisCluster, logger, metric)
	if err != nil {
		return fmt.Errorf("init_redis.%w", err)
	}

	taskDependency = Dependency{
		Redis:  redisCluster,
		DB:     dbCluster,
		Logger: logger,
		Metric: metric,
	}

	logger.Info(
		"init room task",
		log.String("config", fmt.Sprintf("%+v", taskConfig)),
		log.String("redis", fmt.Sprintf("%+v", *redisCluster.Options())),
		log.String("database", dbCluster.String()),
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

	logger, metric, dbCluster, err := initService(
		"room.collect_event", collectEventConfig.Log,
		collectEventConfig.Metric, collectEventConfig.DB)
	if err != nil {
		return err
	}

	collectEventDependency = CollectEventDependency{
		DB:     dbCluster,
		Logger: logger,
		Metric: metric,
	}

	logger.Info(
		"init room collect event service",
		log.String("config", fmt.Sprintf("%+v", collectEventConfig)),
		log.String("database", dbCluster.String()),
	)

	return nil
}

func GetHashTagEventService() *HashTagEventService {
	return hashTagEventService
}

func GetHashTagLoadedCache() *cache.Cache {
	return hashTagLoadedCache
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
	serverDependency.Otel.Shutdown(context.TODO())
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
	Otel   *opentelemetry.Otel
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
	return serverDependency
}

func GetTaskDependency() Dependency {
	return taskDependency
}

type CollectEventDependency struct {
	DB     *DBCluster
	Logger *log.Logger
	Metric *MetricClient
}

func (dep CollectEventDependency) Check() error {
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

func GetCollectEventDependency() CollectEventDependency {
	return collectEventDependency
}
