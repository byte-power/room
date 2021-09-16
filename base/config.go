package base

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v2"
)

func newConfigFromFile(filePath string) (Config, error) {
	config := Config{}
	bs, err := readFileFromPath(filePath)
	if err != nil {
		return config, err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(bs))
	if err = decoder.Decode(&config); err != nil {
		return config, err
	}
	if err := config.check(); err != nil {
		return config, err
	}
	return config, nil
}

func readFileFromPath(path string) ([]byte, error) {
	fp, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, err
	}
	defer fp.Close()
	return readBytes(fp)
}

func readBytes(fp io.Reader) ([]byte, error) {
	var buf bytes.Buffer
	_, err := io.Copy(&buf, fp)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type Config struct {
	Server       RoomServerConfig       `yaml:"server"`
	CollectEvent RoomCollectEventConfig `yaml:"collect_event"`
	Task         RoomTaskConfig         `yaml:"task"`
}

func (config Config) check() error {
	if err := config.Server.check(); err != nil {
		return fmt.Errorf("room_server.%w", err)
	}
	if err := config.CollectEvent.check(); err != nil {
		return fmt.Errorf("room_collect_event.%w", err)
	}
	if err := config.Task.check(); err != nil {
		return fmt.Errorf("room_task.%w", err)
	}
	return nil
}

type RoomServerConfig struct {
	EnablePProf         bool                      `yaml:"enable_pprof"`
	Log                 map[string]interface{}    `yaml:"log"`
	Metric              MetricConfig              `yaml:"metric"`
	LoadKey             LoadKeyConfig             `yaml:"load_key"`
	HashTagEventService HashTagEventServiceConfig `yaml:"hash_tag_event_service"`
	RedisCluster        RedisClusterConfig        `yaml:"redis_cluster"`
	DB                  DBClusterConfig           `yaml:"db_cluster"`
}

func (config RoomServerConfig) Check() error {
	return config.check()
}

func (config RoomServerConfig) check() error {
	if len(config.Log) == 0 {
		return errors.New("log should not be empty")
	}
	if err := config.Metric.check(); err != nil {
		return fmt.Errorf("metric.%w", err)
	}
	if err := config.LoadKey.check(); err != nil {
		return fmt.Errorf("load_key.%w", err)
	}
	if err := config.HashTagEventService.check(); err != nil {
		return fmt.Errorf("hash_tag_event_service.%w", err)
	}
	if err := config.RedisCluster.check(); err != nil {
		return fmt.Errorf("redis_cluster.%w", err)
	}
	if err := config.DB.check(); err != nil {
		return fmt.Errorf("db_cluster.%w", err)
	}
	return nil
}

func (config *RoomServerConfig) init() error {
	if err := config.check(); err != nil {
		return fmt.Errorf("room_server.%w", err)
	}

	d, err := time.ParseDuration(config.LoadKey.RawRetryInterval)
	if err != nil {
		return fmt.Errorf("load_key.retry_interval=%s is invalid %w", config.LoadKey.RawRetryInterval, err)
	}
	config.LoadKey.retryInterval = d

	d, err = time.ParseDuration(config.LoadKey.RawLoadTimeout)
	if err != nil {
		return fmt.Errorf("load_key.load_timeout=%s is invalid %w", config.LoadKey.RawLoadTimeout, err)
	}
	config.LoadKey.loadTimeout = d

	d, err = time.ParseDuration(config.HashTagEventService.RawAggInterval)
	if err != nil {
		return fmt.Errorf("hash_tag_event_service.agg_interval.%w", err)
	}
	config.HashTagEventService.AggInterval = d

	d, err = time.ParseDuration(config.HashTagEventService.RawMonitorInterval)
	if err != nil {
		return fmt.Errorf("hash_tag_event_service.monitor_interval.%w", err)
	}
	config.HashTagEventService.MonitorInterval = d

	d, err = time.ParseDuration(config.HashTagEventService.EventReport.RawRequestTimeout)
	if err != nil {
		return fmt.Errorf("hash_tag_event_service.event_report.request_timeout.%w", err)
	}
	config.HashTagEventService.EventReport.RequestTimeout = d

	d, err = time.ParseDuration(config.HashTagEventService.EventReport.RawRequestMaxWaitDuration)
	if err != nil {
		return fmt.Errorf("hash_tag_event_service.event_report.request_max_wait_duration.%w", err)
	}
	config.HashTagEventService.EventReport.RequestMaxWaitDuration = d

	d, err = time.ParseDuration(config.HashTagEventService.EventReport.RawRequestConnKeepAliveInterval)
	if err != nil {
		return fmt.Errorf("hash_tag_event_service.event_report.request_conn_keep_alive_interval.%w", err)
	}
	config.HashTagEventService.EventReport.RequestConnKeepAliveInterval = d

	d, err = time.ParseDuration(config.HashTagEventService.EventReport.RawRequestIdleConnTimeout)
	if err != nil {
		return fmt.Errorf("hash_tag_event_service.event_report.request_idle_conn_timeout.%w", err)
	}
	config.HashTagEventService.EventReport.RequestIdleConnTimeout = d

	return nil
}

type LoadKeyConfig struct {
	RetryTimes       int    `yaml:"retry_times"`
	RawRetryInterval string `yaml:"retry_interval"`
	RawLoadTimeout   string `yaml:"load_timeout"`
	loadTimeout      time.Duration
	retryInterval    time.Duration
}

func (config LoadKeyConfig) check() error {
	if config.RetryTimes <= 0 {
		return fmt.Errorf("retry_times=%d, should be greater than 0", config.RetryTimes)
	}
	d, err := time.ParseDuration(config.RawRetryInterval)
	if err != nil {
		return fmt.Errorf("retry_interval=%s, should be in valid duration format", config.RawRetryInterval)
	}
	if d <= 0 {
		return fmt.Errorf("retry_interval=%s, duration should be positive", config.RawRetryInterval)
	}
	d, err = time.ParseDuration(config.RawLoadTimeout)
	if err != nil {
		return fmt.Errorf("load_timeout=%s, should be in valid duration format", config.RawLoadTimeout)
	}
	if d <= 0 {
		return fmt.Errorf("load_timeout=%s, duration should be positive", config.RawLoadTimeout)
	}
	return nil
}

func (config LoadKeyConfig) GetRetryTimes() int {
	return config.RetryTimes
}

func (config LoadKeyConfig) GetRetryInterval() time.Duration {
	return config.retryInterval
}

func (config LoadKeyConfig) GetLoadTimeout() time.Duration {
	return config.loadTimeout
}

type RoomCollectEventConfig struct {
	Log    map[string]interface{} `yaml:"log"`
	Metric MetricConfig           `yaml:"metric"`

	Server    CollectEventServiceServerConfig    `yaml:"server"`
	SaveEvent CollectEventServiceSaveEventConfig `yaml:"save_event"`

	BufferLimit int `yaml:"buffer_limit"`

	RawAggInterval string `yaml:"agg_interval"`
	AggInterval    time.Duration

	ServerShutdownTimeoutSeconds int `yaml:"server_shutdown_timeout_seconds"`

	RawMonitorInterval string `yaml:"monitor_interval"`
	MonitorInterval    time.Duration

	DB DBClusterConfig `yaml:"db_cluster"`
}

func (config RoomCollectEventConfig) check() error {
	if len(config.Log) == 0 {
		return errors.New("log should not be empty")
	}
	if err := config.Metric.check(); err != nil {
		return fmt.Errorf("metric.%w", err)
	}
	if err := config.Server.check(); err != nil {
		return fmt.Errorf("server.%w", err)
	}
	if err := config.SaveEvent.check(); err != nil {
		return fmt.Errorf("save_event.%w", err)
	}
	if config.BufferLimit <= 0 {
		return fmt.Errorf("buffer_limit is %d, it should be greater than 0", config.BufferLimit)
	}
	if config.RawAggInterval == "" {
		return errors.New("agg_interval should not be empty")
	}
	if config.ServerShutdownTimeoutSeconds <= 0 {
		return fmt.Errorf("server_shutdown_timeout_seconds is %d, it should be greater than 0", config.ServerShutdownTimeoutSeconds)
	}
	if config.RawMonitorInterval == "" {
		return errors.New("monitor_interval should not be empty")
	}
	if err := config.DB.check(); err != nil {
		return fmt.Errorf("db_cluster.%w", err)
	}
	return nil
}

func (config *RoomCollectEventConfig) init() error {
	if err := config.check(); err != nil {
		return fmt.Errorf("room_collect_event.%w", err)
	}

	duration, err := time.ParseDuration(config.RawAggInterval)
	if err != nil {
		return fmt.Errorf("agg_interval.%w", err)
	}
	config.AggInterval = duration

	duration, err = time.ParseDuration(config.RawMonitorInterval)
	if err != nil {
		return fmt.Errorf("monitor_interval is inavlid %w", err)
	}
	config.MonitorInterval = duration
	return nil
}

type CollectEventServiceServerConfig struct {
	URL            string `yaml:"url"`
	ReadTimeoutMS  int    `yaml:"read_timeout_ms"`
	WriteTimeoutMS int    `yaml:"write_timeout_ms"`
	IdleTimeoutMS  int    `yaml:"idle_timeout_ms"`
}

func (config CollectEventServiceServerConfig) check() error {
	if config.URL == "" {
		return errors.New("url should not be empty")
	}
	if config.ReadTimeoutMS <= 0 {
		return fmt.Errorf("read_timeout_ms is %d, it should be greater than 0", config.ReadTimeoutMS)
	}
	if config.WriteTimeoutMS <= 0 {
		return fmt.Errorf("write_timeout_ms is %d, it should be greater than 0", config.WriteTimeoutMS)
	}
	if config.IdleTimeoutMS <= 0 {
		return fmt.Errorf("idle_timeout_ms is %d, it should be greater than 0", config.IdleTimeoutMS)
	}
	return nil
}

type CollectEventServiceSaveEventConfig struct {
	RetryTimes      int `yaml:"retry_times"`
	RetryIntervalMS int `yaml:"retry_interval_ms"`
	TimeoutMS       int `yaml:"timeout_ms"`
	WorkerCount     int `yaml:"worker_count"`
}

func (config CollectEventServiceSaveEventConfig) check() error {
	if config.RetryTimes <= 0 {
		return fmt.Errorf("retry_times is %d, it should be greater than 0", config.RetryTimes)
	}
	if config.RetryIntervalMS <= 0 {
		return fmt.Errorf("retry_interval_ms is %d, it should be greater than 0", config.RetryIntervalMS)
	}
	if config.TimeoutMS <= 0 {
		return fmt.Errorf("timeout_ms is %d, it should be greater than 0", config.TimeoutMS)
	}
	if config.WorkerCount <= 0 {
		return fmt.Errorf("worker_count is %d, it should be greater than 0", config.WorkerCount)
	}
	return nil
}

type RoomTaskConfig struct {
	Log          map[string]interface{} `yaml:"log"`
	Metric       MetricConfig           `yaml:"metric"`
	RedisCluster RedisClusterConfig     `yaml:"redis_cluster"`
	DB           DBClusterConfig        `yaml:"db_cluster"`
	Coordinator  CoordinatorConfig      `yaml:"coordinator"`
	SyncKeyTask  SyncKeyTaskConfig      `yaml:"sync_key_task"`
	CleanKeyTask CleanKeyTaskConfig     `yaml:"clean_key_task"`
}

func (config RoomTaskConfig) check() error {
	if len(config.Log) == 0 {
		return errors.New("log should not be empty")
	}
	if err := config.Metric.check(); err != nil {
		return fmt.Errorf("metric.%w", err)
	}
	if err := config.RedisCluster.check(); err != nil {
		return fmt.Errorf("redis_cluster.%w", err)
	}
	if err := config.DB.check(); err != nil {
		return fmt.Errorf("db_cluster.%w", err)
	}
	if err := config.Coordinator.check(); err != nil {
		return fmt.Errorf("coordinator.%w", err)
	}
	if err := config.SyncKeyTask.check(); err != nil {
		return fmt.Errorf("sync_key_task.%w", err)
	}
	if err := config.CleanKeyTask.check(); err != nil {
		return fmt.Errorf("clean_key_task.%w", err)
	}
	return nil
}

func (config *RoomTaskConfig) init() error {
	if err := config.check(); err != nil {
		return fmt.Errorf("room_task.%w", err)
	}

	rawNoWrittenDuration := config.SyncKeyTask.RawNoWrittenDuration
	duration, err := time.ParseDuration(rawNoWrittenDuration)
	if err != nil {
		return fmt.Errorf("sync_key_task.no_written_duration=%s is invalid %w", rawNoWrittenDuration, err)
	}
	config.SyncKeyTask.NoWrittenDuration = duration

	rawInactiveDuration := config.CleanKeyTask.RawInactiveDuration
	duration, err = time.ParseDuration(rawInactiveDuration)
	if err != nil {
		return fmt.Errorf("clean_key_task.inactive_duration=%s is invalid %w", rawInactiveDuration, err)
	}
	config.CleanKeyTask.InactiveDuration = duration
	return nil
}

type CoordinatorConfig struct {
	Name  string   `yaml:"name"`
	Addrs []string `yaml:"addrs"`
}

func (config CoordinatorConfig) check() error {
	if config.Name == "" {
		return errors.New("name should not be empty")
	}
	if len(config.Addrs) == 0 {
		return errors.New("addrs should not be empty")
	}
	return nil
}

type SyncKeyTaskConfig struct {
	IntervalMinutes    int  `yaml:"interval_minutes"`
	Off                bool `yaml:"off"`
	UpSertTryTimes     int  `yaml:"upsert_try_times"`
	RateLimitPerSecond int  `yaml:"rate_limit_per_second"`

	RawNoWrittenDuration string `yaml:"no_written_duration"`
	NoWrittenDuration    time.Duration
}

func (config SyncKeyTaskConfig) check() error {
	if config.IntervalMinutes <= 0 {
		return fmt.Errorf("interval_minutes is %d, it should be greater than 0", config.IntervalMinutes)
	}
	if config.UpSertTryTimes <= 0 {
		return fmt.Errorf("upsert_try_times is %d, it should be greater than 0", config.UpSertTryTimes)
	}
	if config.RateLimitPerSecond <= 0 {
		return fmt.Errorf("rate_limit_per_second is %d, it should be greater than 0", config.RateLimitPerSecond)
	}
	if config.RawNoWrittenDuration == "" {
		return fmt.Errorf("no_written_duration should not be empty")
	}
	return nil
}

type CleanKeyTaskConfig struct {
	IntervalMinutes    int  `yaml:"interval_minutes"`
	Off                bool `yaml:"off"`
	RateLimitPerSecond int  `yaml:"rate_limit_per_second"`

	RawInactiveDuration string `yaml:"inactive_duration"`
	InactiveDuration    time.Duration
}

func (config CleanKeyTaskConfig) check() error {
	if config.IntervalMinutes <= 0 {
		return fmt.Errorf("interval_minutes=%d, it should be greater than 0", config.IntervalMinutes)
	}
	if config.RateLimitPerSecond <= 0 {
		return fmt.Errorf("rate_limit_per_second is %d, it should be greater than 0", config.RateLimitPerSecond)
	}
	if config.RawInactiveDuration == "" {
		return errors.New("inactive_duration should not be empty")
	}
	return nil
}
