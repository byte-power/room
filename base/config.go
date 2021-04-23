package base

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"gopkg.in/yaml.v2"
)

type Config struct {
	Name         string                            `yaml:"name"`
	Server       RoomServerConfig                  `yaml:"room_server"`
	RedisCluster RedisClusterConfig                `yaml:"redis_cluster"`
	DBCluster    DBClusterConfig                   `yaml:"db_cluster"`
	EventService EventServiceConfig                `yaml:"event_service"`
	Metric       MetricConfig                      `yaml:"metric"`
	Log          map[string]map[string]interface{} `yaml:"log"`
	LoadKey      LoadKeyConfig                     `yaml:"load_key"`
	SyncService  SyncServiceConfig                 `yaml:"sync"`
}

func (config Config) check() error {
	if config.Name == "" {
		return errors.New("config.name should not be empty")
	}
	if err := config.Server.check(); err != nil {
		return fmt.Errorf("config.%w", err)
	}
	if err := config.RedisCluster.check(); err != nil {
		return fmt.Errorf("config.%w", err)
	}
	if err := config.DBCluster.check(); err != nil {
		return fmt.Errorf("config.%w", err)
	}
	if err := config.Metric.check(); err != nil {
		return fmt.Errorf("config.%w", err)
	}
	if len(config.Log) == 0 {
		return errors.New("config.log should not be empty.")
	}
	if err := config.LoadKey.check(); err != nil {
		return fmt.Errorf("config.%w", err)
	}
	if err := config.SyncService.check(); err != nil {
		return fmt.Errorf("config.%w", err)
	}
	return nil
}

type RoomServerConfig struct {
	URL      string `yaml:"url"`
	PProfURL string `yaml:"pprof_url"`
}

func (config RoomServerConfig) check() error {
	if config.URL == "" {
		return errors.New("room_service.url should not be empty")
	}
	return nil
}

type connectionConfig struct {
	PoolSize int `yaml:"pool_size"`

	DialTimeoutMS     int `yaml:"dial_timeout_ms"`
	ReadTimeoutMS     int `yaml:"read_timeout_ms"`
	WriteTimeoutMS    int `yaml:"write_timeout_ms"`
	IdleTimeoutSecond int `yaml:"idle_timeout_second"`
	PoolTimeoutMS     int `yaml:"pool_timeout_ms"`

	MaxRetries                int `yaml:"max_retries"`
	MaxConnAgeSeconds         int `yaml:"max_conn_age_second"`
	MinIdleConns              int `yaml:"min_idle_conns"`
	MinRetryBackoffMS         int `yaml:"min_retry_backoff_ms"`
	MaxRetryBackoffMS         int `yaml:"max_retry_backoff_ms"`
	IdleCheckFrequencySeconds int `yaml:"idle_check_frequency_second"`
}

func (config connectionConfig) check() error {
	if v := config.PoolSize; v <= 0 {
		return fmt.Errorf("pool_size=%d, it should be > 0", v)
	}
	if v := config.DialTimeoutMS; v < 0 {
		return fmt.Errorf("dial_timeout_ms=%d, it should be >= 0", v)
	}
	if v := config.ReadTimeoutMS; v < 0 {
		return fmt.Errorf("read_timeout_ms=%d, it should be >= 0", v)
	}
	if v := config.WriteTimeoutMS; v < 0 {
		return fmt.Errorf("write_timeout_ms=%d, it should be >= 0", v)
	}
	if v := config.IdleTimeoutSecond; v < -1 {
		return fmt.Errorf("idle_timeout_second=%d, it should be >= -1", v)
	}
	if v := config.PoolTimeoutMS; v < 0 {
		return fmt.Errorf("pool_timeout_ms=%d, it should be >= 0", v)
	}
	if v := config.MaxRetries; v < 0 {
		return fmt.Errorf("max_retries=%d, it should be >= 0", v)
	}
	if v := config.MaxConnAgeSeconds; v < 0 {
		return fmt.Errorf("max_conn_age_second=%d, it should be >= 0", v)
	}
	if v := config.MinIdleConns; v < 0 {
		return fmt.Errorf("min_idle_conns=%d, it should be >= 0", v)
	}
	if v := config.MinRetryBackoffMS; v < -1 {
		return fmt.Errorf("min_retry_backoff_ms=%d, it should be >= -1", v)
	}
	if v := config.MaxRetryBackoffMS; v < -1 {
		return fmt.Errorf("max_retry_backoff_ms=%d, it should be >= -1", v)
	}
	if config.MinRetryBackoffMS > config.MaxRetryBackoffMS {
		return fmt.Errorf(
			"min_retry_backoff_ms=%d, max_retry_backoff_ms=%d, min_retry_backoff_ms shoule be less than or equal to max_retry_backoff_ms",
			config.MinRetryBackoffMS, config.MaxRetryBackoffMS)
	}
	if v := config.IdleCheckFrequencySeconds; v < -1 {
		return fmt.Errorf("idle_check_frequency_seconds=%d, it should be >= -1", v)
	}
	return nil
}

type RedisClusterConfig struct {
	Addrs      []string         `yaml:"addrs"`
	Connection connectionConfig `yaml:",inline"`
}

func (config RedisClusterConfig) check() error {
	if len(config.Addrs) == 0 {
		return fmt.Errorf("redis_cluster.adds should not be empty")
	}
	for _, addr := range config.Addrs {
		if addr == "" {
			return fmt.Errorf("address in redis_cluster.adds should not be empty")
		}
	}
	if err := config.Connection.check(); err != nil {
		return fmt.Errorf("redis_cluster.%w", err)
	}

	return nil
}

type DBClusterConfig struct {
	ShardingCount int        `yaml:"sharding_count"`
	Shardings     []DBConfig `yaml:"shardings"`
}

func (config DBClusterConfig) check() error {
	if config.ShardingCount <= 0 {
		return errors.New("db_cluster.sharding_count should be greater than 0")
	}
	for _, sharding := range config.Shardings {
		if err := sharding.check(); err != nil {
			return fmt.Errorf("db_cluster.shardings.%w", err)
		}
	}
	return nil
}

type DBConfig struct {
	URL string `yaml:"url"`

	Connection connectionConfig `yaml:",inline"`

	StartShardingIndex int `yaml:"start_index"`
	EndShardingIndex   int `yaml:"end_index"`
}

func (config DBConfig) check() error {
	if config.URL == "" {
		return errors.New("db_config.url should not be empty")
	}
	if err := config.Connection.check(); err != nil {
		return fmt.Errorf("db_config.%w", err)
	}
	if config.StartShardingIndex < 0 {
		return errors.New("db_config.start_index shoule be equal to or greater than 0")
	}
	if config.EndShardingIndex < 0 {
		return errors.New("db_config.end_index shoule be equal to or greater than 0")
	}
	if config.StartShardingIndex > config.EndShardingIndex {
		return errors.New("db_config.start_index should be equal to or less than end_index")
	}
	return nil
}

func NewConfigFromFile(filePath string) (Config, error) {
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

type MetricConfig struct {
	Prefix             string   `yaml:"prefix"`
	Host               string   `yaml:"host"`
	Network            string   `yaml:"network"`
	MaxPacktSize       int      `yaml:"max_packet_size"`
	FlushPeriodSeconds int64    `yaml:"flush_period_seconds"`
	SampleRate         float32  `yaml:"sample_rate"`
	Tags               []string `yaml:"tags"`
}

func (config MetricConfig) check() error {
	if config.Host == "" {
		return errors.New("metric.host should not be empty")
	}
	if len(config.Tags)%2 != 0 {
		return errors.New("metric.tags count should be even")
	}
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
		return fmt.Errorf("load_key.retry_times=%d, should be greater than 0", config.RetryTimes)
	}
	d, err := time.ParseDuration(config.RawRetryInterval)
	if err != nil {
		return fmt.Errorf("load_key.retry_interval=%s, should be in valid duration format", config.RawRetryInterval)
	}
	if d <= 0 {
		return fmt.Errorf("load_key.retry_interval=%s, duration should be positive", config.RawRetryInterval)
	}
	d, err = time.ParseDuration(config.RawLoadTimeout)
	if err != nil {
		return fmt.Errorf("load_key.load_timeout=%s, should be in valid duration format", config.RawLoadTimeout)
	}
	if d <= 0 {
		return fmt.Errorf("load_key.load_timeout=%s, duration should be positive", config.RawLoadTimeout)
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

type SyncServiceConfig struct {
	Metric                  MetricConfig         `yaml:"metric"`
	WrittenRecordDBCluster  DBClusterConfig      `yaml:"written_record_db_cluster"`
	AccessedRecordDBCluster DBClusterConfig      `yaml:"accessed_record_db_cluster"`
	SQS                     SQSConfig            `yaml:"sqs"`
	S3                      S3Config             `yaml:"s3"`
	Coordinator             CoordinatorConfig    `yaml:"coordinator"`
	SyncRecordTask          SyncRecordTaskConfig `yaml:"sync_record_task"`
	SyncKeyTask             SyncKeyTaskConfig    `yaml:"sync_key_task"`
	CleanKeyTask            CleanKeyTaskConfig   `yaml:"clean_key_task"`
}

func (config SyncServiceConfig) check() error {
	if err := config.Metric.check(); err != nil {
		return fmt.Errorf("sync.%w", err)
	}
	if err := config.WrittenRecordDBCluster.check(); err != nil {
		return fmt.Errorf("sync.%w", err)
	}
	if err := config.AccessedRecordDBCluster.check(); err != nil {
		return fmt.Errorf("sync.%w", err)
	}
	if err := config.SQS.check(); err != nil {
		return fmt.Errorf("sync.%w", err)
	}
	if err := config.S3.check(); err != nil {
		return fmt.Errorf("sync.%w", err)
	}
	if err := config.Coordinator.check(); err != nil {
		return fmt.Errorf("sync.%w", err)
	}
	if err := config.SyncRecordTask.check(); err != nil {
		return fmt.Errorf("sync.%w", err)
	}
	if err := config.SyncKeyTask.check(); err != nil {
		return fmt.Errorf("sync.%w", err)
	}
	if err := config.CleanKeyTask.check(); err != nil {
		return fmt.Errorf("sync.%w", err)
	}
	return nil
}

type SQSConfig struct {
	Queue                    string           `yaml:"queue"`
	MaxReceivedMessages      int64            `yaml:"max_received_messages"`
	VisibilityTimeoutSeconds int64            `yaml:"visibility_timeout_seconds"`
	WaitTimeSeconds          int64            `yaml:"wait_time_seconds"`
	Session                  AWSSessionConfig `yaml:",inline"`
}

func (config SQSConfig) check() error {
	if config.Queue == "" {
		return errors.New("sqs.queue should not be empty")
	}
	if config.MaxReceivedMessages <= 0 {
		return fmt.Errorf("sqs.max_received_messages=%d, should be greater than 0", config.MaxReceivedMessages)
	}
	if config.VisibilityTimeoutSeconds <= 0 {
		return fmt.Errorf("sqs.visibility_timeout_seconds=%d, should be greater than 0", config.VisibilityTimeoutSeconds)
	}
	if config.WaitTimeSeconds <= 0 {
		return fmt.Errorf("sqs.wait_time_seconds=%d, should be greater than 0", config.WaitTimeSeconds)
	}
	if err := config.Session.check(); err != nil {
		return fmt.Errorf("sqs.%w", err)
	}
	return nil
}

type AWSSessionConfig struct {
	Region          string `yaml:"region"`
	AccessKeyID     string `yaml:"aws_access_key_id"`
	SecretAccessKey string `yaml:"aws_secret_access_key"`
}

func (config AWSSessionConfig) check() error {
	if config.Region == "" {
		return errors.New("aws_session.region should not be empty")
	}
	if config.AccessKeyID == "" {
		return errors.New("aws_session.aws_access_key_id should not be empty")
	}
	if config.SecretAccessKey == "" {
		return errors.New("aws_session.aws_secret_access_key should not be empty")
	}
	return nil
}

func NewAWSSession(config AWSSessionConfig) (*session.Session, error) {
	return session.NewSession(&aws.Config{
		Region:      aws.String(config.Region),
		Credentials: credentials.NewStaticCredentials(config.AccessKeyID, config.SecretAccessKey, ""),
	})
}

type S3Config struct {
	Session AWSSessionConfig `yaml:",inline"`
}

func (config S3Config) check() error {
	if err := config.Session.check(); err != nil {
		return fmt.Errorf("s3.%w", err)
	}
	return nil
}

type CoordinatorConfig struct {
	Name  string   `yaml:"name"`
	Addrs []string `yaml:"addrs"`
}

func (config CoordinatorConfig) check() error {
	if config.Name == "" {
		return errors.New("coordinator.name should not be empty")
	}
	if len(config.Addrs) == 0 {
		return errors.New("coordinator.addrs should not be empty")
	}
	return nil
}

type SyncRecordTaskConfig struct {
	IntervalMinutes int  `yaml:"interval_minutes"`
	Off             bool `yaml:"off"`
}

func (config SyncRecordTaskConfig) check() error {
	if config.IntervalMinutes <= 0 {
		return fmt.Errorf("sync_record_task.interval_minutes is %d, it should be greater than 0", config.IntervalMinutes)
	}
	return nil
}

type SyncKeyTaskConfig struct {
	IntervalMinutes int  `yaml:"interval_minutes"`
	Off             bool `yaml:"off"`
}

func (config SyncKeyTaskConfig) check() error {
	if config.IntervalMinutes <= 0 {
		return fmt.Errorf("sync_key_task.interval_minutes is %d, it should be greater than 0", config.IntervalMinutes)
	}
	return nil
}

type CleanKeyTaskConfig struct {
	IntervalMinutes     int    `yaml:"interval_minutes"`
	RawInactiveDuration string `yaml:"inactive_duration"`
	Off                 bool   `yaml:"off"`
	InactiveDuration    time.Duration
}

func (config CleanKeyTaskConfig) check() error {
	if config.IntervalMinutes <= 0 {
		return fmt.Errorf("clean_key_task.interval_minutes=%d, it should be greater than 0", config.IntervalMinutes)
	}
	if config.RawInactiveDuration == "" {
		return errors.New("clean_key_task.inactive_duration should not be empty")
	}
	return nil
}
