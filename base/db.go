package base

import (
	"bytepower_room/base/log"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/go-pg/pg/v10"
	"github.com/go-pg/pg/v10/orm"
)

const (
	defaultDBConnMaxRetries = 5
)

type DBCluster struct {
	clients       []dbClient
	shardingCount int
}

type dbClient struct {
	startIndex int
	endIndex   int
	client     *pg.DB
}

type Model interface {
	ShardingKey() string
	GetTablePrefix() string
}

func NewDBClusterFromConfig(config DBClusterConfig, logger *log.Logger, metricClient *MetricClient) (*DBCluster, error) {
	shardingCount := config.ShardingCount
	if shardingCount <= 0 {
		return nil, errors.New("sharding_count should be greater than 0")
	}
	dbCluster := &DBCluster{shardingCount: shardingCount, clients: make([]dbClient, 0)}
	for _, cfg := range config.Shardings {
		client, err := newDBClient(cfg, logger, metricClient)
		if err != nil {
			return nil, err
		}
		dbCluster.clients = append(
			dbCluster.clients,
			dbClient{startIndex: cfg.StartShardingIndex, endIndex: cfg.EndShardingIndex, client: client})
	}
	return dbCluster, nil
}

func newDBClient(config DBConfig, logger *log.Logger, metricClient *MetricClient) (*pg.DB, error) {
	opt, err := initDBOption(config)
	if err != nil {
		return nil, err
	}
	client := pg.Connect(opt)
	client.AddQueryHook(dbLogger{logger: logger, metricClient: metricClient})
	logger.Info("initialize db client", log.String("options", fmt.Sprintf("%+v", *opt)))
	return client, nil
}

func initDBOption(config DBConfig) (*pg.Options, error) {
	if err := config.check(); err != nil {
		return nil, err
	}
	opt, err := pg.ParseURL(config.URL)
	if err != nil {
		return nil, err
	}

	opt.ReadTimeout = time.Duration(config.Connection.ReadTimeoutMS) * time.Millisecond
	opt.WriteTimeout = time.Duration(config.Connection.WriteTimeoutMS) * time.Millisecond
	opt.DialTimeout = time.Duration(config.Connection.DialTimeoutMS) * time.Millisecond
	opt.MinIdleConns = config.Connection.MinIdleConns
	opt.PoolTimeout = time.Duration(config.Connection.PoolTimeoutMS) * time.Millisecond
	opt.PoolSize = config.Connection.PoolSize
	opt.MaxRetries = config.Connection.MaxRetries
	opt.MaxConnAge = time.Duration(config.Connection.MaxConnAgeSeconds) * time.Second

	if config.Connection.IdleTimeoutSecond == -1 {
		opt.IdleTimeout = -1
	} else {
		opt.IdleTimeout = time.Duration(config.Connection.IdleTimeoutSecond) * time.Second
	}
	if config.Connection.MinRetryBackoffMS == -1 {
		opt.MinRetryBackoff = -1
	} else {
		opt.MinRetryBackoff = time.Duration(config.Connection.MinRetryBackoffMS) * time.Millisecond
	}
	if config.Connection.MaxRetryBackoffMS == -1 {
		opt.MaxRetryBackoff = -1
	} else {
		opt.MaxRetryBackoff = time.Duration(config.Connection.MaxRetryBackoffMS) * time.Millisecond
	}
	if config.Connection.IdleCheckFrequencySeconds == -1 {
		opt.IdleCheckFrequency = -1
	} else {
		opt.IdleCheckFrequency = time.Duration(config.Connection.IdleCheckFrequencySeconds) * time.Second
	}
	return opt, nil
}

func (dbCluster *DBCluster) Model(model Model) (*orm.Query, error) {
	tableName, client, err := dbCluster.GetTableNameAndDBClientByModel(model)
	if err != nil {
		return nil, err
	}
	return client.Model(model).Table(tableName), nil
}

func (dbCluster *DBCluster) Models(models interface{}, tablePrefix string, tableIndex int) (*orm.Query, error) {
	tableName := fmt.Sprintf("%s_%d", tablePrefix, tableIndex)
	client := dbCluster.getClientByIndex(tableIndex)
	if client == nil {
		return nil, errors.New("no db client found")
	}
	return client.Model(models).Table(tableName), nil
}

func (dbCluster *DBCluster) getClientByIndex(index int) *pg.DB {
	for _, client := range dbCluster.clients {
		if (client.startIndex <= index) && (index <= client.endIndex) {
			return client.client
		}
	}
	return nil
}

func (dbCluster *DBCluster) GetTableNameAndDBClientByModel(model Model) (string, *pg.DB, error) {
	shardingKey := model.ShardingKey()
	tableIndex := getTableIndex(shardingKey, dbCluster.shardingCount)
	client := dbCluster.getClientByIndex(tableIndex)
	if client == nil {
		return "", nil, errors.New("no db client found")
	}
	tableName := fmt.Sprintf("%s_%d", model.GetTablePrefix(), tableIndex)
	return tableName, client, nil
}

func (dbCluser *DBCluster) GetShardingCount() int {
	return dbCluser.shardingCount
}

func (dbCluster *DBCluster) GetShardingIndex(shardingKey string) int {
	return getTableIndex(shardingKey, dbCluster.shardingCount)
}

func getTableIndex(shardingKey string, shardingCount int) int {
	return int(crc32.ChecksumIEEE([]byte(shardingKey)) % uint32(shardingCount))
}

const (
	dbQueryStartTimeContextKey = "query_start_time"
	dbQueryDurationMetricKey   = "database.query.duration"
)

type dbLogger struct {
	logger       *log.Logger
	metricClient *MetricClient
}

func (d dbLogger) BeforeQuery(ctx context.Context, queryEvent *pg.QueryEvent) (context.Context, error) {
	return context.WithValue(ctx, dbQueryStartTimeContextKey, time.Now()), nil
}

func (d dbLogger) AfterQuery(ctx context.Context, queryEvent *pg.QueryEvent) error {
	query, err := queryEvent.FormattedQuery()
	if err != nil {
		d.logger.Error("dbLogger error", log.Error(err))
		return err
	}
	if startTime, ok := ctx.Value(dbQueryStartTimeContextKey).(time.Time); ok {
		duration := time.Since(startTime)
		d.logger.Debug(
			"execute database query",
			log.String("query", string(query)),
			log.String("duration", duration.String()),
		)
		d.metricClient.MetricTimeDuration(dbQueryDurationMetricKey, duration)
	}
	return nil
}
