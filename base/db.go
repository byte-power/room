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

func NewDBClusterFromConfig(config DBClusterConfig, logger *log.Logger) (*DBCluster, error) {
	shardingCount := config.ShardingCount
	if shardingCount <= 0 {
		return nil, errors.New("sharding_count should be greater than 0")
	}
	dbCluster := &DBCluster{shardingCount: shardingCount, clients: make([]dbClient, 0)}
	for _, cfg := range config.Shardings {
		client, err := newDBClient(cfg, logger)
		if err != nil {
			return nil, err
		}
		dbCluster.clients = append(
			dbCluster.clients,
			dbClient{startIndex: cfg.StartShardingIndex, endIndex: cfg.EndShardingIndex, client: client})
	}
	return dbCluster, nil
}

func newDBClient(config DBConfig, logger *log.Logger) (*pg.DB, error) {
	if err := config.check(); err != nil {
		return nil, err
	}
	opt, err := pg.ParseURL(config.URL)
	if err != nil {
		return nil, err
	}
	opt.MaxConnAge = time.Second * time.Duration(config.ConnMaxLifetimeSeconds)
	opt.MinIdleConns = config.MinIdleConns
	opt.PoolSize = config.MaxOpenConns
	if config.MaxRetries == 0 {
		opt.MaxRetries = defaultDBConnMaxRetries
	} else {
		opt.MaxRetries = config.MaxRetries
	}
	client := pg.Connect(opt)
	client.AddQueryHook(dbLogger{logger: logger})
	return client, nil
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

type dbLogger struct {
	logger *log.Logger
}

func (d dbLogger) BeforeQuery(c context.Context, q *pg.QueryEvent) (context.Context, error) {
	return c, nil
}

func (d dbLogger) AfterQuery(c context.Context, q *pg.QueryEvent) error {
	query, err := q.FormattedQuery()
	if err != nil {
		return err
	}
	d.logger.Debug(string(query))
	return nil
}
