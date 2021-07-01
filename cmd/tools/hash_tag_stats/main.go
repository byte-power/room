package main

import (
	"bytepower_room/base"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/go-pg/pg/v10"
	"github.com/spf13/pflag"
)

type RedisValue struct {
	Type     string `json:"type"`
	Value    string `json:"value"`
	SyncedTs int64  `json:"synced_ts"`
	ExpireTs int64  `json:"expire_ts"`
}

func (v RedisValue) String() string {
	return fmt.Sprintf(
		"[RedisValue:type=%s,value=%s,synced_ts=%d,expire_ts=%d]",
		v.Type, v.Value, v.SyncedTs, v.ExpireTs)
}

type roomDataModelV2 struct {
	tableName struct{} `pg:"_"`

	HashTag   string                `pg:"hash_tag,pk"`
	Value     map[string]RedisValue `pg:"value"`
	DeletedAt time.Time             `pg:"deleted_at"`
	CreatedAt time.Time             `pg:"created_at"`
	UpdatedAt time.Time             `pg:"updated_at"`
	Version   int                   `pg:"version"`
}

func (model *roomDataModelV2) ShardingKey() string {
	return model.HashTag
}

func (model *roomDataModelV2) GetTablePrefix() string {
	return "room_data_v2"
}

func parseAndCheckCommandOptions() error {
	pflag.Parse()
	if configPath == nil || *configPath == "" {
		return errors.New("config is not set")
	}
	return nil
}

var configPath = pflag.StringP("config", "c", "config.yaml", "config file path")

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	startTime := time.Now()
	if err := parseAndCheckCommandOptions(); err != nil {
		logger.Fatalf("command options error %s\n", err)
	}
	if err := base.InitBasicDependencies(*configPath); err != nil {
		logger.Fatalf("init service error %s\n", err)
	}
	db := base.GetDBCluster()
	roomDataTableShardingCount := db.GetShardingCount()
	roomDataTablePrefix := (&roomDataModelV2{}).GetTablePrefix()
	count := 100
	totalProcessCount := 0
	logger.Printf(
		"start to process at %s, table_sharding_count=%d, table_prefix=%s\n",
		startTime, roomDataTableShardingCount, roomDataTablePrefix)
	for index := 0; index < roomDataTableShardingCount; index++ {
		startID := ""
		processCount := 0
		for {
			roomDataModels, id, err := loadRoomDataModels(db, roomDataTablePrefix, index, startID, count)
			if err != nil {
				logger.Fatalf("load room data models error %s\n", err)
			}
			for _, roomDataModel := range roomDataModels {
				keys := make([]string, 0, len(roomDataModel.Value))
				for key := range roomDataModel.Value {
					keys = append(keys, key)
				}
				outPut := fmt.Sprintf(
					"HASHTAGINFO\t%s\t%d\t%d\t%s",
					roomDataModel.HashTag,
					len(roomDataModel.Value),
					roomDataModel.Version,
					strings.Join(keys, ","),
				)
				logger.Println(outPut)
				processCount++
			}
			startID = id
			if len(roomDataModels) < count {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		logger.Printf("process success, count %d, table_index %d\n", processCount, index)
		totalProcessCount += processCount
	}
	logger.Printf("process success, count %d duration %s\n", totalProcessCount, time.Since(startTime))
}

func loadRoomDataModels(db *base.DBCluster, tablePrefix string, tableIndex int, startID string, count int) ([]*roomDataModelV2, string, error) {
	var models []*roomDataModelV2
	query, err := db.Models(&models, tablePrefix, tableIndex)
	if err != nil {
		return nil, "", err
	}
	if startID != "" {
		query.Where("hash_tag>?", startID)
	}
	err = query.OrderExpr("hash_tag ASC").Limit(count).Select()
	if err != nil {
		if errors.Is(err, pg.ErrNoRows) {
			return nil, "", nil
		}
		return nil, "", err
	}
	if len(models) == 0 {
		return nil, "", nil
	}
	return models, models[len(models)-1].HashTag, nil
}
