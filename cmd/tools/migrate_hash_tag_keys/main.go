package main

import (
	"bytepower_room/base"
	"bytepower_room/service"
	"bytepower_room/utility"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/go-pg/pg/v10"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/pflag"
)

type roomHashTagKeys struct {
	tableName struct{} `pg:"_"`

	HashTag    string                    `pg:"hash_tag,pk"`
	Keys       []string                  `pg:"keys,array"`
	AccessedAt time.Time                 `pg:"accessed_at"`
	WrittenAt  time.Time                 `pg:"written_at"`
	SyncedAt   time.Time                 `pg:"synced_at"`
	CreatedAt  time.Time                 `pg:"created_at"`
	UpdatedAt  time.Time                 `pg:"updated_at"`
	Status     service.HashTagKeysStatus `pg:"status"`
	Version    int64                     `pg:"version"`
}

func (model *roomHashTagKeys) ShardingKey() string {
	return model.HashTag
}

func (model *roomHashTagKeys) GetTablePrefix() string {
	return "room_hash_tag_keys"
}

type RedisValue struct {
	Type     string `json:"type"`
	Value    string `json:"value"`
	ExpireTs int64  `json:"expire_ts"`
}

func (v RedisValue) String() string {
	return fmt.Sprintf(
		"[RedisValue:type=%s,value=%s,expire_ts=%d]",
		v.Type, v.Value, v.ExpireTs)
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

var errNoRowsUpdated = errors.New("no rows updated")

func parseAndCheckCommandOptions() error {
	pflag.Parse()
	if configPath == nil || *configPath == "" {
		return errors.New("config is not set")
	}
	if dryRun == nil {
		return errors.New("dry run is not set")
	}
	return nil
}

var configPath = pflag.StringP("config", "c", "config.yaml", "config file path")
var dryRun = pflag.BoolP("dryrun", "d", true, "dry run")

func main() {
	logger := log.New(os.Stdout, "", log.LstdFlags)
	startTime := time.Now()
	if err := parseAndCheckCommandOptions(); err != nil {
		logger.Fatalf("command options error %s\n", err)
	}
	if err := base.InitBasicDependencies(*configPath); err != nil {
		logger.Fatalf("init service error %s\n", err)
	}
	dep := base.GetServerDependency()
	roomDataTableShardingCount := dep.DB.GetShardingCount()
	roomDataTablePrefix := (&roomDataModelV2{}).GetTablePrefix()
	count := 100
	totalProcessCount := 0
	totalErrorCount := 0
	logger.Printf(
		"start to process at %s, dry_run=%t, table_sharding_count=%d, table_prefix=%s\n",
		startTime, *dryRun, roomDataTableShardingCount, roomDataTablePrefix)
	for index := 0; index < roomDataTableShardingCount; index++ {
		startID := ""
		errorCount := 0
		processCount := 0
		for {
			roomDataModels, id, err := loadRoomDataModels(dep.DB, roomDataTablePrefix, index, startID, count)
			if err != nil {
				logger.Fatalf("load room data models error %s\n", err)
			}
			for _, roomDataModel := range roomDataModels {
				realUpdated, isInsert, err := processModel(logger, dep.DB, dep.Redis, roomDataModel, *dryRun)
				if err != nil {
					logger.Printf("process error %s hash_tag %s\n", err, roomDataModel.HashTag)
					errorCount++
				} else {
					logger.Printf("process success hash_tag %s, realUpdated=%t, isInsert=%t\n", roomDataModel.HashTag, realUpdated, isInsert)
					processCount++
				}
			}
			startID = id
			if len(roomDataModels) < count {
				break
			}
		}
		logger.Printf("process success, count %d error count %d, table_index %d\n", processCount, errorCount, index)
		totalProcessCount += processCount
		totalErrorCount += errorCount
	}
	logger.Printf("process success, count %d error count %d duration %s\n", totalProcessCount, totalErrorCount, time.Since(startTime))
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

func processModel(logger *log.Logger, db *base.DBCluster, redisClient *redis.ClusterClient, roomDataModel *roomDataModelV2, dryRun bool) (bool, bool, error) {
	isInsert := false
	realUpdated := false
	keys := make([]string, 0, len(roomDataModel.Value))
	for key := range roomDataModel.Value {
		keys = append(keys, key)
	}
	metaKey := fmt.Sprintf("{%s}:_m", roomDataModel.HashTag)
	result, err := redisClient.HMGet(context.TODO(), metaKey, "at", "wt").Result()
	if err != nil {
		return realUpdated, isInsert, err
	}
	var accessedAt time.Time
	var writtenAt time.Time
	switch at := result[0].(type) {
	case string:
		accessedAt, err = convertMSStringToTime(at)
		if err != nil {
			return realUpdated, isInsert, err
		}
		accessedAt = accessedAt.Add(1 * time.Second)
	case nil:
		logger.Printf("accessed_at not found in meta error:%s\n", metaKey)
		accessedAt = time.Now()
	}
	switch wt := result[1].(type) {
	case string:
		writtenAt, err = convertMSStringToTime(wt)
		if err != nil {
			return realUpdated, isInsert, err
		}
		writtenAt = writtenAt.Add(1 * time.Second)
	case nil:
		logger.Printf("written_at not found in meta error:%s\n", metaKey)
		writtenAt = accessedAt
	}

	if !dryRun {
		realUpdated, isInsert, err = upsertHashTagKeysModel(logger, db, roomDataModel.HashTag, keys, accessedAt, writtenAt)
	}
	return realUpdated, isInsert, err
}

func convertMSStringToTime(s string) (time.Time, error) {
	ts, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	seconds, nanoSeconds := utility.GetSecondsAndNanoSecondsFromTsInMs(ts)
	return time.Unix(seconds, nanoSeconds), nil
}

func upsertHashTagKeysModel(logger *log.Logger, dbCluster *base.DBCluster, hashTag string, keys []string, accessedAt time.Time, writtenAt time.Time) (bool, bool, error) {
	retryTimes := 10
	isInsert := false
	realUpdated := false
	for i := 0; i < retryTimes; i++ {
		updated, insert, err := _upsertHashTagKeysModel(logger, dbCluster, hashTag, keys, accessedAt, writtenAt)
		if err != nil {
			if isRetryError(err) {
				logger.Printf("retry upsert hash_tag %s, error %s\n", hashTag, err.Error())
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return realUpdated, isInsert, err
		}
		isInsert = insert
		realUpdated = updated
		break
	}
	return realUpdated, isInsert, nil
}

func isRetryError(err error) bool {
	if errors.Is(err, errNoRowsUpdated) {
		return true
	}
	var pgErr pg.Error
	if errors.As(err, &pgErr) && pgErr.IntegrityViolation() {
		return true
	}
	if errors.Is(err, pg.ErrTxDone) {
		return true
	}
	return false
}

func _upsertHashTagKeysModel(logger *log.Logger, dbCluster *base.DBCluster, hashTag string, keys []string, accessedAt, writtenAt time.Time) (bool, bool, error) {
	currentTime := time.Now()
	isInsert := false
	model := &roomHashTagKeys{HashTag: hashTag}
	tableName, db, err := dbCluster.GetTableNameAndDBClientByModel(model)
	realUpdated := false
	if err != nil {
		return false, false, err
	}
	err = db.RunInTransaction(context.TODO(), func(tx *pg.Tx) error {
		err := tx.Model(model).Table(tableName).WherePK().Select()
		if err != nil && !errors.Is(err, pg.ErrNoRows) {
			return err
		}
		// Insert new row
		if err != nil && errors.Is(err, pg.ErrNoRows) {
			isInsert = true
			realUpdated = true
			model = &roomHashTagKeys{
				HashTag:    hashTag,
				Keys:       keys,
				AccessedAt: accessedAt,
				WrittenAt:  writtenAt,
				CreatedAt:  currentTime,
				UpdatedAt:  currentTime,
				Status:     service.HashTagKeysStatusNeedSynced,
				Version:    0,
			}
			_, err = tx.Model(model).Table(tableName).Insert()
			if err != nil {
				return err
			}
			return err
		}

		toBeUpdatedColumns := make([]string, 0)

		originKeys := model.Keys
		newKeys := utility.MergeStringSliceAndRemoveDuplicateItems(originKeys, keys)
		if len(originKeys) != len(newKeys) {
			model.Keys = newKeys
			toBeUpdatedColumns = append(toBeUpdatedColumns, "keys")
		}

		if model.AccessedAt.IsZero() {
			model.AccessedAt = accessedAt
			toBeUpdatedColumns = append(toBeUpdatedColumns, "accessed_at")
		}
		if model.WrittenAt.IsZero() {
			model.WrittenAt = writtenAt
			toBeUpdatedColumns = append(toBeUpdatedColumns, "written_at")
		}

		if len(toBeUpdatedColumns) == 0 && model.Status == service.HashTagKeysStatusNeedSynced {
			return nil
		}
		realUpdated = true

		originVersion := model.Version
		model.Status = service.HashTagKeysStatusNeedSynced
		model.Version = model.Version + 1
		model.UpdatedAt = currentTime
		toBeUpdatedColumns = append(toBeUpdatedColumns, "version", "status", "updated_at")

		query := tx.Model(model).Table(tableName)
		for _, column := range toBeUpdatedColumns {
			query.Column(column)
		}
		result, err := query.WherePK().Where("version=?", originVersion).Update()
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return errNoRowsUpdated
		}
		return nil
	})
	return realUpdated, isInsert, err
}
