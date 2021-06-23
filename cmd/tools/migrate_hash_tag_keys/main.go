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
	"time"

	"github.com/go-pg/pg/v10"
	"github.com/spf13/pflag"
)

type roomHashTagKeys struct {
	tableName struct{} `pg:"_"`

	HashTag    string                    `pg:"hash_tag,pk"`
	Keys       []string                  `pg:"keys"`
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

type roomAccessedRecordModelV2 struct {
	tableName struct{} `pg:"_"`

	HashTag    string    `pg:"hash_tag,pk"`
	AccessedAt time.Time `pg:"accessed_at"`
	CreatedAt  time.Time `pg:"created_at"`
}

func (model *roomAccessedRecordModelV2) ShardingKey() string {
	return model.HashTag
}

func (model *roomAccessedRecordModelV2) GetTablePrefix() string {
	return "room_accessed_record_v2"
}

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
	if err := base.InitSyncService(*configPath); err != nil {
		logger.Fatalf("init service error %s\n", err)
	}
	dep := base.GetTaskDependency()
	roomDataTableShardingCount := dep.DB.GetShardingCount()
	roomDataTablePrefix := (&roomDataModelV2{}).GetTablePrefix()
	count := 100
	totalProcessCount := 0
	totalErrorCount := 0
	logger.Printf(
		"start to process at %s, table_sharding_count=%d, table_prefix=%s\n",
		startTime, roomDataTableShardingCount, roomDataTablePrefix)
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
				err := processModel(logger, dep.DB, dep.AccessedRecordDB, roomDataModel, *dryRun)
				if err != nil {
					logger.Printf("process error %s hash_tag %s\n", err, roomDataModel.HashTag)
					errorCount++
				} else {
					logger.Printf("process success hash_tag %s\n", roomDataModel.HashTag)
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

func processModel(logger *log.Logger, db, accessedRecordDB *base.DBCluster, roomDataModel *roomDataModelV2, dryRun bool) error {
	accessRecordModel, err := loadAccessedRecordModelByID(accessedRecordDB, roomDataModel.HashTag)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(roomDataModel.Value))
	for key := range roomDataModel.Value {
		keys = append(keys, key)
	}
	if accessRecordModel == nil || accessRecordModel.AccessedAt.IsZero() {
		logger.Printf("skip hash_tag %s, accessed record not found\n", roomDataModel.HashTag)
		return nil
	}
	accessedAt := accessRecordModel.AccessedAt

	if !dryRun {
		err = upsertHashTagKeysModel(logger, db, roomDataModel.HashTag, keys, accessedAt)
	}
	return err
}

func loadAccessedRecordModelByID(db *base.DBCluster, hashTag string) (*roomAccessedRecordModelV2, error) {
	model := &roomAccessedRecordModelV2{HashTag: hashTag}
	query, err := db.Model(model)
	if err != nil {
		return nil, err
	}
	err = query.WherePK().Select()
	if err != nil {
		if errors.Is(err, pg.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return model, nil
}

func upsertHashTagKeysModel(logger *log.Logger, dbCluster *base.DBCluster, hashTag string, keys []string, t time.Time) error {
	retryTimes := 10
	for i := 0; i < retryTimes; i++ {
		err := _upsertHashTagKeysModel(logger, dbCluster, hashTag, keys, t)
		if err != nil {
			if isRetryError(err) {
				logger.Printf("retry upsert hash_tag %s, error %s\n", hashTag, err.Error())
				time.Sleep(50 * time.Millisecond)
				continue
			}
			return err
		}
		logger.Printf("process success no dry run hash_tag %s\n", hashTag)
		break
	}
	return nil
}

func isRetryError(err error) bool {
	return errors.Is(err, errNoRowsUpdated) || errors.Is(err, pg.ErrTxDone)
}

func _upsertHashTagKeysModel(logger *log.Logger, dbCluster *base.DBCluster, hashTag string, keys []string, t time.Time) error {
	currentTime := time.Now()
	model := &roomHashTagKeys{HashTag: hashTag}
	tableName, db, err := dbCluster.GetTableNameAndDBClientByModel(model)
	if err != nil {
		return err
	}
	err = db.RunInTransaction(context.TODO(), func(tx *pg.Tx) error {
		err := tx.Model(model).Table(tableName).WherePK().For("UPDATE").Select()
		if err != nil && !errors.Is(err, pg.ErrNoRows) {
			return err
		}
		// Insert new row
		if err != nil && errors.Is(err, pg.ErrNoRows) {
			model = &roomHashTagKeys{
				HashTag:    hashTag,
				Keys:       keys,
				AccessedAt: t,
				WrittenAt:  t,
				CreatedAt:  currentTime,
				UpdatedAt:  currentTime,
				SyncedAt:   t,
				Status:     service.HashTagKeysStatusSynced,
				Version:    0,
			}
			_, err = tx.Model(model).Table(tableName).Insert()
			if err != nil {
				return err
			}
			logger.Printf("insert hash_tag_keys record hash_tag %s\n", hashTag)
			return err
		}
		// update
		originKeys := model.Keys
		model.Keys = utility.MergeStringSliceAndRemoveDuplicateItems(originKeys, keys)
		model.UpdatedAt = currentTime
		originVersion := model.Version
		model.Version = originVersion + 1
		if model.WrittenAt.IsZero() {
			logger.Printf("update hash_tag_keys record hash_tag %s written_at %s\n", hashTag, t)
			model.WrittenAt = t
		}
		model.AccessedAt = utility.GetLatestTime(model.AccessedAt, t)
		model.Status = service.HashTagKeysStatusNeedSynced
		result, err := tx.Model(model).Table(tableName).WherePK().Where("version=?", originVersion).Update()
		if err != nil {
			return err
		}
		if result.RowsAffected() != 1 {
			return errNoRowsUpdated
		}
		logger.Printf("update hash_tag_keys success hash_tag %s\n", hashTag)
		return nil
	})
	return err
}
