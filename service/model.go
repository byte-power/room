package service

import (
	"bytepower_room/base"
	"errors"
	"time"

	"github.com/go-pg/pg/v10"
)

const (
	stringType = "string"
	listType   = "list"
	hashType   = "hash"
	setType    = "set"
	zsetType   = "zset"
)

type roomDataModel struct {
	tableName struct{} `pg:"_"`

	Key       string    `pg:"key,pk"`
	Type      string    `pg:"type"`
	Value     string    `pg:"value"`
	Deleted   bool      `pg:"deleted"`
	UpdatedAt time.Time `pg:"updated_at"`
	SyncedAt  time.Time `pg:"synced_at"`
	ExpireAt  time.Time `pg:"expire_at"`
	CreatedAt time.Time `pg:"created_at"`
	Version   int64     `pg:"version"`
}

func (model *roomDataModel) ShardingKey() string {
	return model.Key
}

func (model *roomDataModel) GetTablePrefix() string {
	return "room_data"
}

func (model *roomDataModel) IsExpired(t time.Time) bool {
	if model.ExpireAt.IsZero() {
		return false
	}
	return model.ExpireAt.Before(t)
}

func loadDataByKey(key string) (*roomDataModel, error) {
	dbCluster := base.GetDBCluster()
	model := &roomDataModel{Key: key}
	query, err := dbCluster.Model(model)
	if err != nil {
		return nil, err
	}
	if err := query.WherePK().Where("deleted != ?", true).Select(); err != nil {
		if errors.Is(err, pg.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return model, nil
}

type clusterRoomDataModel struct {
	tableName string
	client    *pg.DB
	models    []*roomDataModel
}

func getClusterModelsFromRoomDataModels(models ...*roomDataModel) ([]clusterRoomDataModel, error) {
	db := base.GetDBCluster()
	clusterModelsMap := make(map[string]clusterRoomDataModel)
	for _, model := range models {
		tableName, client, err := db.GetTableNameAndDBClientByModel(model)
		if err != nil {
			return nil, err
		}
		if origin, ok := clusterModelsMap[tableName]; ok {
			origin.models = append(origin.models, model)
			clusterModelsMap[tableName] = origin
		} else {
			clusterModelsMap[tableName] = clusterRoomDataModel{tableName: tableName, client: client, models: []*roomDataModel{model}}
		}
	}
	clusterModels := make([]clusterRoomDataModel, 0, len(clusterModelsMap))
	for _, clusterModel := range clusterModelsMap {
		clusterModels = append(clusterModels, clusterModel)
	}
	return clusterModels, nil
}

type roomWrittenRecordModel struct {
	tableName struct{} `pg:"_"`

	Key       string    `pg:"key,pk"`
	WrittenAt time.Time `pg:"written_at"`
	CreatedAt time.Time `pg:"created_at"`
}

func (model *roomWrittenRecordModel) ShardingKey() string {
	return model.Key
}

func (model *roomWrittenRecordModel) GetTablePrefix() string {
	return "room_written_record"
}

func loadWrittenRecordModels(count int) ([]*roomWrittenRecordModel, error) {
	db := base.GetWrittenRecordDBCluster()
	shardingCount := db.GetShardingCount()
	tablePrefix := (&roomWrittenRecordModel{}).GetTablePrefix()
	var models []*roomWrittenRecordModel
	for index := 0; index < shardingCount; index++ {
		query, err := db.Models(&models, tablePrefix, index)
		if err != nil {
			return nil, err
		}
		if err := query.Limit(count).Select(); err != nil {
			if errors.Is(err, pg.ErrNoRows) {
				continue
			}
			return nil, err
		}
		if len(models) > 0 {
			return models, nil
		}
	}
	return nil, nil
}

func bulkUpsertWrittenRecordModels(models ...*roomWrittenRecordModel) error {
	clusterModels, err := getClusterModelsFromWrittenRecordModels(models...)
	if err != nil {
		return err
	}
	for _, clusterModel := range clusterModels {
		_, err := clusterModel.client.Model(&clusterModel.models).
			Table(clusterModel.tableName).
			OnConflict("(key) DO UPDATE").
			Set("written_at=EXCLUDED.written_at").
			Where("room_written_record_model.written_at<EXCLUDED.written_at").
			Insert()
		if err != nil {
			return err
		}
	}
	return nil
}

type clusterWrittenRecordModel struct {
	tableName string
	client    *pg.DB
	models    []*roomWrittenRecordModel
}

func getClusterModelsFromWrittenRecordModels(models ...*roomWrittenRecordModel) ([]clusterWrittenRecordModel, error) {
	db := base.GetWrittenRecordDBCluster()
	clusterModelsMap := make(map[string]clusterWrittenRecordModel)
	for _, model := range models {
		tableName, client, err := db.GetTableNameAndDBClientByModel(model)
		if err != nil {
			return nil, err
		}
		if origin, ok := clusterModelsMap[tableName]; ok {
			origin.models = append(origin.models, model)
			clusterModelsMap[tableName] = origin
		} else {
			clusterModelsMap[tableName] = clusterWrittenRecordModel{tableName: tableName, client: client, models: []*roomWrittenRecordModel{model}}
		}
	}
	clusterModels := make([]clusterWrittenRecordModel, 0, len(clusterModelsMap))
	for _, clusterModel := range clusterModelsMap {
		clusterModels = append(clusterModels, clusterModel)
	}
	return clusterModels, nil
}

type roomAccessedRecordModel struct {
	tableName struct{} `pg:"_"`

	Key        string    `pg:"key,pk"`
	AccessedAt time.Time `pg:"accessed_at"`
	CreatedAt  time.Time `pg:"created_at"`
}

func (model *roomAccessedRecordModel) ShardingKey() string {
	return model.Key
}

func (model *roomAccessedRecordModel) GetTablePrefix() string {
	return "room_accessed_record"
}

func loadAccessedRecordModels(count int, t time.Time, excludedKeys []string) ([]*roomAccessedRecordModel, error) {
	db := base.GetAccessedRecordDBCluster()
	shardingCount := db.GetShardingCount()
	tablePrefix := (&roomAccessedRecordModel{}).GetTablePrefix()
	excludedKeysInSharding := make(map[int][]string)
	for _, key := range excludedKeys {
		index := db.GetShardingIndex(key)
		if keys, ok := excludedKeysInSharding[index]; !ok {
			excludedKeysInSharding[index] = []string{key}
		} else {
			excludedKeysInSharding[index] = append(keys, key)
		}
	}
	var models []*roomAccessedRecordModel
	for index := 0; index < shardingCount; index++ {
		query, err := db.Models(&models, tablePrefix, index)
		if err != nil {
			return nil, err
		}
		if keys, ok := excludedKeysInSharding[index]; ok {
			query = query.Where("key not in (?)", pg.In(keys))
		}
		if err := query.Where("accessed_at<?", t).Limit(count).Select(); err != nil {
			if errors.Is(err, pg.ErrNoRows) {
				continue
			}
			return nil, err
		}
		if len(models) > 0 {
			return models, nil
		}
	}
	return nil, nil
}

func bulkUpsertAccessedRecordModels(models ...*roomAccessedRecordModel) error {
	clusterModels, err := getClusterModelsFromAccessedRecordModels(models...)
	if err != nil {
		return err
	}
	for _, clusterModel := range clusterModels {
		_, err := clusterModel.client.Model(&clusterModel.models).
			Table(clusterModel.tableName).
			OnConflict("(key) DO UPDATE").
			Set("accessed_at=EXCLUDED.accessed_at").
			Where("room_accessed_record_model.accessed_at<EXCLUDED.accessed_at").
			Insert()
		if err != nil {
			return err
		}
	}
	return nil
}

type clusterAccessedRecordModel struct {
	tableName string
	client    *pg.DB
	models    []*roomAccessedRecordModel
}

func getClusterModelsFromAccessedRecordModels(models ...*roomAccessedRecordModel) ([]clusterAccessedRecordModel, error) {
	db := base.GetAccessedRecordDBCluster()
	clusterModelsMap := make(map[string]clusterAccessedRecordModel)
	for _, model := range models {
		tableName, client, err := db.GetTableNameAndDBClientByModel(model)
		if err != nil {
			return nil, err
		}
		if origin, ok := clusterModelsMap[tableName]; ok {
			origin.models = append(origin.models, model)
			clusterModelsMap[tableName] = origin
		} else {
			clusterModelsMap[tableName] = clusterAccessedRecordModel{tableName: tableName, client: client, models: []*roomAccessedRecordModel{model}}
		}
	}
	clusterModels := make([]clusterAccessedRecordModel, 0, len(clusterModelsMap))
	for _, clusterModel := range clusterModelsMap {
		clusterModels = append(clusterModels, clusterModel)
	}
	return clusterModels, nil
}
