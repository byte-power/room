package service

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"context"
	"errors"
	"fmt"
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
	Value     string    `pg:"value,use_zero"`
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
	logger := base.GetServerLogger()
	dbCluster := base.GetDBCluster()
	model := &roomDataModel{Key: key}
	query, err := dbCluster.Model(model)
	if err != nil {
		return nil, err
	}
	startTime := time.Now()
	if err := query.WherePK().Where("deleted != ?", true).Select(); err != nil {
		if errors.Is(err, pg.ErrNoRows) {
			logger.Info(
				"query database",
				log.String("key", key),
				log.String("duration", time.Since(startTime).String()))
			return nil, nil
		}
		return nil, err
	}
	logger.Info(
		"query database",
		log.String("key", key),
		log.String("duration", time.Since(startTime).String()))
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

func deleteRoomWrittenRecordModel(db *base.DBCluster, key string, writtenAt time.Time) error {
	model := &roomWrittenRecordModel{Key: key}
	query, err := db.Model(model)
	if err != nil {
		return err
	}
	_, err = query.WherePK().Where("written_at=?", writtenAt).Delete()
	return err
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

func loadAccessedRecordModels(count int, t time.Time, excludedHashTags []string) ([]*roomAccessedRecordModelV2, error) {
	db := base.GetAccessedRecordDBCluster()
	shardingCount := db.GetShardingCount()
	tablePrefix := (&roomAccessedRecordModelV2{}).GetTablePrefix()
	excludedKeysInSharding := make(map[int][]string)
	for _, hashTag := range excludedHashTags {
		index := db.GetShardingIndex(hashTag)
		if hashTags, ok := excludedKeysInSharding[index]; !ok {
			excludedKeysInSharding[index] = []string{hashTag}
		} else {
			excludedKeysInSharding[index] = append(hashTags, hashTag)
		}
	}
	var models []*roomAccessedRecordModelV2
	for index := 0; index < shardingCount; index++ {
		query, err := db.Models(&models, tablePrefix, index)
		if err != nil {
			return nil, err
		}
		if hashTags, ok := excludedKeysInSharding[index]; ok {
			query = query.Where("hash_tag not in (?)", pg.In(hashTags))
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

func bulkUpsertAccessedRecordModelsV2(models ...*roomAccessedRecordModelV2) error {
	clusterModels, err := getClusterModelsFromAccessedRecordModelsV2(models...)
	if err != nil {
		return err
	}
	for _, clusterModel := range clusterModels {
		_, err := clusterModel.client.Model(&clusterModel.models).
			Table(clusterModel.tableName).
			OnConflict("(hash_tag) DO UPDATE").
			Set("accessed_at=EXCLUDED.accessed_at").
			Where("room_accessed_record_model_v2.accessed_at<EXCLUDED.accessed_at").
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

func getClusterModelsFromAccessedRecordModels(models ...*roomAccessedRecordModelV2) ([]clusterAccessedRecordModelV2, error) {
	db := base.GetAccessedRecordDBCluster()
	clusterModelsMap := make(map[string]clusterAccessedRecordModelV2)
	for _, model := range models {
		tableName, client, err := db.GetTableNameAndDBClientByModel(model)
		if err != nil {
			return nil, err
		}
		if origin, ok := clusterModelsMap[tableName]; ok {
			origin.models = append(origin.models, model)
			clusterModelsMap[tableName] = origin
		} else {
			clusterModelsMap[tableName] = clusterAccessedRecordModelV2{tableName: tableName, client: client, models: []*roomAccessedRecordModelV2{model}}
		}
	}
	clusterModels := make([]clusterAccessedRecordModelV2, 0, len(clusterModelsMap))
	for _, clusterModel := range clusterModelsMap {
		clusterModels = append(clusterModels, clusterModel)
	}
	return clusterModels, nil
}

type clusterAccessedRecordModelV2 struct {
	tableName string
	client    *pg.DB
	models    []*roomAccessedRecordModelV2
}

func getClusterModelsFromAccessedRecordModelsV2(models ...*roomAccessedRecordModelV2) ([]clusterAccessedRecordModelV2, error) {
	db := base.GetAccessedRecordDBCluster()
	clusterModelsMap := make(map[string]clusterAccessedRecordModelV2)
	for _, model := range models {
		tableName, client, err := db.GetTableNameAndDBClientByModel(model)
		if err != nil {
			return nil, err
		}
		if origin, ok := clusterModelsMap[tableName]; ok {
			origin.models = append(origin.models, model)
			clusterModelsMap[tableName] = origin
		} else {
			clusterModelsMap[tableName] = clusterAccessedRecordModelV2{tableName: tableName, client: client, models: []*roomAccessedRecordModelV2{model}}
		}
	}
	clusterModels := make([]clusterAccessedRecordModelV2, 0, len(clusterModelsMap))
	for _, clusterModel := range clusterModelsMap {
		clusterModels = append(clusterModels, clusterModel)
	}
	return clusterModels, nil
}

type redisValue struct {
	Type     string `json:"type"`
	Value    string `json:"value"`
	SyncedTs int64  `json:"synced_ts"`
	ExpireTs int64  `json:"expire_ts"`
}

func (v redisValue) isExpired(t time.Time) bool {
	if v.ExpireTs == 0 {
		return false
	}
	if t.IsZero() {
		return false
	}
	return t.UnixNano()/1000/1000 >= v.ExpireTs
}

func (v redisValue) expireDuration(t time.Time) time.Duration {
	if v.ExpireTs == 0 {
		return 0
	}
	return time.Unix(v.ExpireTs/1000, v.ExpireTs%1000*1000*1000).Sub(t)
}

func (v redisValue) IsZero() bool {
	return v.Type == ""
}

func (v redisValue) String() string {
	return fmt.Sprintf(
		"[RedisValue:type=%s,value=%s,synced_ts=%d,expire_ts=%d]",
		v.Type, v.Value, v.SyncedTs, v.ExpireTs)
}

type roomDataModelV2 struct {
	tableName struct{} `pg:"_"`

	HashTag   string                `pg:"hash_tag,pk"`
	Value     map[string]redisValue `pg:"value"`
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

type loadResult struct {
	model *roomDataModelV2
	err   error
}

func loadDataByIDWithContext(ctx context.Context, db *base.DBCluster, hashTag string) (*roomDataModelV2, error) {
	loadResultCh := make(chan loadResult)
	go func(ch chan loadResult) {
		model, err := loadDataByID(db, hashTag)
		ch <- loadResult{model: model, err: err}
	}(loadResultCh)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-loadResultCh:
		return r.model, r.err
	}
}

func loadDataByID(db *base.DBCluster, hashTag string) (*roomDataModelV2, error) {
	model := &roomDataModelV2{HashTag: hashTag}
	query, err := db.Model(model)
	if err != nil {
		return nil, err
	}
	if err := query.WherePK().Where("deleted_at is NULL").Select(); err != nil {
		if errors.Is(err, pg.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return model, nil
}

var errNoRowsUpdated = errors.New("no rows is updated")

func upsertRoomData(db *base.DBCluster, hashTag, key string, value redisValue) error {
	retryTimes := 3
	var err error
	for i := 0; i < retryTimes; i++ {
		if err = _upsertRoomData(db, hashTag, key, value); err != nil {
			if !isRetryErrorForUpdate(err) {
				return err
			}
			continue
		}
		break
	}
	return err
}

func isRetryErrorForUpdate(err error) bool {
	if errors.Is(err, errNoRowsUpdated) {
		return true
	}
	var pgErr pg.Error
	if errors.As(err, &pgErr) && pgErr.IntegrityViolation() {
		return true
	}
	return false
}

func _upsertRoomData(db *base.DBCluster, hashTag, key string, value redisValue) error {
	currentTime := time.Now()
	model := &roomDataModelV2{HashTag: hashTag}
	query, err := db.Model(model)
	if err != nil {
		return err
	}
	err = query.WherePK().Select()
	if err != nil {
		if errors.Is(err, pg.ErrNoRows) {
			model = &roomDataModelV2{
				HashTag:   hashTag,
				Value:     map[string]redisValue{key: value},
				CreatedAt: currentTime,
				UpdatedAt: currentTime,
				Version:   0,
			}
			query, err = db.Model(model)
			if err != nil {
				return err
			}
			_, err = query.Insert()
			return err
		}
		return err
	}
	result, err := query.Set("value=jsonb_set(value, ?, ?)", key, value).
		Set("updated_at=?", currentTime).
		Set("version=?", model.Version+1).
		WherePK().
		Where("version=?", model.Version).
		Update()
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return errNoRowsUpdated
	}
	return nil
}

func deleteRoomData(db *base.DBCluster, hashTag, key string) error {
	currentTime := time.Now()
	model := &roomDataModelV2{HashTag: hashTag}
	query, err := db.Model(model)
	if err != nil {
		return err
	}
	_, err = query.Set("value=value-?", key).
		Set("updated_at=?", currentTime).
		Set("version=version+1").
		WherePK().
		Update()
	return err
}
