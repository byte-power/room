package service

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"bytepower_room/utility"
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

var supportedRedisDataTypes = []string{stringType, listType, hashType, setType, zsetType}

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

type RedisValue struct {
	Type     string `json:"type"`
	Value    string `json:"value"`
	SyncedTs int64  `json:"synced_ts"`
	ExpireTs int64  `json:"expire_ts"`
}

func (v RedisValue) IsExpired(t time.Time) bool {
	if v.ExpireTs == 0 {
		return false
	}
	if t.IsZero() {
		return false
	}
	return utility.TimestampInMS(t) >= v.ExpireTs
}

// key does not have expiration, return time.Duration(-1)
// key has expiration and has already expired, return time.Duration(0)
// key has expiration and has not expired yet, return positive time.Duration
func (v RedisValue) TTL(t time.Time) time.Duration {
	if v.ExpireTs == 0 {
		return time.Duration(-1)
	}
	seconds, nanoSeconds := utility.GetSecondsAndNanoSecondsFromTsInMs(v.ExpireTs)
	duration := time.Unix(seconds, nanoSeconds).Sub(t)
	if duration < 0 {
		return time.Duration(0)
	}
	return duration
}

func (v RedisValue) IsZero() bool {
	return v.Type == ""
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

func upsertRoomData(db *base.DBCluster, hashTag, key string, value RedisValue, tryTimes int) error {
	var err error
	for i := 0; i < tryTimes; i++ {
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

func _upsertRoomData(db *base.DBCluster, hashTag, key string, value RedisValue) error {
	if value.IsZero() {
		return nil
	}
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
				Value:     map[string]RedisValue{key: value},
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
	result, err := query.Set("value=jsonb_set(value, ?, ?)", pg.Array([]string{key}), value).
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

func loadWrittenRecordModelByID(db *base.DBCluster, id string) (*roomWrittenRecordModel, error) {
	model := &roomWrittenRecordModel{Key: id}
	query, err := db.Model(model)
	if err != nil {
		return nil, err
	}
	if err := query.WherePK().Select(); err != nil {
		if errors.Is(err, pg.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return model, nil
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

type clusterAccessedRecordModel struct {
	tableName string
	client    *pg.DB
	models    []*roomAccessedRecordModel
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

func loadAccessedRecordModelByID(db *base.DBCluster, id string) (*roomAccessedRecordModelV2, error) {
	model := &roomAccessedRecordModelV2{HashTag: id}
	query, err := db.Model(model)
	if err != nil {
		return nil, err
	}
	if err := query.WherePK().Select(); err != nil {
		if errors.Is(err, pg.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return model, nil
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

type HashTagKeysStatus string

const (
	HashTagKeysStatusNeedSynced HashTagKeysStatus = "need_synced"
	HashTagKeysStatusSynced     HashTagKeysStatus = "synced"
	HashTagKeysStatusCleaned    HashTagKeysStatus = "cleaned"
)

type roomHashTagKeys struct {
	tableName struct{} `pg:"_"`

	HashTag    string            `pg:"hash_tag,pk"`
	Keys       []string          `pg:"keys"`
	AccessedAt time.Time         `pg:"accessed_at"`
	WrittenAt  time.Time         `pg:"written_at"`
	SyncedAt   time.Time         `pg:"synced_at"`
	CreatedAt  time.Time         `pg:"created_at"`
	UpdatedAt  time.Time         `pg:"updated_at"`
	Status     HashTagKeysStatus `pg:"status"`
	Version    int64             `pg:"version"`
}

func (model *roomHashTagKeys) ShardingKey() string {
	return model.HashTag
}

func (model *roomHashTagKeys) GetTablePrefix() string {
	return "room_keys"
}

func upsertHashTagKeysRecordByEvent(ctx context.Context, dbCluster *base.DBCluster, event base.Event) error {
	currentTime := time.Now()
	model := &roomHashTagKeys{HashTag: event.HashTag}
	tableName, db, err := dbCluster.GetTableNameAndDBClientByModel(model)
	if err != nil {
		return err
	}
	//TODO: add timeout
	err = db.RunInTransaction(ctx, func(tx *pg.Tx) error {
		err := tx.Model(model).Table(tableName).WherePK().For("UPDATE").Select()
		if err != nil && !errors.Is(err, pg.ErrNoRows) {
			return err
		}
		// Insert new row
		if err != nil && errors.Is(err, pg.ErrNoRows) {
			model = &roomHashTagKeys{
				HashTag:    event.HashTag,
				Keys:       event.Keys.ToSlice(),
				AccessedAt: event.AccessTime,
				CreatedAt:  currentTime,
				UpdatedAt:  currentTime,
				Status:     HashTagKeysStatusNeedSynced,
				Version:    0,
			}
			if event.AccessMode == base.HashTagAccessModeWrite {
				model.WrittenAt = event.AccessTime
			}
			_, err = tx.Model(model).Table(tableName).Insert()
			return err
		}
		// update
		model.HashTag = event.HashTag
		originKeys := model.Keys
		model.Keys = utility.MergeStringSliceAndRemoveDuplicateItems(originKeys, event.Keys.ToSlice())
		model.AccessedAt = utility.GetLatestTime(model.AccessedAt, event.AccessTime)
		model.UpdatedAt = currentTime
		originVersion := model.Version
		model.Version = originVersion + 1
		if event.AccessMode == base.HashTagAccessModeWrite {
			model.WrittenAt = utility.GetLatestTime(model.WrittenAt, event.AccessTime)
		}
		if (len(originKeys) != len(model.Keys)) || event.AccessMode == base.HashTagAccessModeWrite {
			model.Status = HashTagKeysStatusNeedSynced
		} else {
			model.Status = HashTagKeysStatusSynced
		}
		result, err := tx.Model(model).Table(tableName).WherePK().Where("version=?", originVersion).Update()
		if err != nil {
			return err
		}
		// This seems will not happen since use lock in `select for update`
		if result.RowsAffected() != 1 {
			return errNoRowsUpdated
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return base.NewDBTxError(err)
	}
	return nil
}

// find keys to sync
// select * from table where status = "syncing";
// update table set status = "synced", syncedAt = time.Now() where hash_tag = "xxx" and version = xx

// find keys to clean
// select * from table where status != "cleaned" and accessed_at < ?;
// update table set status = "cheaned" where hash_tag = "xxx" and version = "xxx"
