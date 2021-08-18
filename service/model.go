package service

import (
	"bytepower_room/base"
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

type RedisValue struct {
	Type     string `json:"type"`
	Value    string `json:"value"`
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

func isRetryErrorForUpdateInTx(err error) bool {
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

func upsertRoomDataValue(db *base.DBCluster, hashTag string, value map[string]RedisValue, tryTimes int) error {
	var err error
	for i := 0; i < tryTimes; i++ {
		if err = _upsertRoomDataValue(db, hashTag, value); err != nil {
			if !isRetryErrorForUpdateInTx(err) {
				return err
			}
			continue
		}
		break
	}
	return err
}

func _upsertRoomDataValue(dbCluster *base.DBCluster, hashTag string, value map[string]RedisValue) error {
	currentTime := time.Now()
	model := &roomDataModelV2{HashTag: hashTag}
	tableName, db, err := dbCluster.GetTableNameAndDBClientByModel(model)
	if err != nil {
		return err
	}

	err = db.RunInTransaction(context.TODO(), func(tx *pg.Tx) error {
		err := tx.Model(model).Table(tableName).WherePK().Select()
		if err != nil && !errors.Is(err, pg.ErrNoRows) {
			return err
		}
		if err != nil && errors.Is(err, pg.ErrNoRows) {
			model = &roomDataModelV2{
				HashTag:   hashTag,
				Value:     value,
				CreatedAt: currentTime,
				UpdatedAt: currentTime,
				Version:   0,
			}
			_, err = tx.Model(model).Table(tableName).Insert()
			return err
		}

		result, err := tx.Model(model).Table(tableName).
			Set("value=?", value).
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
	})
	return err
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
	Keys       []string          `pg:"keys,array"`
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
	return "room_hash_tag_keys"
}

func (model *roomHashTagKeys) SetStatusAsSynced(db *base.DBCluster, t time.Time) error {
	query, err := db.Model(model)
	if err != nil {
		return err
	}
	result, err := query.Set("status=?", HashTagKeysStatusSynced).
		Set("synced_at=?", t).
		Set("updated_at=?", t).
		Set("version=?", model.Version+1).
		WherePK().
		Where("version=?", model.Version).
		Update()
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return errNoRowsUpdated
	}
	return nil
}

func (model *roomHashTagKeys) SetStatusAsCleaned(db *base.DBCluster, t time.Time) error {
	query, err := db.Model(model)
	if err != nil {
		return err
	}
	result, err := query.Set("status=?", HashTagKeysStatusCleaned).
		Set("updated_at=?", t).
		Set("version=?", model.Version+1).
		WherePK().
		Where("version=?", model.Version).
		Update()
	if err != nil {
		return err
	}
	if result.RowsAffected() != 1 {
		return errNoRowsUpdated
	}
	return nil
}

func (model *roomHashTagKeys) updateFromEvent(event base.HashTagEvent) []string {
	toBeUpdatedColumns := []string{}

	originKeys := model.Keys
	newKeys := utility.MergeStringSliceAndRemoveDuplicateItems(originKeys, event.Keys.ToSlice())
	if len(originKeys) != len(newKeys) {
		model.Keys = newKeys
		toBeUpdatedColumns = append(toBeUpdatedColumns, "keys")
	}

	if event.AccessTime.After(model.AccessedAt) {
		model.AccessedAt = event.AccessTime
		toBeUpdatedColumns = append(toBeUpdatedColumns, "accessed_at")
	}
	if event.WriteTime.After(model.WrittenAt) {
		model.WrittenAt = event.WriteTime
		toBeUpdatedColumns = append(toBeUpdatedColumns, "written_at")
	}

	var newStatus HashTagKeysStatus
	if (len(originKeys) != len(newKeys)) || !event.WriteTime.IsZero() {
		newStatus = HashTagKeysStatusNeedSynced
	} else if model.Status == HashTagKeysStatusCleaned {
		newStatus = HashTagKeysStatusSynced
	}
	if newStatus != "" && newStatus != model.Status {
		model.Status = newStatus
		toBeUpdatedColumns = append(toBeUpdatedColumns, "status")
	}
	return toBeUpdatedColumns
}

func upsertHashTagKeysRecordByEvent(ctx context.Context, dbCluster *base.DBCluster, event base.HashTagEvent, currentTime time.Time) error {
	model := &roomHashTagKeys{HashTag: event.HashTag}
	tableName, db, err := dbCluster.GetTableNameAndDBClientByModel(model)
	if err != nil {
		return err
	}
	err = db.RunInTransaction(ctx, func(tx *pg.Tx) error {
		err := tx.Model(model).Table(tableName).WherePK().Select()
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
				Version:    0,
			}
			if !event.WriteTime.IsZero() {
				model.WrittenAt = event.WriteTime
			}
			if event.Keys.Len() == 0 && event.WriteTime.IsZero() {
				model.Status = HashTagKeysStatusSynced
			} else {
				model.Status = HashTagKeysStatusNeedSynced
			}
			_, err = tx.Model(model).Table(tableName).Insert()
			return err
		}
		// update
		originVersion := model.Version
		toBeUpdatedColumns := model.updateFromEvent(event)
		if len(toBeUpdatedColumns) == 0 {
			return nil
		}
		model.Version = model.Version + 1
		model.UpdatedAt = currentTime
		toBeUpdatedColumns = append(toBeUpdatedColumns, "version", "updated_at")
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
	if err != nil {
		return err
	}
	return nil
}

type dbWhereCondition struct {
	column    string
	operator  string
	parameter interface{}
}

func (condition dbWhereCondition) getConditionAndParameter() (string, interface{}) {
	return fmt.Sprintf("%s %s", condition.column, condition.operator), condition.parameter
}

func (condition dbWhereCondition) string() string {
	return fmt.Sprintf("%s%s%v", condition.column, condition.operator, condition.parameter)
}

func loadHashTagKeysModelsByCondition(db *base.DBCluster, count int, startIndex int, conditions ...dbWhereCondition) (int, []*roomHashTagKeys, error) {
	shardingCount := db.GetShardingCount()
	tablePrefix := (&roomHashTagKeys{}).GetTablePrefix()
	var models []*roomHashTagKeys
	for index := startIndex; index < shardingCount; index++ {
		query, err := db.Models(&models, tablePrefix, index)
		if err != nil {
			return 0, nil, err
		}
		for _, condition := range conditions {
			cond, parameter := condition.getConditionAndParameter()
			query.Where(cond, parameter)
		}
		err = query.Limit(count).Select()
		if err != nil {
			if errors.Is(err, pg.ErrNoRows) {
				continue
			}
			return 0, nil, err
		}
		if len(models) > 0 {
			return index, models, nil
		}
	}
	return shardingCount, nil, nil
}
