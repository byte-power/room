package service

import (
	"bytepower_room/base"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testInsertDataToDB(
	db *base.DBCluster,
	hashTag string, Value map[string]redisValue,
	DeletedAt, UpdatedAt, CreatedAt time.Time,
	Version int,
) error {
	model := &roomDataModelV2{
		HashTag:   hashTag,
		Value:     Value,
		DeletedAt: DeletedAt,
		UpdatedAt: UpdatedAt,
		CreatedAt: CreatedAt,
		Version:   Version,
	}
	query, _ := db.Model(model)
	_, err := query.Insert()
	return err
}

func testCleanDataInDB(db *base.DBCluster, hashTags ...string) {
	for _, hashTag := range hashTags {
		model := &roomDataModelV2{HashTag: hashTag}
		query, _ := db.Model(model)
		query.WherePK().ForceDelete()
	}
}

func TestLoadDataByID(t *testing.T) {
	db := base.GetDBCluster()

	// load not existed row
	hashTag := "hash_tag_not_exist"
	model, err := loadDataByID(db, hashTag)
	assert.Nil(t, model)
	assert.Nil(t, err)

	currentTime := time.Now()
	// load empty value row
	hashTag = "hash_tag1"
	value := make(map[string]redisValue)
	testInsertDataToDB(db, hashTag, value, time.Time{}, currentTime, currentTime, 0)
	defer testCleanDataInDB(db, hashTag)
	model, err = loadDataByID(db, hashTag)
	assert.Nil(t, err)
	assert.Equal(t, hashTag, model.HashTag)
	assert.Equal(t, 0, len(model.Value))
	assert.Equal(t, time.Time{}, model.DeletedAt)
	assert.Equal(t, 0, model.Version)
	assert.True(t, currentTime.Equal(model.UpdatedAt))
	assert.True(t, currentTime.Equal(model.CreatedAt))

	// load deleted row
	hashTag = "hash_tag2"
	value = make(map[string]redisValue)
	testInsertDataToDB(db, hashTag, value, currentTime, currentTime, currentTime, 0)
	defer testCleanDataInDB(db, hashTag)
	model, err = loadDataByID(db, hashTag)
	assert.Nil(t, err)
	assert.Nil(t, model)

	// load one item value row
	hashTag = "hash_tag3"
	k := "{hash_tag3}a"
	v := redisValue{Type: "string", Value: "abcd", SyncedTs: 1234567, ExpireTs: 12345678}
	value = map[string]redisValue{k: v}
	testInsertDataToDB(db, hashTag, value, time.Time{}, currentTime, currentTime, 0)
	defer testCleanDataInDB(db, hashTag)
	model, err = loadDataByID(db, hashTag)
	assert.Nil(t, err)
	assert.Equal(t, hashTag, model.HashTag)
	assert.Equal(t, 1, len(model.Value))
	assert.Contains(t, model.Value, k)
	assert.Equal(t, v, model.Value[k])
	assert.Equal(t, time.Time{}, model.DeletedAt)
	assert.Equal(t, 0, model.Version)
	assert.True(t, currentTime.Equal(model.UpdatedAt))
	assert.True(t, currentTime.Equal(model.CreatedAt))

	// load multiple items value row
	hashTag = "hash_tag4"
	k1 := "{hash_tag4}a"
	v1 := redisValue{Type: "string", Value: "abcd", SyncedTs: 1234567, ExpireTs: 12345678}
	k2 := "{hash_tag4}b"
	v2 := redisValue{Type: "string", Value: "xyz", SyncedTs: 1234567890, ExpireTs: currentTime.Unix() * 1000}
	value = map[string]redisValue{k1: v1, k2: v2}
	testInsertDataToDB(db, hashTag, value, time.Time{}, currentTime, currentTime, 0)
	defer testCleanDataInDB(db, hashTag)
	model, err = loadDataByID(db, hashTag)
	assert.Nil(t, err)
	assert.Equal(t, hashTag, model.HashTag)
	assert.Equal(t, 2, len(model.Value))
	assert.Contains(t, model.Value, k1)
	assert.Equal(t, v1, model.Value[k1])
	assert.Contains(t, model.Value, k2)
	assert.Equal(t, v2, model.Value[k2])
	assert.Equal(t, time.Time{}, model.DeletedAt)
	assert.Equal(t, 0, model.Version)
	assert.True(t, currentTime.Equal(model.UpdatedAt))
	assert.True(t, currentTime.Equal(model.CreatedAt))
}

func TestLoadDataByIDWithContext(t *testing.T) {
	db := base.GetDBCluster()
	currentTime := time.Now()
	currentTsInMS := currentTime.Unix() * 1000

	// with background context
	hashTag := "hash_tag_context_1"
	k := fmt.Sprintf("{%s}a", hashTag)
	v := redisValue{Type: "string", Value: "abcd", SyncedTs: currentTsInMS, ExpireTs: currentTsInMS}
	value := map[string]redisValue{k: v}
	testInsertDataToDB(db, hashTag, value, time.Time{}, currentTime, currentTime, 0)
	defer testCleanDataInDB(db, hashTag)
	model, err := loadDataByIDWithContext(context.Background(), db, hashTag)
	assert.Nil(t, err)
	assert.Equal(t, hashTag, model.HashTag)
	assert.Equal(t, 1, len(model.Value))
	assert.Contains(t, model.Value, k)
	assert.Equal(t, v, model.Value[k])
	assert.Equal(t, time.Time{}, model.DeletedAt)
	assert.Equal(t, 0, model.Version)
	assert.True(t, currentTime.Equal(model.UpdatedAt))
	assert.True(t, currentTime.Equal(model.CreatedAt))

	// with timeout context
	hashTag = "hash_taag_context_2"
	k = fmt.Sprintf("{%s}a", hashTag)
	v = redisValue{Type: "string", Value: "abcd", SyncedTs: currentTsInMS, ExpireTs: currentTsInMS}
	value = map[string]redisValue{k: v}
	testInsertDataToDB(db, hashTag, value, time.Time{}, currentTime, currentTime, 0)
	defer testCleanDataInDB(db, hashTag)
	// context will timeout in 1ns
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	model, err = loadDataByIDWithContext(ctx, db, hashTag)
	assert.Nil(t, model)
	assert.Equal(t, context.DeadlineExceeded, err)
}
