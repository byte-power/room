package service

import (
	"bytepower_room/base"
	"bytepower_room/utility"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testInsertDataToDB(
	db *base.DBCluster,
	hashTag string, Value map[string]RedisValue,
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
	value := make(map[string]RedisValue)
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
	value = make(map[string]RedisValue)
	testInsertDataToDB(db, hashTag, value, currentTime, currentTime, currentTime, 0)
	defer testCleanDataInDB(db, hashTag)
	model, err = loadDataByID(db, hashTag)
	assert.Nil(t, err)
	assert.Nil(t, model)

	// load one item value row
	hashTag = "hash_tag3"
	k := "{hash_tag3}a"
	v := RedisValue{Type: "string", Value: "abcd", SyncedTs: 1234567, ExpireTs: 12345678}
	value = map[string]RedisValue{k: v}
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
	v1 := RedisValue{Type: "string", Value: "abcd", SyncedTs: 1234567, ExpireTs: 12345678}
	k2 := "{hash_tag4}b"
	v2 := RedisValue{Type: "string", Value: "xyz", SyncedTs: 1234567890, ExpireTs: currentTime.Unix() * 1000}
	value = map[string]RedisValue{k1: v1, k2: v2}
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
	v := RedisValue{Type: "string", Value: "abcd", SyncedTs: currentTsInMS, ExpireTs: currentTsInMS}
	value := map[string]RedisValue{k: v}
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
	v = RedisValue{Type: "string", Value: "abcd", SyncedTs: currentTsInMS, ExpireTs: currentTsInMS}
	value = map[string]RedisValue{k: v}
	testInsertDataToDB(db, hashTag, value, time.Time{}, currentTime, currentTime, 0)
	defer testCleanDataInDB(db, hashTag)
	// context will timeout in 1ns
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	model, err = loadDataByIDWithContext(ctx, db, hashTag)
	assert.Nil(t, model)
	assert.Equal(t, context.DeadlineExceeded, err)
}

func TestRedisValue(t *testing.T) {
	currentTime := time.Now()
	currentTs := currentTime.Unix()*1000 + currentTime.UnixNano()/1000/1000%1000

	// test empty value
	value := RedisValue{}
	assert.True(t, value.IsZero())
	assert.False(t, value.IsExpired(currentTime))
	assert.False(t, value.IsExpired(time.Time{}))
	assert.Equal(t, time.Duration(-1), value.TTL(currentTime))
	assert.Equal(t, time.Duration(-1), value.TTL(time.Time{}))

	// test value without expiration
	value = RedisValue{
		Type:     "string",
		Value:    "abcd",
		SyncedTs: currentTs,
	}
	assert.False(t, value.IsZero())
	assert.False(t, value.IsExpired(currentTime))
	assert.False(t, value.IsExpired(time.Time{}))
	assert.Equal(t, time.Duration(-1), value.TTL(currentTime))
	assert.Equal(t, time.Duration(-1), value.TTL(time.Time{}))

	// test value with expiration that just expires
	value = RedisValue{
		Type:     "string",
		Value:    "abcd",
		SyncedTs: currentTs,
		ExpireTs: currentTs,
	}
	assert.False(t, value.IsZero())
	assert.True(t, value.IsExpired(currentTime))
	assert.False(t, value.IsExpired(time.Time{}))
	assert.Equal(t, time.Duration(0), value.TTL(currentTime))
	assert.Greater(t, int64(value.TTL(time.Time{})), int64(0))

	// test value with expiration that has expired
	value = RedisValue{
		Type:     "string",
		Value:    "abcd",
		SyncedTs: currentTs,
		ExpireTs: currentTs - 10,
	}
	assert.False(t, value.IsZero())
	assert.True(t, value.IsExpired(currentTime))
	assert.False(t, value.IsExpired(time.Time{}))
	assert.Equal(t, time.Duration(0), value.TTL(currentTime))
	assert.Greater(t, int64(value.TTL(time.Time{})), int64(0))

	// test value with expiration that has not expired yet
	value = RedisValue{
		Type:     "string",
		Value:    "abcd",
		SyncedTs: currentTs,
		ExpireTs: currentTs + 10,
	}
	assert.False(t, value.IsZero())
	assert.False(t, value.IsExpired(currentTime))
	assert.False(t, value.IsExpired(time.Time{}))
	assert.Greater(t, int64(10*time.Millisecond), int64(value.TTL(currentTime)))
	assert.Greater(t, int64(value.TTL(currentTime)), int64(9*time.Millisecond))
	assert.Greater(t, int64(value.TTL(time.Time{})), int64(0))
}

func TestDeleteRoomWrittenRecordModel(t *testing.T) {
	db := base.GetWrittenRecordDBCluster()
	currentTime := time.Now()
	key := "{a}bc"

	// delete a non existed record
	err := deleteRoomWrittenRecordModel(db, key, currentTime)
	assert.Nil(t, err)

	model := &roomWrittenRecordModel{Key: key, CreatedAt: currentTime, WrittenAt: currentTime}
	query, _ := db.Model(model)
	query.Insert(model)

	// delete a record with writtenAt not matched
	err = deleteRoomWrittenRecordModel(db, key, currentTime.Add(10*time.Second))
	model, _ = loadWrittenRecordModelByID(db, key)
	assert.NotNil(t, model)

	// delete a record with writtenAt
	err = deleteRoomWrittenRecordModel(db, key, currentTime)
	model, _ = loadWrittenRecordModelByID(db, key)
	assert.Nil(t, model)
}

func TestDeleteRoomData(t *testing.T) {
	hashTag := "abc"
	currentTs := utility.TimestampInMS(time.Now())
	db := base.GetDBCluster()
	defer testEmptyRoomDataRecordInDatabase(hashTag)
	value := map[string]RedisValue{
		"{abc}a":  {Type: "string", Value: "v", SyncedTs: currentTs},
		"a{abc}b": {Type: "string", Value: "v", SyncedTs: currentTs},
		"a{abc}c": {Type: "string", Value: "v", SyncedTs: currentTs},
	}
	model := &roomDataModelV2{
		HashTag: hashTag,
		Value:   value,
		Version: 0,
	}
	query, _ := db.Model(model)
	query.Insert()

	// delete a non existed key
	err := deleteRoomData(db, hashTag, "{a}notexist")
	assert.Nil(t, err)
	m, err := loadDataByID(db, hashTag)
	assert.Equal(t, 1, m.Version)
	assert.Equal(t, len(value), len(m.Value))

	// delete a key
	err = deleteRoomData(db, hashTag, "a{abc}b")
	assert.Nil(t, err)
	m, err = loadDataByID(db, hashTag)
	assert.Equal(t, 2, m.Version)
	assert.Equal(t, len(value)-1, len(m.Value))
	assert.True(t, m.Value["a{abc}b"].IsZero())

	// delete all keys
	for key := range value {
		deleteRoomData(db, hashTag, key)
	}
	m, err = loadDataByID(db, hashTag)
	assert.Equal(t, 5, m.Version)
	assert.Equal(t, map[string]RedisValue{}, m.Value)
}

func TestUpsertRoomData(t *testing.T) {
	hashTag := "abc"
	currentTs := utility.TimestampInMS(time.Now())
	db := base.GetDBCluster()
	defer testEmptyRoomDataRecordInDatabase(hashTag)

	// upsert key in a not exist hash_tag record
	key := "{abc}ab"
	value := RedisValue{Type: "string", Value: "ab", SyncedTs: currentTs}
	err := _upsertRoomData(db, hashTag, key, value)
	assert.Nil(t, err)
	m, _ := loadDataByID(db, hashTag)
	assert.Equal(t, hashTag, m.HashTag)
	assert.Equal(t, 0, m.Version)
	assert.Equal(t, 1, len(m.Value))
	assert.Equal(t, value, m.Value[key])
	assert.True(t, m.DeletedAt.IsZero())
	assert.False(t, m.CreatedAt.IsZero())
	assert.False(t, m.UpdatedAt.IsZero())

	// upsert key in an existed hash_tag record
	key = "{abc}abc"
	value = RedisValue{Type: "string", Value: "abc", SyncedTs: currentTs}
	err = _upsertRoomData(db, hashTag, key, value)
	assert.Nil(t, err)
	m, _ = loadDataByID(db, hashTag)
	assert.Equal(t, hashTag, m.HashTag)
	assert.Equal(t, 1, m.Version)
	assert.Equal(t, 2, len(m.Value))
	assert.Equal(t, value, m.Value[key])
	assert.True(t, m.DeletedAt.IsZero())
	assert.False(t, m.CreatedAt.IsZero())
	assert.False(t, m.UpdatedAt.IsZero())

	// upsert an existed key in an existed hash_tag record
	value = RedisValue{Type: "string", Value: "abc_new", SyncedTs: currentTs}
	err = _upsertRoomData(db, hashTag, key, value)
	assert.Nil(t, err)
	m, _ = loadDataByID(db, hashTag)
	assert.Equal(t, hashTag, m.HashTag)
	assert.Equal(t, 2, m.Version)
	assert.Equal(t, 2, len(m.Value))
	assert.Equal(t, value, m.Value[key])
	assert.True(t, m.DeletedAt.IsZero())
	assert.False(t, m.CreatedAt.IsZero())
	assert.False(t, m.UpdatedAt.IsZero())

	// upsert conflict
	hashTag = "abcd"
	defer testEmptyRoomDataRecordInDatabase(hashTag)
	count := 10
	values := make(map[string]RedisValue, count)
	for i := 0; i < count; i++ {
		key := fmt.Sprintf("%d{%s}%d", i, hashTag, i)
		value := fmt.Sprintf("%d", i)
		values[key] = RedisValue{Type: "string", Value: value, SyncedTs: currentTs}
	}
	ch := make(chan error, 100)
	wg := sync.WaitGroup{}
	for key, value := range values {
		wg.Add(1)
		go func(ch chan error, key string, value RedisValue) {
			err := _upsertRoomData(db, hashTag, key, value)
			if err != nil {
				ch <- err
			}
			wg.Done()
		}(ch, key, value)
	}
	wg.Wait()
	close(ch)
	errorCount := 0
	for err := range ch {
		assert.True(t, isRetryErrorForUpdate(err))
		errorCount += 1
	}
	m, _ = loadDataByID(db, hashTag)
	assert.Equal(t, count-1-errorCount, m.Version)
	assert.Equal(t, count-errorCount, len(m.Value))
}

func TestUpsertHashTagKeysRecordByEvent(t *testing.T) {
	db := base.GetDBCluster()

	// insert row with read event
	hashTag := "abc"
	defer testEmptyHashTagKeysRecordInDB(hashTag)
	keys := []string{"{abc}a", "{abc}b", "{abc}a", "{abc}c", "{abc}b", "{abc}d"}
	uniqueKeys := []string{"{abc}a", "{abc}b", "{abc}c", "{abc}d"}
	currentTime := time.Now()
	eventTime, _ := time.Parse("2006-01-02 15:04:05", "2021-06-25 11:30:25")
	event, _ := base.NewHashTagEvent(hashTag, keys, base.HashTagAccessModeRead, eventTime)
	err := upsertHashTagKeysRecordByEvent(context.TODO(), db, event, currentTime)
	assert.Nil(t, err)

	models, _ := loadHashTagKeysModelsByCondition(db, 100, dbWhereCondition{column: "hash_tag", operator: "=", parameter: hashTag})
	assert.Equal(t, 1, len(models))
	model := models[0]
	assert.Equal(t, hashTag, model.HashTag)
	assert.ElementsMatch(t, uniqueKeys, model.Keys)
	assert.True(t, model.WrittenAt.IsZero())
	assert.True(t, model.AccessedAt.Equal(event.AccessTime))
	assert.True(t, model.SyncedAt.IsZero())
	assert.Equal(t, HashTagKeysStatusNeedSynced, model.Status)
	assert.Equal(t, int64(0), model.Version)
	assert.True(t, currentTime.Equal(model.UpdatedAt))
	assert.True(t, currentTime.Equal(model.CreatedAt))

	// insert row with write event
	hashTag = "def"
	defer testEmptyHashTagKeysRecordInDB(hashTag)
	keys = []string{"{def}a", "{def}b", "{def}a", "{def}c", "{def}b", "{def}d"}
	uniqueKeys = []string{"{def}a", "{def}b", "{def}c", "{def}d"}
	currentTime = time.Now()
	eventTime, _ = time.Parse("2006-01-02 15:04:05", "2021-06-25 12:35:20")
	event, _ = base.NewHashTagEvent(hashTag, keys, base.HashTagAccessModeWrite, eventTime)
	err = upsertHashTagKeysRecordByEvent(context.TODO(), db, event, currentTime)
	assert.Nil(t, err)

	models, _ = loadHashTagKeysModelsByCondition(db, 100, dbWhereCondition{column: "hash_tag", operator: "=", parameter: hashTag})
	assert.Equal(t, 1, len(models))
	model = models[0]
	assert.Equal(t, hashTag, model.HashTag)
	assert.ElementsMatch(t, uniqueKeys, model.Keys)
	assert.True(t, model.WrittenAt.Equal(event.AccessTime))
	assert.True(t, model.AccessedAt.Equal(event.AccessTime))
	assert.True(t, model.SyncedAt.IsZero())
	assert.Equal(t, HashTagKeysStatusNeedSynced, model.Status)
	assert.Equal(t, int64(0), model.Version)
	assert.True(t, currentTime.Equal(model.UpdatedAt))
	assert.True(t, currentTime.Equal(model.CreatedAt))

	hashTag = "xyz"
	defer testEmptyHashTagKeysRecordInDB(hashTag)
	keys = []string{"{xyz}a", "{xyz}b", "{xyz}a", "{xyz}c"}
	uniqueKeys = []string{"{xyz}a", "{xyz}b", "{xyz}c"}
	currentTime = time.Now()
	eventTime, _ = time.Parse("2006-01-02 15:04:05", "2021-06-25 13:42:30")
	event, _ = base.NewHashTagEvent(hashTag, keys, base.HashTagAccessModeRead, eventTime)
	_ = upsertHashTagKeysRecordByEvent(context.TODO(), db, event, currentTime)

	// update row with read keys
	newKeys := []string{"{xyz}x", "{xyz}y", "{xyz}z", "{xyz}a", "{xyz}b", "{xyz}z"}
	uniqueNewKeys := []string{"{xyz}x", "{xyz}y", "{xyz}z"}
	currentTime = time.Now()
	eventTime, _ = time.Parse("2006-01-02 15:04:05", "2021-06-25 13:43:25")
	event, _ = base.NewHashTagEvent(hashTag, newKeys, base.HashTagAccessModeRead, eventTime)
	err = upsertHashTagKeysRecordByEvent(context.TODO(), db, event, currentTime)
	assert.Nil(t, err)

	models, _ = loadHashTagKeysModelsByCondition(db, 100, dbWhereCondition{column: "hash_tag", operator: "=", parameter: hashTag})
	assert.Equal(t, 1, len(models))
	model = models[0]
	assert.Equal(t, hashTag, model.HashTag)
	assert.ElementsMatch(t, append(uniqueKeys, uniqueNewKeys...), model.Keys)
	assert.True(t, model.WrittenAt.IsZero())
	assert.True(t, model.AccessedAt.Equal(event.AccessTime))
	assert.True(t, model.SyncedAt.IsZero())
	assert.Equal(t, HashTagKeysStatusNeedSynced, model.Status)
	assert.Equal(t, int64(1), model.Version)
	assert.True(t, currentTime.Equal(model.UpdatedAt))
	assert.True(t, currentTime.After(model.CreatedAt))

	// update row with write keys
	newKeys2 := []string{"{xyz}n", "{xyz}m", "{xyz}m", "{xyz}a", "{xyz}b", "{xyz}z", "{xyz}x"}
	uniqueNewKeys2 := []string{"{xyz}m", "{xyz}n"}
	currentTime = time.Now()
	eventTime, _ = time.Parse("2006-01-02 15:04:05", "2021-06-25 13:53:45")
	event, _ = base.NewHashTagEvent(hashTag, newKeys2, base.HashTagAccessModeWrite, eventTime)
	err = upsertHashTagKeysRecordByEvent(context.TODO(), db, event, currentTime)
	assert.Nil(t, err)

	models, _ = loadHashTagKeysModelsByCondition(db, 100, dbWhereCondition{column: "hash_tag", operator: "=", parameter: hashTag})
	assert.Equal(t, 1, len(models))
	model = models[0]
	assert.Equal(t, hashTag, model.HashTag)
	assert.ElementsMatch(t, append(append(uniqueKeys, uniqueNewKeys...), uniqueNewKeys2...), model.Keys)
	assert.True(t, model.WrittenAt.Equal(event.AccessTime))
	assert.True(t, model.AccessedAt.Equal(event.AccessTime))
	assert.True(t, model.SyncedAt.IsZero())
	assert.Equal(t, HashTagKeysStatusNeedSynced, model.Status)
	assert.Equal(t, int64(2), model.Version)
	assert.True(t, currentTime.Equal(model.UpdatedAt))
	assert.True(t, currentTime.After(model.CreatedAt))
}
