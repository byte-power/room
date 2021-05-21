package service

import (
	"bytepower_room/base"
	"bytepower_room/utility"
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func testEmptyWrittenRecordsInDB(keys ...string) {
	db := base.GetWrittenRecordDBCluster()
	for _, key := range keys {
		model := &roomWrittenRecordModel{Key: key}
		query, _ := db.Model(model)
		query.WherePK().Delete()
	}
}

func testEmptyAccessedRecordsInDB(hashTags ...string) {
	db := base.GetAccessedRecordDBCluster()
	for _, hashTag := range hashTags {
		model := &roomAccessedRecordModelV2{HashTag: hashTag}
		query, _ := db.Model(model)
		query.WherePK().ForceDelete()
	}
}

func testEmptyRecordsInDB(keys ...string) {
	testEmptyAccessedRecordsInDB(keys...)
	testEmptyWrittenRecordsInDB(keys...)
}

func TestDownloadFilesFromS3(t *testing.T) {
	s3Config := base.GetServerConfig().SyncService.S3
	bucket := "dev-bytepower"
	keyPrefix := "bytepower-server/room-service/access/2020/12/28/07"
	keys := []string{
		"48/00-20201228074900-0ff73314-fd5c-40f7-be8f-a62a2f4ef3f5.log.gz",
		"51/00-20201228075200-6ee8fd27-12bc-4648-a828-05e795f2e125.log.gz",
		"53/00-20201228075400-549b905e-94f9-4f7f-a9c1-52fb1fdb1d5e.log.gz"}
	metaInfos := make([]base.S3MetaInfo, len(keys))
	for index, key := range keys {
		metaInfos[index] = base.S3MetaInfo{
			Bucket: bucket,
			Key:    fmt.Sprintf("%s/%s", keyPrefix, key)}
	}
	contents, err := downloadS3Files(s3Config, metaInfos...)
	assert.Nil(t, err)
	assert.Equal(t, len(keys), len(contents))
}

func TestProcessAccessFile(t *testing.T) {
	testdataFile := "../cmd/testdata.txt"
	accessedHashTags := []string{
		"a", "b",
	}
	writtenKeys := []string{"{a}36", "{a}38", "{a}40", "{b}20"}
	content, err := ioutil.ReadFile(testdataFile)
	assert.Nil(t, err)
	accessedMap, writtenMap, err := processAccessFile(content)
	assert.Nil(t, nil)
	assert.Equal(t, 2, len(accessedMap))
	assert.Equal(t, 4, len(writtenMap))
	for _, hashTag := range accessedHashTags {
		_, ok := accessedMap[hashTag]
		assert.True(t, ok)
	}
	for _, key := range writtenKeys {
		_, ok := writtenMap[key]
		assert.True(t, ok)
	}
}

func TestBulkUpsertRecordModels(t *testing.T) {
	testdataFile := "../cmd/testdata.txt"
	content, err := ioutil.ReadFile(testdataFile)
	assert.Nil(t, err)
	accessedMap, writtenMap, err := processAccessFile(content)
	writtenModels := writtenMapToModels(writtenMap)
	accessedModels := accessedMapToModels(accessedMap)
	err = bulkUpsertWrittenRecordModels(writtenModels...)
	assert.Nil(t, err)
	err = bulkUpsertAccessedRecordModelsV2(accessedModels...)
	assert.Nil(t, err)

	accessedHashTags := make([]string, 0, len(accessedModels))
	for _, model := range accessedModels {
		accessedHashTags = append(accessedHashTags, model.HashTag)
	}
	testEmptyAccessedRecordsInDB(accessedHashTags...)

	writtenKeys := make([]string, 0, len(writtenModels))
	for _, model := range writtenModels {
		writtenKeys = append(writtenKeys, model.Key)
	}
	testEmptyWrittenRecordsInDB(writtenKeys...)
}

func TestBulkUpsertRecordModelsOnConflict(t *testing.T) {
	testdataFile := "../cmd/testdata.txt"
	content, err := ioutil.ReadFile(testdataFile)
	assert.Nil(t, err)
	accessedMap, writtenMap, err := processAccessFile(content)
	writtenModels := writtenMapToModels(writtenMap)
	accessedModels := accessedMapToModels(accessedMap)
	err = bulkUpsertWrittenRecordModels(writtenModels...)
	assert.Nil(t, err)
	err = bulkUpsertAccessedRecordModelsV2(accessedModels...)
	assert.Nil(t, err)

	currentTime := time.Now()
	for index, model := range writtenModels {
		model.WrittenAt = currentTime
		writtenModels[index] = model
	}
	err = bulkUpsertWrittenRecordModels(writtenModels...)
	assert.Nil(t, err)
	db := base.GetWrittenRecordDBCluster()
	for _, model := range writtenModels {
		m, _ := loadWrittenRecordModelByID(db, model.Key)
		assert.True(t, currentTime.Equal(m.WrittenAt))
	}

	for index, model := range accessedModels {
		model.AccessedAt = currentTime
		accessedModels[index] = model
	}
	err = bulkUpsertAccessedRecordModelsV2(accessedModels...)
	assert.Nil(t, err)
	db = base.GetAccessedRecordDBCluster()
	for _, model := range accessedModels {
		m, _ := loadAccessedRecordModelByID(db, model.HashTag)
		assert.True(t, currentTime.Equal(m.AccessedAt))
	}

	accessedHashTags := make([]string, 0, len(accessedModels))
	for _, model := range accessedModels {
		accessedHashTags = append(accessedHashTags, model.HashTag)
	}
	testEmptyAccessedRecordsInDB(accessedHashTags...)

	writtenKeys := make([]string, 0, len(writtenModels))
	for _, model := range writtenModels {
		writtenKeys = append(writtenKeys, model.Key)
	}
	testEmptyWrittenRecordsInDB(writtenKeys...)
}

func TestLoadRecordModels(t *testing.T) {
	testdataFile := "../cmd/testdata.txt"
	content, err := ioutil.ReadFile(testdataFile)
	assert.Nil(t, err)
	accessedMap, writtenMap, err := processAccessFile(content)
	writtenModels := writtenMapToModels(writtenMap)
	accessedModels := accessedMapToModels(accessedMap)
	err = bulkUpsertWrittenRecordModels(writtenModels...)
	assert.Nil(t, err)
	err = bulkUpsertAccessedRecordModelsV2(accessedModels...)
	assert.Nil(t, err)

	count := 0
	for {
		models, err := loadWrittenRecordModels(100)
		assert.Nil(t, err)
		if (models == nil) || len(models) == 0 {
			break
		}
		count += len(models)
		keys := make([]string, len(models))
		for index, model := range models {
			keys[index] = model.Key
		}
		testEmptyWrittenRecordsInDB(keys...)
	}
	assert.Equal(t, len(writtenMap), count)

	count = 0
	for {
		models, err := loadAccessedRecordModels(100, time.Now(), []string{})
		assert.Nil(t, err)
		if (models == nil) || len(models) == 0 {
			break
		}
		count += len(models)
		keys := make([]string, len(models))
		for index, model := range models {
			keys[index] = model.HashTag
		}
		testEmptyAccessedRecordsInDB(keys...)
	}
	assert.Equal(t, len(accessedMap), count)
}

func TestDeleteModels(t *testing.T) {
	testdataFile := "../cmd/testdata.txt"
	content, err := ioutil.ReadFile(testdataFile)
	assert.Nil(t, err)
	accessedMap, writtenMap, err := processAccessFile(content)
	writtenModels := writtenMapToModels(writtenMap)
	accessedModels := accessedMapToModels(accessedMap)
	err = bulkUpsertWrittenRecordModels(writtenModels...)
	assert.Nil(t, err)
	err = bulkUpsertAccessedRecordModelsV2(accessedModels...)
	assert.Nil(t, err)

	err = deleteWrittenRecordModels(writtenModels)
	assert.Nil(t, err)

	err = deleteAccessedRecordModels(accessedModels)
	assert.Nil(t, err)
}

// func TestSyncWrittenModels(t *testing.T) {
// 	redisClient := base.GetRedisCluster()
// 	currentTime := time.Now()

// 	keys := []string{"{a}abc", "{a}abcd"}
// 	for _, key := range keys {
// 		redisClient.Set(testContextTODO, key, key, 0)
// 	}
// 	notExistedKeys := []string{"{b}xxx", "{d}xxx"}
// 	for _, key := range notExistedKeys {
// 		db := base.GetDBCluster()
// 		model := &roomDataModel{
// 			Key:       key,
// 			Type:      "string",
// 			Value:     key,
// 			Deleted:   false,
// 			UpdatedAt: currentTime,
// 			SyncedAt:  currentTime,
// 			CreatedAt: currentTime,
// 			Version:   0,
// 		}
// 		query, _ := db.Model(model)
// 		query.Insert()
// 	}
// 	models := make([]*roomWrittenRecordModel, len(keys)+len(notExistedKeys))
// 	for index, key := range append(keys, notExistedKeys...) {
// 		models[index] = &roomWrittenRecordModel{
// 			Key:       key,
// 			CreatedAt: currentTime,
// 			WrittenAt: currentTime}
// 	}
// 	_, _, err := syncWrittenModels(models)
// 	assert.Nil(t, err)
// 	testEmptyKeysInDatabase(keys...)
// 	testEmptyKeysInDatabase(notExistedKeys...)
// 	testEmptyKeysInRedis(keys...)
// }

func generateListValueForRedis(count int) []interface{} {
	items := make([]interface{}, count)
	for i := 0; i < count; i++ {
		items[i] = utility.GenerateUUID(10)
	}
	return items
}

func generateHashValueForRedis(count int) map[string]interface{} {
	hash := make(map[string]interface{})
	for i := 0; i < count; i++ {
		key := utility.GenerateUUID(10)
		value := utility.GenerateUUID(10)
		hash[key] = value
	}
	return hash
}

func generateSetValueForRedis(count int) []interface{} {
	return generateListValueForRedis(count)
}

func generateZSetValueForRedis(count int) ([]*redis.Z, map[string]float64) {
	zset := make([]*redis.Z, count)
	m := make(map[string]float64)
	for i := 0; i < count; i++ {
		member := utility.GenerateUUID(10)
		score := generateRandFloat(0, 100)
		z := &redis.Z{Member: member, Score: score}
		zset[i] = z
		m[member] = score
	}
	return zset, m
}

func TestSerializeNonStringValue(t *testing.T) {
	redisClient := base.GetRedisCluster()

	// test list
	testListItems := []struct {
		key   string
		count int
	}{
		{
			key:   "{a}:serialize_list1",
			count: 50,
		}, {
			key:   "{a}:serialize_list2",
			count: 99,
		}, {
			key:   "{a}:serialize_list3",
			count: 100,
		}, {
			key:   "{a}:serialize_list4",
			count: 200,
		}, {
			key:   "{a}:serialize_list5",
			count: 230,
		}, {
			key:   "{a}:serialize_list6",
			count: 1299,
		},
	}
	for _, item := range testListItems {
		key := item.key
		defer testEmptyKeysInRedis(key)
		values := generateListValueForRedis(item.count)
		redisClient.RPush(testContextTODO, key, values...).Result()
		result, err := serializeNonStringValue(key, listType)
		assert.Nil(t, err)
		assert.Equal(t, len(values), len(result))
		for index, value := range values {
			assert.Equal(t, value.(string), result[index])
		}
	}

	// test hash
	testHashItems := []struct {
		key   string
		count int
	}{
		{
			key:   "{a}:serialize_hash1",
			count: 50,
		}, {
			key:   "{a}:serialize_hash2",
			count: 99,
		}, {
			key:   "{a}:serialize_hash3",
			count: 100,
		}, {
			key:   "{a}:serialize_hash4",
			count: 200,
		}, {
			key:   "{a}:serialize_hash5",
			count: 230,
		}, {
			key:   "{a}:serialize_hash6",
			count: 1299,
		},
	}
	for _, item := range testHashItems {
		key := item.key
		defer testEmptyKeysInRedis(key)
		values := generateHashValueForRedis(item.count)
		redisClient.HSet(testContextTODO, key, values).Result()
		result, err := serializeNonStringValue(key, hashType)
		assert.Nil(t, err)
		assert.Equal(t, len(values)*2, len(result))
		for index := 0; index < len(result)-1; index += 2 {
			key := result[index]
			value := result[index+1]
			assert.Equal(t, values[key].(string), value)
		}
	}

	// test set
	testSetItems := []struct {
		key   string
		count int
	}{
		{
			key:   "{a}:serialize_set1",
			count: 50,
		}, {
			key:   "{a}:serialize_set2",
			count: 99,
		}, {
			key:   "{a}:serialize_set3",
			count: 100,
		}, {
			key:   "{a}:serialize_set4",
			count: 200,
		}, {
			key:   "{a}:serialize_set5",
			count: 230,
		}, {
			key:   "{a}:serialize_set6",
			count: 1299,
		},
	}
	for _, item := range testSetItems {
		key := item.key
		defer testEmptyKeysInRedis(key)
		values := generateSetValueForRedis(item.count)
		redisClient.SAdd(testContextTODO, key, values...).Result()
		result, err := serializeNonStringValue(key, setType)
		assert.Nil(t, err)
		assert.Equal(t, len(values), len(result))
		for _, item := range values {
			assert.True(t, utility.StringSliceContains(result, item.(string)))
		}
	}

	// test zset
	testZSetItems := []struct {
		key   string
		count int
	}{
		{
			key:   "{a}:serialize_zset1",
			count: 50,
		}, {
			key:   "{a}:serialize_zset2",
			count: 99,
		}, {
			key:   "{a}:serialize_zset3",
			count: 100,
		}, {
			key:   "{a}:serialize_zset4",
			count: 200,
		}, {
			key:   "{a}:serialize_zset5",
			count: 230,
		}, {
			key:   "{a}:serialize_zset6",
			count: 1299,
		},
	}
	for _, item := range testZSetItems {
		key := item.key
		defer testEmptyKeysInRedis(key)
		values, m := generateZSetValueForRedis(item.count)
		redisClient.ZAdd(testContextTODO, key, values...).Result()
		result, err := serializeNonStringValue(key, zsetType)
		assert.Nil(t, err)
		assert.Equal(t, len(m)*2, len(result))
		for i := 0; i < len(result)-1; i += 2 {
			member := result[i]
			score, err := strconv.ParseFloat(result[i+1], 64)
			assert.Nil(t, err)
			assert.True(t, math.Abs(m[member]-score) < 0.01)
		}
	}
}

func TestGetValueFromRedis(t *testing.T) {
	//get not exist key
	key := "{a}not_exist"
	value, err := getValueFromRedis(key)
	assert.Nil(t, err)
	assert.Equal(t, "", value.Type)
	assert.Equal(t, "", value.Value)
	assert.Equal(t, int64(0), value.ExpireTs)
	assert.Equal(t, int64(0), value.SyncedTs)

	// get a string key
	key = "{b}string"
	stringValue := "abc"
	client := base.GetRedisCluster()
	defer client.Del(context.TODO(), key)
	client.Set(context.TODO(), key, stringValue, 0)
	value, err = getValueFromRedis(key)
	assert.Nil(t, err)
	assert.Equal(t, stringType, value.Type)
	assert.Equal(t, stringValue, value.Value)
	assert.Equal(t, int64(0), value.ExpireTs)
	assert.Greater(t, value.SyncedTs, int64(0))

	// get a string key with expiration
	key = "{b}string2"
	stringValue = "abcd"
	defer client.Del(context.TODO(), key)
	client.Set(context.TODO(), key, stringValue, 10*time.Second)
	value, err = getValueFromRedis(key)
	assert.Nil(t, err)
	assert.Equal(t, stringType, value.Type)
	assert.Equal(t, stringValue, value.Value)
	assert.Greater(t, value.ExpireTs, int64(0))
	assert.Greater(t, value.SyncedTs, int64(0))

	// get a list key
	key = "{b}list"
	listValue := []interface{}{"a", "b", "c", "d"}
	defer client.Del(context.TODO(), key)
	client.RPush(context.TODO(), key, listValue...)
	value, err = getValueFromRedis(key)
	assert.Nil(t, err)
	assert.Equal(t, listType, value.Type)

	v := make([]interface{}, 0)
	json.Unmarshal([]byte(value.Value), &v)
	assert.Equal(t, listValue, v)

	assert.Equal(t, int64(0), value.ExpireTs)
	assert.Greater(t, value.SyncedTs, int64(0))

	// get a set key
	key = "{b}set"
	setValue := []interface{}{"a", "b", "c", "d"}
	defer client.Del(context.TODO(), key)
	client.SAdd(context.TODO(), key, setValue...)
	value, err = getValueFromRedis(key)
	assert.Nil(t, err)
	assert.Equal(t, setType, value.Type)

	v = make([]interface{}, 0)
	json.Unmarshal([]byte(value.Value), &v)
	assert.Equal(t, len(setValue), len(v))
	for _, item := range v {
		assert.True(t, utility.StringSliceContains(utility.AnyArrayToStringArray(setValue), item.(string)))
	}

	assert.Equal(t, int64(0), value.ExpireTs)
	assert.Greater(t, value.SyncedTs, int64(0))

	// get a hash key
	key = "{b}hash"
	hashValue := map[string]interface{}{"a": "b", "c": "d", "e": "f"}
	defer client.Del(context.TODO(), key)
	client.HSet(context.TODO(), key, hashValue)
	value, err = getValueFromRedis(key)
	assert.Nil(t, err)
	assert.Equal(t, hashType, value.Type)

	v = make([]interface{}, 0)
	json.Unmarshal([]byte(value.Value), &v)
	assert.Equal(t, len(hashValue)*2, len(v))
	for i := 0; i < len(v)-1; i += 2 {
		hk := v[i].(string)
		hv := v[i+1].(string)
		assert.Equal(t, hashValue[hk], hv)
	}

	assert.Equal(t, int64(0), value.ExpireTs)
	assert.Greater(t, value.SyncedTs, int64(0))

	// get a zset key, with expiration
	key = "{b}zset"
	zsetValue := map[string]redis.Z{
		"a": {Member: "a", Score: 1.1},
		"b": {Member: "b", Score: 2.2},
		"c": {Member: "c", Score: 3.3},
	}
	defer client.Del(context.TODO(), key)
	for _, z := range zsetValue {
		client.ZAdd(context.TODO(), key, &z)
	}
	client.Expire(context.TODO(), key, 10*time.Second)
	value, err = getValueFromRedis(key)
	assert.Nil(t, err)
	assert.Equal(t, zsetType, value.Type)

	v = make([]interface{}, 0)
	json.Unmarshal([]byte(value.Value), &v)
	assert.Equal(t, len(zsetValue)*2, len(v))

	for i := 0; i < len(v)-1; i += 2 {
		member := v[i].(string)
		score, err := strconv.ParseFloat(v[i+1].(string), 64)
		assert.Nil(t, err)
		assert.True(t, math.Abs(zsetValue[member].Score-score) < 0.01)
	}

	assert.Greater(t, value.ExpireTs, int64(0))
	assert.Greater(t, value.SyncedTs, int64(0))
}

func TestIsValueEqual(t *testing.T) {
	equal, err := isValueEqual(
		stringType,
		[]string{"abc"},
		RedisValue{Type: stringType, Value: "abc"})
	assert.Nil(t, err)
	assert.True(t, equal)

	equal, err = isValueEqual(
		stringType,
		[]string{"abc"},
		RedisValue{Type: stringType, Value: "abcd"})
	assert.Nil(t, nil)
	assert.False(t, equal)

	listValue := []string{"a", "b", "x", "y", "z"}
	v, _ := json.Marshal(listValue)
	value := RedisValue{
		Type:  listType,
		Value: string(v),
	}
	equal, err = isValueEqual(listType, listValue, value)
	assert.Nil(t, err)
	assert.True(t, equal)

	setValue := []string{"a", "b", "x", "y", "z", "1", "2", "3"}
	v, _ = json.Marshal(setValue)
	value = RedisValue{
		Type:  setType,
		Value: string(v),
	}
	equal, err = isValueEqual(setType, setValue, value)
	assert.Nil(t, err)
	assert.True(t, equal)

	hashValue := []string{"1", "2", "3", "4", "a", "b", "b", "a"}
	v, _ = json.Marshal(hashValue)
	value = RedisValue{
		Type:  hashType,
		Value: string(v),
	}
	equal, err = isValueEqual(hashType, hashValue, value)
	assert.Nil(t, err)
	assert.True(t, equal)

	zsetValue := []string{"a", "1.2", "b", "1.3", "d", "2.34"}
	v, _ = json.Marshal(zsetValue)
	value = RedisValue{
		Type:  zsetType,
		Value: string(v),
	}
	equal, err = isValueEqual(zsetType, zsetValue, value)
	assert.Nil(t, err)
	assert.True(t, equal)
}
