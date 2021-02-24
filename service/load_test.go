package service

import (
	"bytepower_room/base"
	"bytepower_room/utility"
	"context"

	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

var testContextTODO = context.TODO()

func TestMain(m *testing.M) {
	configFile := "../cmd/config.yaml"
	if err := base.InitSyncService(configFile); err != nil {
		panic(err)
	}
	code := m.Run()
	os.Exit(code)
}

func testEmptyKeysInRedis(keys ...string) {
	for _, key := range keys {
		metaKey := getMetaKey(key)
		base.GetRedisCluster().Del(contextTODO, key, metaKey)
	}
}

func testEmptyKeysInDatabase(keys ...string) {
	db := base.GetDBCluster()
	for _, key := range keys {
		model := &roomDataModel{Key: key}
		query, _ := db.Model(model)
		query.WherePK().Delete()
	}
}

type testInsertRoomDataInput struct {
	key      string
	dataType string
	value    *string
	expireAt time.Time
}

func (input testInsertRoomDataInput) check() error {
	if input.key == "" {
		return errors.New("key is empty")
	}
	if !utility.StringSliceContains([]string{stringType, listType, hashType, setType, zsetType}, input.dataType) {
		return errors.New("data type is invalid")
	}
	if input.value == nil {
		return errors.New("value is null")
	}
	return nil
}

func testInsertRoomData(input testInsertRoomDataInput) (*roomDataModel, error) {
	if err := input.check(); err != nil {
		return nil, err
	}
	currentTime := time.Now()
	model := &roomDataModel{
		Key:       input.key,
		Type:      input.dataType,
		Value:     *input.value,
		Deleted:   false,
		UpdatedAt: currentTime,
		SyncedAt:  currentTime,
		CreatedAt: currentTime,
		Version:   1,
	}
	if !input.expireAt.IsZero() {
		model.ExpireAt = input.expireAt
	}
	dbCluster := base.GetDBCluster()
	query, err := dbCluster.Model(model)
	if err != nil {
		return nil, err
	}
	if _, err := query.Returning("*").Insert(); err != nil {
		return nil, err
	}
	return model, nil
}

func TestLoadKeyNotExist(t *testing.T) {
	key := "{a}:does_not_exist"
	defer testEmptyKeysInRedis(key)
	err := loadKey(key)
	assert.Nil(t, err)

	client := base.GetRedisCluster()
	_, err = client.Get(testContextTODO, key).Result()
	assert.Equal(t, redis.Nil, err)

	loaded, err := client.HGet(testContextTODO, getMetaKey(key), "loaded").Result()
	assert.Nil(t, err)
	assert.Equal(t, "1", loaded)

	_, err = client.Get(testContextTODO, getLockKey(key)).Result()
	assert.Equal(t, redis.Nil, err)
}

func TestLoadKeyStringWithExpire(t *testing.T) {
	key := "{a}:string:expire"
	value := "hello world"
	defer testEmptyKeysInRedis(key)
	defer testEmptyKeysInDatabase(key)
	// Insert data
	input := testInsertRoomDataInput{key: key, dataType: stringType, value: &value, expireAt: time.Now().Add(100 * time.Second)}
	testInsertRoomData(input)

	// load data
	err := loadKey(key)
	assert.Nil(t, err)

	client := base.GetRedisCluster()
	v, err := client.Get(testContextTODO, key).Result()
	assert.Nil(t, err)
	assert.Equal(t, value, v)

	duration, err := client.TTL(testContextTODO, key).Result()
	assert.Nil(t, err)
	assert.Greater(t, int64(duration), int64(0))

	loaded, err := client.HGet(testContextTODO, getMetaKey(key), "loaded").Result()
	assert.Nil(t, err)
	assert.Equal(t, "1", loaded)

	_, err = client.Get(testContextTODO, getLockKey(key)).Result()
	assert.Equal(t, redis.Nil, err)
}

func TestLoadKeyString(t *testing.T) {
	key := "{a}:string"
	value := "hello world"
	defer testEmptyKeysInRedis(key)
	defer testEmptyKeysInDatabase(key)
	// Insert data
	input := testInsertRoomDataInput{key: key, dataType: stringType, value: &value}
	testInsertRoomData(input)

	// load data
	err := loadKey(key)
	assert.Nil(t, err)

	client := base.GetRedisCluster()
	v, err := client.Get(testContextTODO, key).Result()
	assert.Nil(t, err)
	assert.Equal(t, value, v)

	duration, err := client.TTL(testContextTODO, key).Result()
	assert.Nil(t, err)
	assert.Equal(t, time.Duration(-1), duration)

	loaded, err := client.HGet(testContextTODO, getMetaKey(key), "loaded").Result()
	assert.Nil(t, err)
	assert.Equal(t, "1", loaded)

	_, err = client.Get(testContextTODO, getLockKey(key)).Result()
	assert.Equal(t, redis.Nil, err)
}

func generateListValue(count int) string {
	items := make([]string, count)
	for i := 0; i < count; i++ {
		items[i] = utility.GenerateUUID(10)
	}
	value, _ := json.Marshal(items)
	return string(value)
}

func TestLoadKeyList(t *testing.T) {
	testItems := []struct {
		key   string
		count int
	}{
		{
			key:   "{a}:list1",
			count: 50,
		}, {
			key:   "{a}:list2",
			count: 99,
		}, {
			key:   "{a}:list3",
			count: 100,
		}, {
			key:   "{a}:list4",
			count: 200,
		}, {
			key:   "{a}:list5",
			count: 230,
		}, {
			key:   "{a}:list6",
			count: 1299,
		},
	}

	for _, item := range testItems {
		key := item.key
		defer testEmptyKeysInRedis(key)
		defer testEmptyKeysInDatabase(key)
		value := generateListValue(item.count)
		// Insert data
		input := testInsertRoomDataInput{key: key, dataType: listType, value: &value}
		testInsertRoomData(input)

		// load data
		err := loadKey(key)
		assert.Nil(t, err)

		client := base.GetRedisCluster()
		b, err := client.LRange(testContextTODO, key, 0, -1).Result()
		assert.Nil(t, err)
		v, _ := json.Marshal(b)
		assert.Equal(t, value, string(v))

		duration, err := client.TTL(testContextTODO, key).Result()
		assert.Nil(t, err)
		assert.Equal(t, time.Duration(-1), duration)

		loaded, err := client.HGet(testContextTODO, getMetaKey(key), "loaded").Result()
		assert.Nil(t, err)
		assert.Equal(t, "1", loaded)

		_, err = client.Get(testContextTODO, getLockKey(key)).Result()
		assert.Equal(t, redis.Nil, err)
	}
}

func generateHashValue(count int) (string, map[string]string) {
	hash := make(map[string]string)
	items := make([]string, count*2)
	for i := 0; i < count*2-1; i += 2 {
		items[i] = utility.GenerateUUID(10)
		items[i+1] = utility.GenerateUUID(10)
		hash[items[i]] = items[i+1]
	}
	value, _ := json.Marshal(items)
	return string(value), hash
}

func TestLoadKeyHash(t *testing.T) {
	testItems := []struct {
		key   string
		count int
	}{
		{
			key:   "{a}:hash1",
			count: 50,
		}, {
			key:   "{a}:hash2",
			count: 99,
		}, {
			key:   "{a}:hash3",
			count: 100,
		}, {
			key:   "{a}:hash4",
			count: 200,
		}, {
			key:   "{a}:hash5",
			count: 230,
		}, {
			key:   "{a}:hash6",
			count: 1299,
		},
	}

	for _, item := range testItems {
		key := item.key
		defer testEmptyKeysInRedis(key)
		defer testEmptyKeysInDatabase(key)
		value, hash := generateHashValue(item.count)
		// Insert data
		input := testInsertRoomDataInput{key: key, dataType: hashType, value: &value}
		testInsertRoomData(input)

		// load data
		err := loadKey(key)
		assert.Nil(t, err)

		client := base.GetRedisCluster()
		m, err := client.HGetAll(testContextTODO, key).Result()
		assert.Nil(t, err)
		assert.Equal(t, len(hash), len(m))
		for key, value := range hash {
			assert.Equal(t, value, m[key])
		}

		duration, err := client.TTL(testContextTODO, key).Result()
		assert.Nil(t, err)
		assert.Equal(t, time.Duration(-1), duration)

		loaded, err := client.HGet(testContextTODO, getMetaKey(key), "loaded").Result()
		assert.Nil(t, err)
		assert.Equal(t, "1", loaded)

		_, err = client.Get(testContextTODO, getLockKey(key)).Result()
		assert.Equal(t, redis.Nil, err)
	}
}

func generateSetValue(count int) (string, []string) {
	items := make([]string, count)
	for i := 0; i < count; i++ {
		items[i] = utility.GenerateUUID(10)
	}
	value, _ := json.Marshal(items)
	return string(value), items
}

func TestLoadKeySet(t *testing.T) {
	testItems := []struct {
		key   string
		count int
	}{
		{
			key:   "{a}:set1",
			count: 50,
		}, {
			key:   "{a}:set2",
			count: 99,
		}, {
			key:   "{a}:set3",
			count: 100,
		}, {
			key:   "{a}:set4",
			count: 200,
		}, {
			key:   "{a}:set5",
			count: 230,
		}, {
			key:   "{a}:set6",
			count: 1299,
		},
	}

	for _, item := range testItems {
		key := item.key
		defer testEmptyKeysInRedis(key)
		defer testEmptyKeysInDatabase(key)
		value, set := generateSetValue(item.count)
		// Insert data
		input := testInsertRoomDataInput{key: key, dataType: setType, value: &value}
		testInsertRoomData(input)

		// load data
		err := loadKey(key)
		assert.Nil(t, err)

		client := base.GetRedisCluster()
		m, err := client.SMembers(testContextTODO, key).Result()
		assert.Nil(t, err)
		assert.Equal(t, len(set), len(m))
		for _, value := range set {
			assert.True(t, utility.StringSliceContains(m, value))
		}

		duration, err := client.TTL(testContextTODO, key).Result()
		assert.Nil(t, err)
		assert.Equal(t, time.Duration(-1), duration)

		loaded, err := client.HGet(testContextTODO, getMetaKey(key), "loaded").Result()
		assert.Nil(t, err)
		assert.Equal(t, "1", loaded)

		_, err = client.Get(testContextTODO, getLockKey(key)).Result()
		assert.Equal(t, redis.Nil, err)
	}
}

func generateZSetValue(count int) (string, map[string]float64) {
	items := make([]string, count*2)
	zset := make(map[string]float64)
	for i := 0; i < count*2-1; i += 2 {
		item := utility.GenerateUUID(10)
		score := generateRandFloat(0, 100)
		items[i] = item
		items[i+1] = fmt.Sprintf("%g", score)
		zset[item] = score
	}
	value, _ := json.Marshal(items)
	return string(value), zset
}

func generateRandFloat(min, max float64) float64 {
	rand.Seed(time.Now().UnixNano())
	return min + rand.Float64()*(max-min)
}

func TestLoadKeyZSet(t *testing.T) {
	testItems := []struct {
		key   string
		count int
	}{
		{
			key:   "{a}:zset1",
			count: 50,
		}, {
			key:   "{a}:zset2",
			count: 99,
		}, {
			key:   "{a}:zset3",
			count: 100,
		}, {
			key:   "{a}:zset4",
			count: 200,
		}, {
			key:   "{a}:zset5",
			count: 230,
		}, {
			key:   "{a}:zset6",
			count: 1299,
		},
	}

	for _, item := range testItems {
		key := item.key
		defer testEmptyKeysInRedis(key)
		defer testEmptyKeysInDatabase(key)
		value, zset := generateZSetValue(item.count)
		// Insert data
		input := testInsertRoomDataInput{key: key, dataType: zsetType, value: &value}
		testInsertRoomData(input)

		// load data
		err := loadKey(key)
		assert.Nil(t, err)

		client := base.GetRedisCluster()
		zset2, err := client.ZRangeWithScores(testContextTODO, key, 0, -1).Result()
		assert.Nil(t, err)
		assert.Equal(t, len(zset), len(zset2))
		for _, value := range zset2 {
			member, ok := value.Member.(string)
			assert.True(t, ok)
			assert.True(t, math.Abs(zset[member]-value.Score) < 0.001)
		}

		duration, err := client.TTL(testContextTODO, key).Result()
		assert.Nil(t, err)
		assert.Equal(t, time.Duration(-1), duration)

		loaded, err := client.HGet(testContextTODO, getMetaKey(key), "loaded").Result()
		assert.Nil(t, err)
		assert.Equal(t, "1", loaded)

		_, err = client.Get(testContextTODO, getLockKey(key)).Result()
		assert.Equal(t, redis.Nil, err)
	}
}

func TestIsKeyNeedLoaded(t *testing.T) {
	client := base.GetRedisCluster()
	key := "{a}:need_loaded"
	metaKey := getMetaKey(key)
	// both key and meta key do not exist
	testEmptyKeysInRedis(key, metaKey)
	needLoaded, err := isKeyNeedLoaded(key)
	assert.True(t, needLoaded)
	assert.Nil(t, err)
	testEmptyKeysInRedis(key, metaKey)

	// key exists, meta key does not exist
	testEmptyKeysInRedis(key, metaKey)
	client.Set(testContextTODO, key, "value", 0)
	needLoaded, err = isKeyNeedLoaded(key)
	assert.False(t, needLoaded)
	assert.Nil(t, err)
	testEmptyKeysInRedis(key, metaKey)

	// key does not exist, meta key exists
	testEmptyKeysInRedis(key, metaKey)
	client.HSet(testContextTODO, metaKey, "loaded", "1")
	needLoaded, err = isKeyNeedLoaded(key)
	assert.False(t, needLoaded)
	assert.Nil(t, err)
	testEmptyKeysInRedis(key, metaKey)

	// both key and meta key exist
	testEmptyKeysInRedis(key, metaKey)
	client.Set(testContextTODO, key, "value", 0)
	client.HSet(testContextTODO, metaKey, "loaded", "1")
	needLoaded, err = isKeyNeedLoaded(key)
	assert.False(t, needLoaded)
	assert.Nil(t, err)
	testEmptyKeysInRedis(key, metaKey)

	// metaKey exist, but loaded filed not exist
	testEmptyKeysInRedis(key, metaKey)
	client.HSet(testContextTODO, metaKey, "loaded_test", "1")
	needLoaded, err = isKeyNeedLoaded(key)
	assert.True(t, needLoaded)
	assert.Nil(t, err)
	testEmptyKeysInRedis(key, metaKey)

	// metaKey exist, but loaded filed != "1"
	testEmptyKeysInRedis(key, metaKey)
	client.HSet(testContextTODO, metaKey, "loaded", "2")
	needLoaded, err = isKeyNeedLoaded(key)
	assert.True(t, needLoaded)
	assert.Nil(t, err)
	testEmptyKeysInRedis(key, metaKey)
}
