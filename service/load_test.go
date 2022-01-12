package service

import (
	"bytepower_room/base"
	"bytepower_room/commands"
	"bytepower_room/utility"
	"context"
	"math"
	"strconv"

	"errors"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

var testContextTODO = context.TODO()

func TestMain(m *testing.M) {
	configFile := "../test/config.yaml"
	if err := base.InitRoomServer(configFile); err != nil {
		panic(err)
	}
	if err := base.InitRoomTask(configFile); err != nil {
		panic(err)
	}
	code := m.Run()
	os.Exit(code)
}

func testEmptyKeysInRedis(keys ...string) {
	redisCluster := base.GetServerDependency().Redis
	for _, key := range keys {
		hashTag := commands.ExtractHashTagFromKey(key)
		metaKey := getHashTagMetaKey(hashTag)
		redisCluster.Del(contextTODO, key, metaKey)
	}
}

func testEmptyRoomDataRecordInDatabase(hashTag string) {
	db := base.GetServerDependency().DB
	model := &roomDataModelV2{HashTag: hashTag}
	query, _ := db.Model(model)
	query.WherePK().ForceDelete()
}

func testEmptyHashTagKeysRecordInDB(hashTag string) {
	db := base.GetServerDependency().DB
	model := &roomHashTagKeys{HashTag: hashTag}
	query, _ := db.Model(model)
	query.WherePK().ForceDelete()
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

func testSetMetaKeyCleaned(hashTag string) {
	redisCluster := base.GetServerDependency().Redis
	metaKey := getHashTagMetaKey(hashTag)
	redisCluster.Del(context.TODO(), metaKey)
}

func testCleanLocalloadedCache(hashTag string) {
	cacheService := base.GetHashTagLoadedCache()
	cacheService.Delete(hashTag)
}

func TestLoadKeyNotExist(t *testing.T) {
	hashTag := "a"
	currentTime := time.Now()
	key := "{a}:does_not_exist"
	defer testEmptyKeysInRedis(key)
	testCleanLocalloadedCache(hashTag)
	err := Load(base.GetServerDependency(), hashTag, currentTime, base.HashTagAccessModeRead)
	assert.Nil(t, err)

	redisCluster := base.GetServerDependency().Redis
	_, err = redisCluster.Get(testContextTODO, key).Result()
	assert.Equal(t, redis.Nil, err)

	loaded, err := redisCluster.HGet(testContextTODO, getHashTagMetaKey(hashTag), HashTagMetaInfoStatusFieldName).Result()
	assert.Nil(t, err)
	assert.Equal(t, HashTagStatusLoaded, loaded)

	_, err = redisCluster.Get(testContextTODO, getHashTagLockKey(hashTag)).Result()
	assert.Equal(t, redis.Nil, err)
}

func testInsertRoomData(hashTag string, value map[string]RedisValue) *roomDataModelV2 {
	db := base.GetServerDependency().DB
	model := &roomDataModelV2{HashTag: hashTag, Value: value}
	query, _ := db.Model(model)
	query.Returning("*").Insert()
	return model
}

func testMergeMaps(m1, m2 map[string]RedisValue) map[string]RedisValue {
	m := make(map[string]RedisValue)
	for key, value := range m1 {
		m[key] = value
	}
	for key, value := range m2 {
		m[key] = value
	}
	return m
}

func TestLoadKeyString(t *testing.T) {
	hashTag := "a"
	currentTime := time.Now()
	expireTimeValid := currentTime.Add(100 * time.Second)
	expireTsValid := utility.TimestampInMS(expireTimeValid)
	expireTimeInvalid := currentTime.Add(-100 * time.Second)
	expireTsInvalid := utility.TimestampInMS(expireTimeInvalid)
	validValue := map[string]RedisValue{
		"{a}:string:expire1":  {Type: stringType, Value: "value1", ExpireTs: expireTsValid},
		"{a}:string:expire2":  {Type: stringType, Value: "value2", ExpireTs: expireTsValid},
		"{a}:string:noexpire": {Type: stringType, Value: "value_noexpire", ExpireTs: 0},
	}
	expireValue := map[string]RedisValue{
		"{a}:string:expire3": {Type: stringType, Value: "value3", ExpireTs: expireTsInvalid},
		"{a}:string:expire4": {Type: stringType, Value: "value4", ExpireTs: expireTsInvalid},
	}
	value := testMergeMaps(validValue, expireValue)

	for key := range value {
		defer testEmptyKeysInRedis(key)
	}
	defer testEmptyRoomDataRecordInDatabase(hashTag)
	// Insert data
	testInsertRoomData(hashTag, value)
	testSetMetaKeyCleaned(hashTag)

	// load data
	testCleanLocalloadedCache(hashTag)
	err := Load(base.GetServerDependency(), hashTag, currentTime, base.HashTagAccessModeRead)
	assert.Nil(t, err)

	redisCluster := base.GetServerDependency().Redis
	for key, value := range validValue {
		v, err := redisCluster.Get(testContextTODO, key).Result()
		assert.Nil(t, err)
		assert.Equal(t, value.Value, v)

		d, err := redisCluster.TTL(testContextTODO, key).Result()
		assert.Nil(t, err)
		if value.ExpireTs > 0 {
			assert.Greater(t, int64(d), int64(0))
		} else {
			assert.Equal(t, int64(-1), int64(d))
		}
	}

	for key := range expireValue {
		_, err := redisCluster.Get(testContextTODO, key).Result()
		assert.Equal(t, redis.Nil, err)
	}

	status, err := redisCluster.HGet(testContextTODO, getHashTagMetaKey(hashTag), HashTagMetaInfoStatusFieldName).Result()
	assert.Nil(t, err)
	assert.Equal(t, HashTagStatusLoaded, status)

	_, err = redisCluster.Get(testContextTODO, getHashTagLockKey(hashTag)).Result()
	assert.Equal(t, redis.Nil, err)
}

func testGenerateListValue(count int) string {
	items := make([]string, count)
	for i := 0; i < count; i++ {
		items[i] = utility.GenerateUUID(10)
	}
	value, _ := json.Marshal(items)
	return string(value)
}

func TestLoadKeyList(t *testing.T) {
	hashTag := "a"
	currentTime := time.Now()
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
	defer testEmptyRoomDataRecordInDatabase(hashTag)

	value := make(map[string]RedisValue, 0)

	for _, item := range testItems {
		k := item.key
		defer testEmptyKeysInRedis(k)
		v := testGenerateListValue(item.count)
		value[k] = RedisValue{Type: listType, Value: v, ExpireTs: 0}
	}

	testInsertRoomData(hashTag, value)
	testSetMetaKeyCleaned(hashTag)

	testCleanLocalloadedCache(hashTag)
	err := Load(base.GetServerDependency(), hashTag, currentTime, base.HashTagAccessModeRead)
	assert.Nil(t, err)

	redisCluster := base.GetServerDependency().Redis
	for _, item := range testItems {
		key := item.key
		b, err := redisCluster.LRange(testContextTODO, key, 0, -1).Result()
		assert.Nil(t, err)
		v, _ := json.Marshal(b)
		assert.Equal(t, value[key].Value, string(v))

		duration, err := redisCluster.TTL(testContextTODO, key).Result()
		assert.Nil(t, err)
		assert.Equal(t, time.Duration(-1), duration)
	}

	status, err := redisCluster.HGet(testContextTODO, getHashTagMetaKey(hashTag), HashTagMetaInfoStatusFieldName).Result()
	assert.Nil(t, err)
	assert.Equal(t, HashTagStatusLoaded, status)

	_, err = redisCluster.Get(testContextTODO, getHashTagLockKey(hashTag)).Result()
	assert.Equal(t, redis.Nil, err)
}

func testGenerateHashValue(count int) (string, map[string]string) {
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
	hashTag := "a"
	currentTime := time.Now()
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

	defer testEmptyRoomDataRecordInDatabase(hashTag)

	value := make(map[string]RedisValue, 0)
	hashes := make(map[string]map[string]string)

	for _, item := range testItems {
		k := item.key
		defer testEmptyKeysInRedis(k)
		v, h := testGenerateHashValue(item.count)
		hashes[k] = h
		value[k] = RedisValue{Type: hashType, Value: v, ExpireTs: 0}
	}

	testInsertRoomData(hashTag, value)
	testSetMetaKeyCleaned(hashTag)

	testCleanLocalloadedCache(hashTag)
	err := Load(base.GetServerDependency(), hashTag, currentTime, base.HashTagAccessModeRead)
	assert.Nil(t, err)

	redisCluster := base.GetServerDependency().Redis
	for _, item := range testItems {
		key := item.key
		hash := hashes[key]
		m, err := redisCluster.HGetAll(testContextTODO, key).Result()
		assert.Nil(t, err)
		assert.Equal(t, len(hash), len(m))
		for key, value := range hash {
			assert.Equal(t, value, m[key])
		}

		duration, err := redisCluster.TTL(testContextTODO, key).Result()
		assert.Nil(t, err)
		assert.Equal(t, time.Duration(-1), duration)
	}
	status, err := redisCluster.HGet(testContextTODO, getHashTagMetaKey(hashTag), HashTagMetaInfoStatusFieldName).Result()
	assert.Nil(t, err)
	assert.Equal(t, HashTagStatusLoaded, status)

	_, err = redisCluster.Get(testContextTODO, getHashTagLockKey(hashTag)).Result()
	assert.Equal(t, redis.Nil, err)
}

func testGenerateSetValue(count int) (string, []string) {
	items := make([]string, count)
	for i := 0; i < count; i++ {
		items[i] = utility.GenerateUUID(10)
	}
	value, _ := json.Marshal(items)
	return string(value), items
}

func TestLoadKeySet(t *testing.T) {
	hashTag := "a"
	currentTime := time.Now()
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
	defer testEmptyRoomDataRecordInDatabase(hashTag)

	value := make(map[string]RedisValue, 0)
	sets := make(map[string][]string, 0)

	for _, item := range testItems {
		k := item.key
		defer testEmptyKeysInRedis(k)
		v, s := testGenerateSetValue(item.count)
		sets[k] = s
		value[k] = RedisValue{Type: setType, Value: v, ExpireTs: 0}
	}

	testInsertRoomData(hashTag, value)
	testSetMetaKeyCleaned(hashTag)

	testCleanLocalloadedCache(hashTag)
	err := Load(base.GetServerDependency(), hashTag, currentTime, base.HashTagAccessModeRead)
	assert.Nil(t, err)

	redisCluster := base.GetServerDependency().Redis

	for _, item := range testItems {
		key := item.key
		set := sets[key]
		m, err := redisCluster.SMembers(testContextTODO, key).Result()
		assert.Nil(t, err)
		assert.Equal(t, len(set), len(m))
		for _, value := range set {
			assert.True(t, utility.StringSliceContains(m, value))
		}

		duration, err := redisCluster.TTL(testContextTODO, key).Result()
		assert.Nil(t, err)
		assert.Equal(t, time.Duration(-1), duration)
	}
	status, err := redisCluster.HGet(testContextTODO, getHashTagMetaKey(hashTag), HashTagMetaInfoStatusFieldName).Result()
	assert.Nil(t, err)
	assert.Equal(t, HashTagStatusLoaded, status)

	_, err = redisCluster.Get(testContextTODO, getHashTagLockKey(hashTag)).Result()
	assert.Equal(t, redis.Nil, err)
}

func testGenerateZSetValue(count int) (string, map[string]float64) {
	items := make([]string, count*2)
	zset := make(map[string]float64)
	for i := 0; i < count*2-1; i += 2 {
		item := utility.GenerateUUID(10)
		score := testGenerateRandFloat(0, 100)
		items[i] = item
		items[i+1] = fmt.Sprintf("%g", score)
		zset[item] = score
	}
	value, _ := json.Marshal(items)
	return string(value), zset
}

func testGenerateRandFloat(min, max float64) float64 {
	rand.Seed(time.Now().UnixNano())
	return min + rand.Float64()*(max-min)
}

func TestLoadKeyZSet(t *testing.T) {
	hashTag := "a"
	currentTime := time.Now()
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
	defer testEmptyRoomDataRecordInDatabase(hashTag)

	value := make(map[string]RedisValue, 0)
	zsets := make(map[string]map[string]float64, 0)

	for _, item := range testItems {
		k := item.key
		defer testEmptyKeysInRedis(k)
		v, s := testGenerateZSetValue(item.count)
		zsets[k] = s
		value[k] = RedisValue{Type: zsetType, Value: v, ExpireTs: 0}
	}

	testInsertRoomData(hashTag, value)
	testSetMetaKeyCleaned(hashTag)

	testCleanLocalloadedCache(hashTag)
	err := Load(base.GetServerDependency(), hashTag, currentTime, base.HashTagAccessModeRead)
	assert.Nil(t, err)

	redisCluster := base.GetServerDependency().Redis

	for _, item := range testItems {
		key := item.key
		zset := zsets[key]
		zset2, err := redisCluster.ZRangeWithScores(testContextTODO, key, 0, -1).Result()
		assert.Nil(t, err)
		assert.Equal(t, len(zset), len(zset2))
		for _, value := range zset2 {
			member, ok := value.Member.(string)
			assert.True(t, ok)
			assert.True(t, math.Abs(zset[member]-value.Score) < 0.001)
		}

		duration, err := redisCluster.TTL(testContextTODO, key).Result()
		assert.Nil(t, err)
		assert.Equal(t, time.Duration(-1), duration)
	}
	status, err := redisCluster.HGet(testContextTODO, getHashTagMetaKey(hashTag), HashTagMetaInfoStatusFieldName).Result()
	assert.Nil(t, err)
	assert.Equal(t, HashTagStatusLoaded, status)

	_, err = redisCluster.Get(testContextTODO, getHashTagLockKey(hashTag)).Result()
	assert.Equal(t, redis.Nil, err)
}

func TestHashTagLockRequireAndRelease(t *testing.T) {
	dep := base.GetServerDependency()
	hashTag, err := NewHashTag("test", dep)
	assert.Nil(t, err)
	lockKey := hashTag.lockKey()
	defer dep.Redis.Del(contextTODO, lockKey)

	err = hashTag.acquireLoadLock()
	assert.Nil(t, err)

	ttl, _ := dep.Redis.TTL(contextTODO, lockKey).Result()
	assert.Greater(t, int64(ttl), int64(0))

	err = hashTag.acquireLoadLock()
	assert.Equal(t, errLoadKeysLockFailed, err)

	hashTag.releaseLoadLock()
	ttl, _ = dep.Redis.TTL(contextTODO, lockKey).Result()
	assert.Equal(t, int64(-2), int64(ttl))

	err = hashTag.acquireLoadLock()
	assert.Nil(t, err)
	ttl, _ = dep.Redis.TTL(contextTODO, lockKey).Result()
	assert.Greater(t, int64(ttl), int64(0))
}

func TestMetaKeyGetLoadStatus(t *testing.T) {
	dep := base.GetServerDependency()
	hashTag := "test"
	metaKey := getHashTagMetaKey(hashTag)
	defer dep.Redis.Del(context.Background(), metaKey)
	meta, err := NewHashTagMetaInfo(hashTag, dep)
	assert.Nil(t, err)

	status, err := meta.GetLoadStatus()
	assert.Nil(t, err)
	assert.Equal(t, HashTagStatusNotExisted, status)

	dep.Redis.HSet(context.Background(), metaKey, "xxx", "1")
	status, err = meta.GetLoadStatus()
	assert.Nil(t, err)
	assert.Equal(t, HashTagStatusNotExisted, status)
}

func TestHashTagCleanKeys(t *testing.T) {
	keys := []string{"{a}", "b{a}", "{a}bc", "x{a}bc"}
	dep := base.GetServerDependency()
	for _, key := range keys {
		dep.Redis.Set(context.TODO(), key, "existed", 0)
		defer dep.Redis.Del(context.TODO(), key)
	}
	tag := "a"
	hashTag, _ := NewHashTag(tag, dep)
	err := hashTag.CleanKeys(keys...)
	assert.Nil(t, err)
	metaKey := getHashTagMetaKey(tag)
	defer dep.Redis.Del(context.TODO(), metaKey)
	_, err = dep.Redis.HGet(context.TODO(), metaKey, HashTagMetaInfoStatusFieldName).Result()
	assert.Equal(t, redis.Nil, err)
	for _, key := range keys {
		existed, _ := dep.Redis.Exists(context.TODO(), key).Result()
		assert.Equal(t, int64(0), existed)
	}
}

func TestLoadSetToRedis(t *testing.T) {
	cases := []struct {
		key     string
		slices  [][]interface{}
		tl      time.Duration
		members []string
	}{
		{
			key:     "{a}b",
			slices:  [][]interface{}{{"a"}, {"b"}},
			tl:      0,
			members: []string{},
		}, {
			key:     "{b}c",
			slices:  [][]interface{}{{"a", "b", "f"}, {"c"}, {"d", "e", "f"}},
			tl:      -1,
			members: []string{"a", "b", "c", "d", "e", "f"},
		}, {
			key:     "{c}d",
			slices:  [][]interface{}{{"a", "b"}},
			tl:      5 * time.Second,
			members: []string{"a", "b"},
		},
	}

	redisCluster := base.GetServerDependency().Redis
	ctx := context.TODO()
	for _, c := range cases {
		defer redisCluster.Del(ctx, c.key)
		err := loadSetToRedis(ctx, redisCluster, c.key, c.slices, c.tl)
		assert.Nil(t, err)
		length, _ := redisCluster.SCard(ctx, c.key).Result()
		assert.Equal(t, len(c.members), int(length))
		members, _ := redisCluster.SMembers(ctx, c.key).Result()
		for _, member := range c.members {
			assert.True(t, utility.StringSliceContains(members, member))
		}
	}
}

func TestLoadListToRedis(t *testing.T) {
	cases := []struct {
		key     string
		slices  [][]interface{}
		ttl     time.Duration
		members []string
	}{
		{
			key:     "{a}b",
			slices:  [][]interface{}{{"a"}, {"b"}},
			ttl:     0,
			members: []string{},
		}, {
			key:     "{b}c",
			slices:  [][]interface{}{{"a", "b", "f"}, {"c"}, {"d", "e", "f"}},
			ttl:     -1,
			members: []string{"a", "b", "f", "c", "d", "e", "f"},
		}, {
			key:     "{c}d",
			slices:  [][]interface{}{{"a", "b"}},
			ttl:     5 * time.Second,
			members: []string{"a", "b"},
		},
	}
	redisCluster := base.GetServerDependency().Redis
	ctx := context.TODO()
	for _, c := range cases {
		defer redisCluster.Del(ctx, c.key)
		err := loadListToRedis(ctx, redisCluster, c.key, c.slices, c.ttl)
		assert.Nil(t, err)
		length, _ := redisCluster.LLen(ctx, c.key).Result()
		assert.Equal(t, len(c.members), int(length))
		members, _ := redisCluster.LRange(ctx, c.key, 0, -1).Result()
		for index, member := range members {
			assert.Equal(t, c.members[index], member)
		}
	}
}

func TestLoadHashToRedis(t *testing.T) {
	cases := []struct {
		key     string
		slices  [][]interface{}
		ttl     time.Duration
		members map[string]string
	}{
		{
			key:     "{a}b",
			slices:  [][]interface{}{{"a", "b"}},
			ttl:     0,
			members: map[string]string{},
		}, {
			key:     "{b}c",
			slices:  [][]interface{}{{"a", "b", "c", "d"}, {"e", "f"}, {"g", "h"}},
			ttl:     -1,
			members: map[string]string{"a": "b", "c": "d", "e": "f", "g": "h"},
		}, {
			key:     "{c}d",
			slices:  [][]interface{}{{"a", "b"}},
			ttl:     5 * time.Second,
			members: map[string]string{"a": "b"},
		},
	}
	redisCluster := base.GetServerDependency().Redis
	ctx := context.TODO()
	for _, c := range cases {
		defer redisCluster.Del(ctx, c.key)
		err := loadHashToRedis(ctx, redisCluster, c.key, c.slices, c.ttl)
		assert.Nil(t, err)
		length, _ := redisCluster.HLen(ctx, c.key).Result()
		assert.Equal(t, len(c.members), int(length))
		members, _ := redisCluster.HGetAll(ctx, c.key).Result()
		for key, value := range members {
			assert.Equal(t, c.members[key], value)
		}
	}
}

func TestLoadZsetToRedis(t *testing.T) {
	cases := []struct {
		key     string
		slices  [][]interface{}
		ttl     time.Duration
		members map[string]string
	}{
		{
			key:     "{a}b",
			slices:  [][]interface{}{{"a", "1.5"}},
			ttl:     0,
			members: map[string]string{},
		}, {
			key:     "{b}c",
			slices:  [][]interface{}{{"a", "1.5", "c", "1.25"}, {"e", "1.33"}, {"g", "2.55"}},
			ttl:     -1,
			members: map[string]string{"a": "1.5", "c": "1.25", "e": "1.33", "g": "2.55"},
		}, {
			key:     "{c}d",
			slices:  [][]interface{}{{"a", "10.89"}},
			ttl:     5 * time.Second,
			members: map[string]string{"a": "10.89"},
		},
	}
	redisCluster := base.GetServerDependency().Redis
	ctx := context.TODO()
	for _, c := range cases {
		defer redisCluster.Del(ctx, c.key)
		err := loadZSetToRedis(ctx, redisCluster, c.key, c.slices, c.ttl)
		assert.Nil(t, err)
		length, _ := redisCluster.ZCard(ctx, c.key).Result()
		assert.Equal(t, len(c.members), int(length))
		members, _ := redisCluster.ZRangeWithScores(ctx, c.key, 0, -1).Result()
		for _, member := range members {
			key := member.Member
			score := member.Score
			s, _ := strconv.ParseFloat(c.members[key.(string)], 64)
			assert.Equal(t, s, score)
		}
	}
}

func TestHashTagLoadWithTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Nanosecond)
	defer cancel()
	time.Sleep(10 * time.Nanosecond)

	tag := "abc"
	dep := base.GetServerDependency()
	hashTag, _ := NewHashTag(tag, dep)
	count, err := hashTag.loadKeys(ctx)
	assert.Equal(t, context.DeadlineExceeded, err)
	assert.Equal(t, 0, count)
}

func TestHashTagMetaUpdateAccessTime(t *testing.T) {
	dep := base.GetServerDependency()
	// update status, at, version field
	hashTag := "abc"
	meta, _ := NewHashTagMetaInfo(hashTag, dep)
	defer testEmptyKeysInRedis(meta.metaKey)
	accessTime := time.Now()
	accessTs := utility.TimestampInMS(accessTime)
	err := meta.UpdateAccessTime(accessTime, base.HashTagAccessModeRead)
	assert.Nil(t, err)
	result, _ := dep.Redis.HGetAll(context.TODO(), meta.metaKey).Result()
	assert.Equal(t, 3, len(result))
	assert.Equal(t, HashTagStatusLoaded, result[HashTagMetaInfoStatusFieldName])
	assert.Equal(t, fmt.Sprintf("%d", accessTs), result[HashTagMetaInfoAccessTimeFieldName])
	assert.Equal(t, "0", result[HashTagMetaInfoVersionFieldName])

	// update status, at, version, wt field
	hashTag = "xyz"
	meta, _ = NewHashTagMetaInfo(hashTag, dep)
	defer testEmptyKeysInRedis(meta.metaKey)
	accessTime = time.Now()
	accessTs = utility.TimestampInMS(accessTime)
	err = meta.UpdateAccessTime(accessTime, base.HashTagAccessModeWrite)
	assert.Nil(t, err)
	result, _ = dep.Redis.HGetAll(context.TODO(), meta.metaKey).Result()
	assert.Equal(t, 4, len(result))
	assert.Equal(t, HashTagStatusLoaded, result[HashTagMetaInfoStatusFieldName])
	assert.Equal(t, fmt.Sprintf("%d", accessTs), result[HashTagMetaInfoAccessTimeFieldName])
	assert.Equal(t, fmt.Sprintf("%d", accessTs), result[HashTagMetaInfoWriteTimeFieldName])
	assert.Equal(t, "1", result[HashTagMetaInfoVersionFieldName])
}

func TestHashTagMetaGetAccessTime(t *testing.T) {
	dep := base.GetServerDependency()
	hashTag := "abc"
	meta, _ := NewHashTagMetaInfo(hashTag, dep)
	defer testEmptyKeysInRedis(meta.metaKey)
	accessTime := time.Now()
	accessTs := utility.TimestampInMS(accessTime)
	_ = meta.UpdateAccessTime(accessTime, base.HashTagAccessModeRead)

	lastAccessTime, err := meta.GetAccessTime()
	assert.Nil(t, err)
	assert.Equal(t, accessTs, lastAccessTime.UnixNano()/1000/1000)
}
