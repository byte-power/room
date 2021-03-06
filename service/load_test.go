package service

import (
	"bytepower_room/base"
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
	configFile := "../cmd/config.yaml"
	if err := base.InitSyncService(configFile); err != nil {
		panic(err)
	}
	code := m.Run()
	os.Exit(code)
}

func testEmptyKeysInRedis(keys ...string) {
	for _, key := range keys {
		hashTag := extractHashTagFromKey(key)
		metaKey := getHashTagMetaKey(hashTag)
		base.GetRedisCluster().Del(contextTODO, key, metaKey)
	}
}

func testEmptyRoomDataRecordInDatabase(hashTag string) {
	db := base.GetDBCluster()
	model := &roomDataModelV2{HashTag: hashTag}
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
	client := base.GetRedisCluster()
	metaKey := getHashTagMetaKey(hashTag)
	client.HSet(context.TODO(), metaKey, HashTagMetaInfoStatusFieldName, HashTagStatusCleaned)
}

func TestLoadKeyNotExist(t *testing.T) {
	hashTag := "a"
	key := "{a}:does_not_exist"
	defer testEmptyKeysInRedis(key)
	err := Load(hashTag)
	assert.Nil(t, err)

	client := base.GetRedisCluster()
	_, err = client.Get(testContextTODO, key).Result()
	assert.Equal(t, redis.Nil, err)

	loaded, err := client.HGet(testContextTODO, getHashTagMetaKey(hashTag), HashTagMetaInfoStatusFieldName).Result()
	assert.Nil(t, err)
	assert.Equal(t, HashTagStatusLoaded, loaded)

	_, err = client.Get(testContextTODO, getHashTagLockKey(hashTag)).Result()
	assert.Equal(t, redis.Nil, err)
}

func testInsertRoomData(hashTag string, value map[string]RedisValue) *roomDataModelV2 {
	db := base.GetDBCluster()
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
	currentTs := utility.TimestampInMS(currentTime)
	expireTimeValid := currentTime.Add(100 * time.Second)
	expireTsValid := utility.TimestampInMS(expireTimeValid)
	expireTimeInvalid := currentTime.Add(-100 * time.Second)
	expireTsInvalid := utility.TimestampInMS(expireTimeInvalid)
	validValue := map[string]RedisValue{
		"{a}:string:expire1":  {Type: stringType, Value: "value1", SyncedTs: currentTs, ExpireTs: expireTsValid},
		"{a}:string:expire2":  {Type: stringType, Value: "value2", SyncedTs: currentTs, ExpireTs: expireTsValid},
		"{a}:string:noexpire": {Type: stringType, Value: "value_noexpire", SyncedTs: currentTs, ExpireTs: 0},
	}
	expireValue := map[string]RedisValue{
		"{a}:string:expire3": {Type: stringType, Value: "value3", SyncedTs: currentTs, ExpireTs: expireTsInvalid},
		"{a}:string:expire4": {Type: stringType, Value: "value4", SyncedTs: currentTs, ExpireTs: expireTsInvalid},
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
	err := Load(hashTag)
	assert.Nil(t, err)

	client := base.GetRedisCluster()
	for key, value := range validValue {
		v, err := client.Get(testContextTODO, key).Result()
		assert.Nil(t, err)
		assert.Equal(t, value.Value, v)

		d, err := client.TTL(testContextTODO, key).Result()
		assert.Nil(t, err)
		if value.ExpireTs > 0 {
			assert.Greater(t, int64(d), int64(0))
		} else {
			assert.Equal(t, int64(-1), int64(d))
		}
	}

	for key, _ := range expireValue {
		_, err := client.Get(testContextTODO, key).Result()
		assert.Equal(t, redis.Nil, err)
	}

	status, err := client.HGet(testContextTODO, getHashTagMetaKey(hashTag), HashTagMetaInfoStatusFieldName).Result()
	assert.Nil(t, err)
	assert.Equal(t, HashTagStatusLoaded, status)

	_, err = client.Get(testContextTODO, getHashTagLockKey(hashTag)).Result()
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
	currentTs := utility.TimestampInMS(time.Now())
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
		value[k] = RedisValue{Type: listType, Value: v, SyncedTs: currentTs, ExpireTs: 0}
	}

	testInsertRoomData(hashTag, value)
	testSetMetaKeyCleaned(hashTag)

	err := Load(hashTag)
	assert.Nil(t, err)

	client := base.GetRedisCluster()
	for _, item := range testItems {
		key := item.key
		b, err := client.LRange(testContextTODO, key, 0, -1).Result()
		assert.Nil(t, err)
		v, _ := json.Marshal(b)
		assert.Equal(t, value[key].Value, string(v))

		duration, err := client.TTL(testContextTODO, key).Result()
		assert.Nil(t, err)
		assert.Equal(t, time.Duration(-1), duration)
	}

	status, err := client.HGet(testContextTODO, getHashTagMetaKey(hashTag), HashTagMetaInfoStatusFieldName).Result()
	assert.Nil(t, err)
	assert.Equal(t, HashTagStatusLoaded, status)

	_, err = client.Get(testContextTODO, getHashTagLockKey(hashTag)).Result()
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
	currentTs := utility.TimestampInMS(time.Now())
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
		value[k] = RedisValue{Type: hashType, Value: v, SyncedTs: currentTs, ExpireTs: 0}
	}

	testInsertRoomData(hashTag, value)
	testSetMetaKeyCleaned(hashTag)

	err := Load(hashTag)
	assert.Nil(t, err)

	client := base.GetRedisCluster()
	for _, item := range testItems {
		key := item.key
		hash := hashes[key]
		m, err := client.HGetAll(testContextTODO, key).Result()
		assert.Nil(t, err)
		assert.Equal(t, len(hash), len(m))
		for key, value := range hash {
			assert.Equal(t, value, m[key])
		}

		duration, err := client.TTL(testContextTODO, key).Result()
		assert.Nil(t, err)
		assert.Equal(t, time.Duration(-1), duration)
	}
	status, err := client.HGet(testContextTODO, getHashTagMetaKey(hashTag), HashTagMetaInfoStatusFieldName).Result()
	assert.Nil(t, err)
	assert.Equal(t, HashTagStatusLoaded, status)

	_, err = client.Get(testContextTODO, getHashTagLockKey(hashTag)).Result()
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
	currentTs := utility.TimestampInMS(time.Now())
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
		value[k] = RedisValue{Type: setType, Value: v, SyncedTs: currentTs, ExpireTs: 0}
	}

	testInsertRoomData(hashTag, value)
	testSetMetaKeyCleaned(hashTag)

	err := Load(hashTag)
	assert.Nil(t, err)

	client := base.GetRedisCluster()

	for _, item := range testItems {
		key := item.key
		set := sets[key]
		m, err := client.SMembers(testContextTODO, key).Result()
		assert.Nil(t, err)
		assert.Equal(t, len(set), len(m))
		for _, value := range set {
			assert.True(t, utility.StringSliceContains(m, value))
		}

		duration, err := client.TTL(testContextTODO, key).Result()
		assert.Nil(t, err)
		assert.Equal(t, time.Duration(-1), duration)
	}
	status, err := client.HGet(testContextTODO, getHashTagMetaKey(hashTag), HashTagMetaInfoStatusFieldName).Result()
	assert.Nil(t, err)
	assert.Equal(t, HashTagStatusLoaded, status)

	_, err = client.Get(testContextTODO, getHashTagLockKey(hashTag)).Result()
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
	currentTs := utility.TimestampInMS(time.Now())
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
		value[k] = RedisValue{Type: zsetType, Value: v, SyncedTs: currentTs, ExpireTs: 0}
	}

	testInsertRoomData(hashTag, value)
	testSetMetaKeyCleaned(hashTag)

	err := Load(hashTag)
	assert.Nil(t, err)

	client := base.GetRedisCluster()

	for _, item := range testItems {
		key := item.key
		zset := zsets[key]
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
	}
	status, err := client.HGet(testContextTODO, getHashTagMetaKey(hashTag), HashTagMetaInfoStatusFieldName).Result()
	assert.Nil(t, err)
	assert.Equal(t, HashTagStatusLoaded, status)

	_, err = client.Get(testContextTODO, getHashTagLockKey(hashTag)).Result()
	assert.Equal(t, redis.Nil, err)
}

func TestExtractHashTagFromKey(t *testing.T) {
	cases := []struct {
		key     string
		hashTag string
	}{
		{"a", ""},
		{"", ""},
		{"a}{", ""},
		{"{}a", ""},
		{"{a}", "a"},
		{"{ab}", "ab"},
		{"{a}b", "a"},
		{"{ab}c", "ab"},
		{"{ab}c{d}", "ab"},
		{"x{ab}c{d}", "ab"},
		{"a{b}", "b"},
		{"a{bc}", "bc"},
		{"a{bc}d", "bc"},
		{"}{ab}cab", "ab"},
		{"{}{abc}xy", ""},
		{"{{abc}}xy", "{abc"},
	}
	for _, c := range cases {
		hashTag := extractHashTagFromKey(c.key)
		assert.Equal(t, c.hashTag, hashTag)
	}
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
	status, _ := dep.Redis.HGet(context.TODO(), metaKey, HashTagMetaInfoStatusFieldName).Result()
	assert.Equal(t, HashTagStatusCleaned, status)
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
	client := base.GetRedisCluster()
	ctx := context.TODO()
	for _, c := range cases {
		defer client.Del(ctx, c.key)
		err := loadSetToRedis(ctx, client, c.key, c.slices, c.tl)
		assert.Nil(t, err)
		length, _ := client.SCard(ctx, c.key).Result()
		assert.Equal(t, len(c.members), int(length))
		members, _ := client.SMembers(ctx, c.key).Result()
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
	client := base.GetRedisCluster()
	ctx := context.TODO()
	for _, c := range cases {
		defer client.Del(ctx, c.key)
		err := loadListToRedis(ctx, client, c.key, c.slices, c.ttl)
		assert.Nil(t, err)
		length, _ := client.LLen(ctx, c.key).Result()
		assert.Equal(t, len(c.members), int(length))
		members, _ := client.LRange(ctx, c.key, 0, -1).Result()
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
	client := base.GetRedisCluster()
	ctx := context.TODO()
	for _, c := range cases {
		defer client.Del(ctx, c.key)
		err := loadHashToRedis(ctx, client, c.key, c.slices, c.ttl)
		assert.Nil(t, err)
		length, _ := client.HLen(ctx, c.key).Result()
		assert.Equal(t, len(c.members), int(length))
		members, _ := client.HGetAll(ctx, c.key).Result()
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
	client := base.GetRedisCluster()
	ctx := context.TODO()
	for _, c := range cases {
		defer client.Del(ctx, c.key)
		err := loadZSetToRedis(ctx, client, c.key, c.slices, c.ttl)
		assert.Nil(t, err)
		length, _ := client.ZCard(ctx, c.key).Result()
		assert.Equal(t, len(c.members), int(length))
		members, _ := client.ZRangeWithScores(ctx, c.key, 0, -1).Result()
		for _, member := range members {
			key := member.Member
			score := member.Score
			s, _ := strconv.ParseFloat(c.members[key.(string)], 64)
			assert.Equal(t, s, score)
		}
	}
}

func TestHashTagLoadWithTimeout(t *testing.T) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Nanosecond)
	time.Sleep(10 * time.Nanosecond)

	tag := "abc"
	dep := base.GetServerDependency()
	hashTag, _ := NewHashTag(tag, dep)
	count, err := hashTag.loadKeys(ctx)
	assert.Equal(t, context.DeadlineExceeded, err)
	assert.Equal(t, 0, count)
}
