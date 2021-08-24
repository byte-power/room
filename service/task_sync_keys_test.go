package service

import (
	"bytepower_room/base"
	"bytepower_room/utility"
	"context"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func testGenerateListValueForRedis(count int) []interface{} {
	items := make([]interface{}, count)
	for i := 0; i < count; i++ {
		items[i] = utility.GenerateUUID(10)
	}
	return items
}

func testGenerateHashValueForRedis(count int) map[string]interface{} {
	hash := make(map[string]interface{})
	for i := 0; i < count; i++ {
		key := utility.GenerateUUID(10)
		value := utility.GenerateUUID(10)
		hash[key] = value
	}
	return hash
}

func testGenerateSetValueForRedis(count int) []interface{} {
	return testGenerateListValueForRedis(count)
}

func testGenerateZSetValueForRedis(count int) ([]*redis.Z, map[string]float64) {
	zset := make([]*redis.Z, count)
	m := make(map[string]float64)
	for i := 0; i < count; i++ {
		member := utility.GenerateUUID(10)
		score := testGenerateRandFloat(0, 100)
		z := &redis.Z{Member: member, Score: score}
		zset[i] = z
		m[member] = score
	}
	return zset, m
}

func TestSerializeNonStringValue(t *testing.T) {
	redisCluster := base.GetTaskDependency().Redis

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
		values := testGenerateListValueForRedis(item.count)
		redisCluster.RPush(testContextTODO, key, values...).Result()
		result, err := serializeNonStringValue(redisCluster, key, listType)
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
		values := testGenerateHashValueForRedis(item.count)
		redisCluster.HSet(testContextTODO, key, values).Result()
		result, err := serializeNonStringValue(redisCluster, key, hashType)
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
		values := testGenerateSetValueForRedis(item.count)
		redisCluster.SAdd(testContextTODO, key, values...).Result()
		result, err := serializeNonStringValue(redisCluster, key, setType)
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
		values, m := testGenerateZSetValueForRedis(item.count)
		redisCluster.ZAdd(testContextTODO, key, values...).Result()
		result, err := serializeNonStringValue(redisCluster, key, zsetType)
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
	redisCluster := base.GetTaskDependency().Redis
	//get not exist key
	key := "{a}not_exist"
	value, err := getValueFromRedis(redisCluster, key)
	assert.Nil(t, err)
	assert.Equal(t, "", value.Type)
	assert.Equal(t, "", value.Value)
	assert.Equal(t, int64(0), value.ExpireTs)

	// get a string key
	key = "{b}string"
	stringValue := "abc"
	defer redisCluster.Del(context.TODO(), key)
	redisCluster.Set(context.TODO(), key, stringValue, 0)
	value, err = getValueFromRedis(redisCluster, key)
	assert.Nil(t, err)
	assert.Equal(t, stringType, value.Type)
	assert.Equal(t, stringValue, value.Value)
	assert.Equal(t, int64(0), value.ExpireTs)

	// get a string key with expiration
	key = "{b}string2"
	stringValue = "abcd"
	defer redisCluster.Del(context.TODO(), key)
	redisCluster.Set(context.TODO(), key, stringValue, 10*time.Second)
	value, err = getValueFromRedis(redisCluster, key)
	assert.Nil(t, err)
	assert.Equal(t, stringType, value.Type)
	assert.Equal(t, stringValue, value.Value)
	assert.Greater(t, value.ExpireTs, int64(0))

	// get a list key
	key = "{b}list"
	listValue := []interface{}{"a", "b", "c", "d"}
	defer redisCluster.Del(context.TODO(), key)
	redisCluster.RPush(context.TODO(), key, listValue...)
	value, err = getValueFromRedis(redisCluster, key)
	assert.Nil(t, err)
	assert.Equal(t, listType, value.Type)

	v := make([]interface{}, 0)
	json.Unmarshal([]byte(value.Value), &v)
	assert.Equal(t, listValue, v)

	assert.Equal(t, int64(0), value.ExpireTs)

	// get a set key
	key = "{b}set"
	setValue := []interface{}{"a", "b", "c", "d"}
	defer redisCluster.Del(context.TODO(), key)
	redisCluster.SAdd(context.TODO(), key, setValue...)
	value, err = getValueFromRedis(redisCluster, key)
	assert.Nil(t, err)
	assert.Equal(t, setType, value.Type)

	v = make([]interface{}, 0)
	json.Unmarshal([]byte(value.Value), &v)
	assert.Equal(t, len(setValue), len(v))
	for _, item := range v {
		assert.True(t, utility.StringSliceContains(utility.AnyArrayToStringArray(setValue), item.(string)))
	}

	assert.Equal(t, int64(0), value.ExpireTs)

	// get a hash key
	key = "{b}hash"
	hashValue := map[string]interface{}{"a": "b", "c": "d", "e": "f"}
	defer redisCluster.Del(context.TODO(), key)
	redisCluster.HSet(context.TODO(), key, hashValue)
	value, err = getValueFromRedis(redisCluster, key)
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

	// get a zset key, with expiration
	key = "{b}zset"
	zsetValue := map[string]redis.Z{
		"a": {Member: "a", Score: 1.1},
		"b": {Member: "b", Score: 2.2},
		"c": {Member: "c", Score: 3.3},
	}
	defer redisCluster.Del(context.TODO(), key)
	for _, z := range zsetValue {
		redisCluster.ZAdd(context.TODO(), key, &z)
	}
	redisCluster.Expire(context.TODO(), key, 10*time.Second)
	value, err = getValueFromRedis(redisCluster, key)
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
}
