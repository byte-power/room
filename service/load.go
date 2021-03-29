package service

import (
	"bytepower_room/base"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

var errDataFormatError = errors.New("data format is invalid")
var errLoadConflict = errors.New("load key conflict")

func newParseError(err error) error {
	return fmt.Errorf("parse value error, %w", err)
}

const (
	letterBytes         = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	loadAndSaveStepSize = 100
)

func generateRandString(n int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

// 1. Get lock, lock is set to avoid duplicate load
// 2. Load key from database to redis
// 3. Set metadata loaded = 1
func loadKey(key string) error {
	needLoaded, err := isKeyNeedLoaded(key)
	if err != nil {
		return err
	}
	if !needLoaded {
		return nil
	}
	client := base.GetRedisCluster()
	lockKey := getLockKey(key)
	isLockSet, err := client.SetNX(contextTODO, lockKey, 1, time.Second).Result()
	if err != nil {
		return err
	}
	if !isLockSet {
		return errLoadConflict
	}
	defer client.Del(contextTODO, lockKey)
	model, err := loadDataByKey(key)
	if err != nil {
		return err
	}
	if model == nil {
		return setLoadedMeta(key)
	}
	var duration time.Duration
	if !model.ExpireAt.IsZero() {
		duration = time.Until(model.ExpireAt)
		// key is expired
		if duration <= 0 {
			return setLoadedMeta(key)
		}
	}
	tempKey := fmt.Sprintf("%s:temp:%s", key, generateRandString(5))
	defer client.Del(contextTODO, tempKey)
	switch model.Type {
	case stringType:
		if _, err = client.Set(contextTODO, tempKey, model.Value, 0).Result(); err != nil {
			return err
		}
	case setType, hashType, listType, zsetType:
		var size int
		if model.Type == hashType || model.Type == zsetType {
			size = loadAndSaveStepSize * 2
		} else {
			size = loadAndSaveStepSize
		}
		slices, err := loadDataIntoSlices(model.Value, size)
		if err != nil {
			return newParseError(err)
		}
		if err := loadDataIntoRedis(tempKey, slices, model.Type); err != nil {
			return err
		}
	default:
		return fmt.Errorf("data type %s is not supported", model.Type)
	}
	metaKey := getMetaKey(key)
	err = client.Watch(
		contextTODO,
		func(tx *redis.Tx) error {
			_, err := tx.TxPipelined(contextTODO, func(pipe redis.Pipeliner) error {
				pipe.RenameNX(contextTODO, tempKey, key)
				if duration > 0 {
					pipe.Expire(contextTODO, key, duration)
				}
				pipe.HSet(contextTODO, metaKey, "loaded", "1")
				return nil
			})
			return err
		}, key, metaKey)
	return err
}

func getLockKey(key string) string {
	return key + ":_lock"
}

func setLoadedMeta(key string) error {
	client := base.GetRedisCluster()
	_, err := client.HSet(contextTODO, getMetaKey(key), "loaded", "1").Result()
	return err
}

func loadDataIntoRedis(tempKey string, slices [][]interface{}, dataType string) error {
	client := base.GetRedisCluster()
	pipeline := client.Pipeline()
	for _, slice := range slices {
		switch dataType {
		case setType:
			pipeline.SAdd(contextTODO, tempKey, slice...)
		case listType:
			pipeline.RPush(contextTODO, tempKey, slice...)
		case hashType:
			if len(slice)%2 != 0 {
				return errDataFormatError
			}
			pipeline.HSet(contextTODO, tempKey, slice...)
		case zsetType:
			if len(slice)%2 != 0 {
				return errDataFormatError
			}
			var zsetValue []*redis.Z
			for index := 0; index < len(slice)-1; index += 2 {
				member := slice[index]
				scoreStr, ok := slice[index+1].(string)
				if !ok {
					return errDataFormatError
				}
				score, err := strconv.ParseFloat(scoreStr, 64)
				if err != nil {
					return errDataFormatError
				}
				item := &redis.Z{Member: member, Score: score}
				zsetValue = append(zsetValue, item)
			}
			pipeline.ZAdd(contextTODO, tempKey, zsetValue...)
		}
	}
	if _, err := pipeline.Exec(contextTODO); err != nil {
		return err
	}
	return nil
}

func loadDataIntoSlices(v string, size int) ([][]interface{}, error) {
	value := []interface{}{}
	if err := json.Unmarshal([]byte(v), &value); err != nil {
		return [][]interface{}{}, err
	}
	slices := splitIntoChunks(value, size)
	return slices, nil
}

func splitIntoChunks(slice []interface{}, chunkSize int) [][]interface{} {
	slices := [][]interface{}{}
	length := len(slice)
	chunkCount := length / chunkSize
	if length%chunkSize != 0 {
		chunkCount++
	}
	for index := 0; index < chunkCount; index++ {
		var s []interface{}
		if (index+1)*chunkSize > length {
			s = slice[index*chunkSize:]
		} else {
			s = slice[index*chunkSize : (index+1)*chunkSize]
		}
		slices = append(slices, s)
	}
	return slices
}
