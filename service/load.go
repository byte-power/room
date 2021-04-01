package service

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

var errDataFormatError = errors.New("data format is invalid")
var errLoadTimeout = errors.New("load key timeout")

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

// 1. check if key exists: if key exists, return else go to step 2
// 2. check if key needs loaded: if not needs, return, else go to step 3
// 3. load key from database
func loadKey(key string) error {
	loadRetryTimes := base.GetServerConfig().LoadKey.GetRetryTimes()
	loadRetryInterval := base.GetServerConfig().LoadKey.GetRetryInterval()
	loadTimeout := base.GetServerConfig().LoadKey.GetLoadTimeout()
	logger := base.GetServerLogger()
	metric := base.GetMetricService()
	var loadErr error
	for i := 0; i < loadRetryTimes; i++ {
		needLoaded, err := isKeyNeedLoad(key)
		if err != nil {
			return err
		}
		if !needLoaded {
			return nil
		}
		loaded, err := loadKeyOnce(key, loadTimeout)
		if err != nil {
			loadErr = err
			if isRetryLoadError(err) {
				time.Sleep(loadRetryInterval)
				metric.MetricIncrease("loadkey.retry")
				logger.Info("load key retry", log.String("key", key), log.Int("load_times", i+1))
				continue
			}
			metric.MetricIncrease("error.loadkey")
			return err
		}
		if loaded {
			return nil
		}
	}
	return loadErr
}

func isRetryLoadError(err error) bool {
	return errors.Is(err, errLoadTimeout) || errors.Is(err, redis.TxFailedErr)
}

// loadingKeys is a map from key to *sync.Once
var loadingKeys sync.Map

func loadKeyOnce(key string, loadTimeout time.Duration) (bool, error) {
	once, _ := loadingKeys.LoadOrStore(key, &sync.Once{})
	var loaded bool
	var err error
	once.(*sync.Once).Do(
		func() {
			startTime := time.Now()
			metric := base.GetMetricService()
			logger := base.GetServerLogger()
			defer loadingKeys.Delete(key)
			ch := make(chan error, 1)
			// TODO: may add context to cancel loadKeyFromDBToRedis when timeout.
			go loadKeyFromDBToRedis(key, ch)
			select {
			case e := <-ch:
				err = e
			case <-time.After(loadTimeout):
				err = errLoadTimeout
			}
			loadDuration := time.Since(startTime)
			if err != nil {
				logger.Error(
					"load key error",
					log.String("key", key),
					log.Error(err),
					log.String("duration", loadDuration.String()))
			} else {
				loaded = true
				logger.Info(
					"load key success",
					log.String("key", key),
					log.String("duration", loadDuration.String()))
			}
			metric.MetricTimeDuration("loadkey.duration", time.Since(startTime))
		})
	return loaded, err
}

// 1. Load key from database to redis
// 2. Set metadata loaded = 1
func loadKeyFromDBToRedis(key string, ch chan error) {
	client := base.GetRedisCluster()
	model, err := loadDataByKey(key)
	if err != nil {
		ch <- err
		return
	}
	if model == nil {
		ch <- setLoadedMeta(key)
		return
	}
	var duration time.Duration
	if !model.ExpireAt.IsZero() {
		duration = time.Until(model.ExpireAt)
		// key is expired
		if duration <= 0 {
			ch <- setLoadedMeta(key)
			return
		}
	}
	tempKey := fmt.Sprintf("%s:temp:%s", key, generateRandString(10))
	defer client.Del(contextTODO, tempKey)
	switch model.Type {
	case stringType:
		if _, err = client.Set(contextTODO, tempKey, model.Value, 0).Result(); err != nil {
			ch <- err
			return
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
			ch <- newParseError(err)
			return
		}
		if err := loadDataIntoRedis(tempKey, slices, model.Type); err != nil {
			ch <- err
			return
		}
	default:
		ch <- fmt.Errorf("data type %s is not supported", model.Type)
		return
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
	ch <- err
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
