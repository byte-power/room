package service

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"context"
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
var errLoadKeysLockFailed = errors.New("do not get load lock")

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

func getMetaKeyByHashTag(hashTag string) string {
	return fmt.Sprintf("{%s}:_m", hashTag)
}

func getLockKeyByHashTag(hashTag string) string {
	return fmt.Sprintf("{%s}:_l", hashTag)
}

const (
	keysStatusLoaded       = "L"
	keysStatusCleaned      = "C"
	metaKeyStatusFieldName = "status"
)

func areKeysNeedLoadForHashTag(client *redis.ClusterClient, hashTag string) (bool, error) {
	metaKey := getMetaKeyByHashTag(hashTag)
	status, err := client.HGet(contextTODO, metaKey, metaKeyStatusFieldName).Result()
	if err != nil {
		if !errors.Is(err, redis.Nil) {
			return false, newInternalError(err)
		}
		if err := setKeysAsLoaded(client, hashTag); err != nil {
			return false, newInternalError(err)
		}
		return false, nil
	}
	return status != keysStatusLoaded, nil
}

func getStatusForHashTag(client *redis.ClusterClient, hashTag string) (string, error) {
	metaKey := getMetaKeyByHashTag(hashTag)
	return client.HGet(contextTODO, metaKey, metaKeyStatusFieldName).Result()
}

func setKeysAsLoaded(client *redis.ClusterClient, hashTag string) error {
	metaKey := getMetaKeyByHashTag(hashTag)
	_, err := client.HSet(contextTODO, metaKey, metaKeyStatusFieldName, keysStatusLoaded).Result()
	return err
}

func recordLoadError(hashTag string, err error, duration time.Duration) {
	logger := base.GetServerLogger()
	metric := base.GetMetricService()
	logger.Error(
		"load keys error",
		log.String("hash_tag", hashTag),
		log.Error(err),
		log.String("duration", duration.String()))
	metric.MetricIncrease("error.loadkey")
}

func recordLoadRetry(hashTag string, err error, times int) {
	logger := base.GetServerLogger()
	metric := base.GetMetricService()
	logger.Info(
		"load keys retry",
		log.String("hash_tag", hashTag),
		log.Int("load_times", times),
		log.Error(err))
	metric.MetricIncrease("loadkey.retry")
}

func recordLoadSuccess(hashTag string, duration time.Duration) {
	logger := base.GetServerLogger()
	metric := base.GetMetricService()
	logger.Info(
		"load keys success",
		log.String("hash_tag", hashTag),
		log.String("duration", duration.String()))
	metric.MetricTimeDuration("loadkey.duration", duration)
}

func loadByHashTag(hashTag string) error {
	loadRetryTimes := base.GetServerConfig().LoadKey.GetRetryTimes()
	loadRetryInterval := base.GetServerConfig().LoadKey.GetRetryInterval()
	var err error
	for i := 0; i < loadRetryTimes; i++ {
		startTime := time.Now()
		err = loadKeysByHashTag(hashTag)
		if err != nil {
			if isRetryLoadErrorV2(err) {
				time.Sleep(loadRetryInterval)
				recordLoadRetry(hashTag, err, i+1)
				continue
			}
			recordLoadError(hashTag, err, time.Since(startTime))
			return err
		}
		recordLoadSuccess(hashTag, time.Since(startTime))
		return nil
	}
	return err
}

// 1. if metaKey.state == "loaded", return; else: go to step 2
// 2. acquire lock, if lock is acquired, load keys from database, go to step 3; else go to step 1 and retry
// 3. release lock, update metaKey.state to loaded
func loadKeysByHashTag(hashTag string) error {
	client := base.GetRedisCluster()
	status, err := getStatusForHashTag(client, hashTag)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			if err := setKeysAsLoaded(client, hashTag); err != nil {
				return newInternalError(err)
			}
			return nil
		}
		return newInternalError(err)
	}
	if status == keysStatusLoaded {
		return nil
	}
	if err := acquireLoadLock(client, hashTag); err != nil {
		return err
	}
	defer releaseLoadLock(client, hashTag)
	db := base.GetDBCluster()
	logger := base.GetServerLogger()
	loadTimeout := base.GetServerConfig().LoadKey.GetLoadTimeout()
	ctx, cancel := context.WithTimeout(context.Background(), loadTimeout)
	defer cancel()
	if err := loadKeysFromDBToRedis(ctx, db, client, logger, hashTag); err != nil {
		return err
	}
	return setKeysAsLoaded(client, hashTag)
}

const loadLockDuration = 5 * time.Second

func acquireLoadLock(client *redis.ClusterClient, hashTag string) error {
	lockKey := getLockKeyByHashTag(hashTag)
	locked, err := client.SetNX(contextTODO, lockKey, "locked", loadLockDuration).Result()
	if err != nil {
		return newInternalError(err)
	}
	if !locked {
		return errLoadKeysLockFailed
	}
	return nil
}

func releaseLoadLock(client *redis.ClusterClient, hashTag string) {
	lockKey := getLockKeyByHashTag(hashTag)
	client.Del(contextTODO, lockKey)
}

func loadKeysFromDBToRedis(ctx context.Context, db *base.DBCluster, client *redis.ClusterClient, logger *log.Logger, hashTag string) error {
	startTime := time.Now()
	model, err := loadDataByIDWithContext(ctx, hashTag)
	if err != nil {
		logger.Info(
			"load from databse",
			log.String("hash_tag", hashTag),
			log.String("duration", time.Since(startTime).String()),
			log.Error(err),
		)
		return err
	}
	logger.Info(
		"load from databse",
		log.String("hash_tag", hashTag),
		log.String("duration", time.Since(startTime).String()),
	)
	if model == nil {
		//TODO, add metric, model should exist.
		return nil
	}
	for key, value := range model.Value {
		if err := loadKeyToRedis(ctx, client, key, value); err != nil {
			return err
		}
	}
	return nil
}

func loadKeyToRedis(ctx context.Context, client *redis.ClusterClient, key string, value redisValue) error {
	expire := value.expireDuration(time.Now())
	if expire < 0 {
		return nil
	}
	switch value.Type {
	case stringType:
		_, err := client.Set(ctx, key, value.Value, expire).Result()
		return err
	case setType, hashType, listType, zsetType:
		var size int
		if value.Type == hashType || value.Type == zsetType {
			size = loadAndSaveStepSize * 2
		} else {
			size = loadAndSaveStepSize
		}
		slices, err := loadDataIntoSlices(value.Value, size)
		if err != nil {
			return newParseError(err)
		}
		if err := loadDataIntoRedisV2(ctx, client, key, slices, value.Type, expire); err != nil {
			return err
		}
	default:
		return fmt.Errorf("data type %s is not supported", value.Type)
	}
	return nil
}

func loadDataIntoRedisV2(
	ctx context.Context, client *redis.ClusterClient,
	key string, slices [][]interface{},
	dataType string, expire time.Duration) error {

	pipeline := client.Pipeline()
	for _, slice := range slices {
		switch dataType {
		case setType:
			pipeline.SAdd(ctx, key, slice...)
		case listType:
			pipeline.RPush(ctx, key, slice...)
		case hashType:
			if len(slice)%2 != 0 {
				return errDataFormatError
			}
			pipeline.HSet(ctx, key, slice...)
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
			pipeline.ZAdd(ctx, key, zsetValue...)
		}
	}
	if expire > 0 {
		pipeline.Expire(ctx, key, expire)
	}
	if _, err := pipeline.Exec(ctx); err != nil {
		return err
	}
	return nil
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
		startTime := time.Now()
		loaded, err := loadKeyOnce(key, loadTimeout)
		if err != nil {
			logger.Error(
				"load key error",
				log.String("key", key),
				log.Error(err),
				log.String("duration", time.Since(startTime).String()))
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
			logger.Info(
				"load key success",
				log.String("key", key),
				log.String("duration", time.Since(startTime).String()))
			metric.MetricTimeDuration("loadkey.duration", time.Since(startTime))
			return nil
		}
	}
	return loadErr
}

func isRetryLoadError(err error) bool {
	return errors.Is(err, errLoadTimeout) || errors.Is(err, redis.TxFailedErr)
}

func isRetryLoadErrorV2(err error) bool {
	return errors.Is(err, errLoadKeysLockFailed) || errors.Is(err, context.DeadlineExceeded)
}

// loadingKeys is a map from key to *sync.Once
var loadingKeys sync.Map

func loadKeyOnce(key string, loadTimeout time.Duration) (bool, error) {
	once, _ := loadingKeys.LoadOrStore(key, &sync.Once{})
	var loaded bool
	var err error
	once.(*sync.Once).Do(
		func() {
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
			if err == nil {
				loaded = true
			}
		})
	return loaded, err
}

// 1. Load key from database to redis
// 2. Set metadata loaded = 1
func loadKeyFromDBToRedis(key string, ch chan error) {
	client := base.GetRedisCluster()
	logger := base.GetServerLogger()
	startTime := time.Now()
	model, err := loadDataByKey(key)
	logger.Info(
		"load from database",
		log.String("key", key),
		log.String("duration", time.Since(startTime).String()),
	)
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
