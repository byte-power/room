package service

import (
	"bytepower_room/base"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

var errDataFormatError = errors.New("data format is invalid")
var errLoadTimeout = errors.New("load key timeout")
var errLoadKeysLockFailed = errors.New("do not get load lock")

var ErrEmptyHashTag = errors.New("hash tag is empty")

func newParseError(err error) error {
	return fmt.Errorf("parse value error, %w", err)
}

const (
	loadAndSaveStepSize = 100

	HashTagStatusLoaded            = "L"
	HashTagStatusCleaned           = "C"
	HashTagMetaInfoStatusFieldName = "status"
)

type HashTag struct {
	name string
	meta HashTagMetaInfo
	dep  base.Dependency
}

var emptyHashTag = HashTag{}

func NewHashTag(name string, dep base.Dependency) (HashTag, error) {
	if err := dep.Check(); err != nil {
		return emptyHashTag, err
	}
	if name == "" {
		return emptyHashTag, ErrEmptyHashTag
	}
	meta, err := NewHashTagMetaInfo(name, dep)
	if err != nil {
		return emptyHashTag, err
	}
	return HashTag{
		name: name,
		meta: meta,
		dep:  dep,
	}, nil
}

func (tag HashTag) Name() string {
	return tag.name
}

func (tag HashTag) CleanKeys(keys ...string) error {
	if err := tag.acquireLoadLock(); err != nil {
		return err
	}
	defer tag.releaseLoadLock()
	if err := tag.meta.SetAsCleaned(); err != nil {
		return err
	}
	_, err := tag.dep.Redis.Del(contextTODO, keys...).Result()
	return err
}

func (tag HashTag) Load(timeout time.Duration) error {
	status, err := tag.meta.GetLoadStatus()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			if err := tag.meta.SetAsLoaded(); err != nil {
				return err
			}
			return nil
		}
		return err
	}
	if status == HashTagStatusLoaded {
		return nil
	}
	if err := tag.acquireLoadLock(); err != nil {
		return err
	}
	defer tag.releaseLoadLock()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := tag.loadKeys(ctx); err != nil {
		return err
	}
	return tag.meta.SetAsLoaded()
}

func (tag HashTag) loadKeys(ctx context.Context) error {
	startTime := time.Now()
	model, err := loadDataByIDWithContext(ctx, tag.dep.DB, tag.name)
	if err != nil {
		recordLoadDBError(tag.dep.Logger, tag.name, time.Since(startTime), err)
		return err
	}
	recordLoadDBSuccess(tag.dep.Logger, tag.name, time.Since(startTime))
	if model == nil {
		recordLoadDBRecordNotFound(tag.dep.Logger, tag.dep.Metric, tag.name)
		return nil
	}
	for key, value := range model.Value {
		if err := loadKeyToRedis(ctx, tag.dep.Redis, key, value); err != nil {
			return err
		}
	}
	return nil
}

func (tag HashTag) lockKey() string {
	return fmt.Sprintf("{%s}:_l", tag.name)
}

const loadLockDuration = 5 * time.Second

func (tag HashTag) acquireLoadLock() error {
	locked, err := tag.dep.Redis.SetNX(contextTODO, tag.lockKey(), "locked", loadLockDuration).Result()
	if err != nil {
		return err
	}
	if !locked {
		return errLoadKeysLockFailed
	}
	return nil
}

func (tag HashTag) releaseLoadLock() {
	tag.dep.Redis.Del(contextTODO, tag.lockKey())
}

type HashTagMetaInfo struct {
	tag     string
	metaKey string
	dep     base.Dependency
}

var emptyHashTagMetaInfo = HashTagMetaInfo{}

func NewHashTagMetaInfo(tag string, dep base.Dependency) (HashTagMetaInfo, error) {
	if err := dep.Check(); err != nil {
		return emptyHashTagMetaInfo, err
	}
	if tag == "" {
		return emptyHashTagMetaInfo, ErrEmptyHashTag
	}
	return HashTagMetaInfo{
		tag:     tag,
		metaKey: getHashTagMetaKey(tag),
		dep:     dep,
	}, nil
}

func getHashTagMetaKey(hashTag string) string {
	return fmt.Sprintf("{%s}:_m", hashTag)
}

func (meta HashTagMetaInfo) GetLoadStatus() (string, error) {
	return meta.dep.Redis.HGet(contextTODO, meta.metaKey, HashTagMetaInfoStatusFieldName).Result()
}

func (meta HashTagMetaInfo) SetAsLoaded() error {
	_, err := meta.dep.Redis.HSet(
		contextTODO, meta.metaKey, HashTagMetaInfoStatusFieldName,
		HashTagStatusLoaded).Result()
	return err
}

func (meta HashTagMetaInfo) SetAsCleaned() error {
	_, err := meta.dep.Redis.HSet(
		contextTODO, meta.metaKey, HashTagMetaInfoStatusFieldName,
		HashTagStatusCleaned).Result()
	return err
}

func Load(hashTag string) error {
	loadRetryTimes := base.GetServerConfig().LoadKey.GetRetryTimes()
	loadRetryInterval := base.GetServerConfig().LoadKey.GetRetryInterval()
	loadTimeout := base.GetServerConfig().LoadKey.GetLoadTimeout()
	dep := base.GetServerDependency()
	var err error
	for i := 0; i < loadRetryTimes; i++ {
		startTime := time.Now()
		tag, e := NewHashTag(hashTag, dep)
		if e != nil {
			return e
		}
		err = tag.Load(loadTimeout)
		if err != nil {
			if isRetryLoadError(err) {
				time.Sleep(loadRetryInterval)
				recordLoadKeyRetryError(dep.Logger, dep.Metric, hashTag, err, i+1)
				continue
			}
			recordLoadKeyError(dep.Logger, dep.Metric, hashTag, err, time.Since(startTime))
			return err
		}
		recordLoadKeySuccess(dep.Logger, dep.Metric, hashTag, time.Since(startTime))
		return nil
	}
	return err
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
		if err := loadDataIntoRedis(ctx, client, key, slices, value.Type, expire); err != nil {
			return err
		}
	default:
		return fmt.Errorf("data type %s is not supported", value.Type)
	}
	return nil
}

func loadDataIntoRedis(
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

func isRetryLoadError(err error) bool {
	return errors.Is(err, errLoadKeysLockFailed) || errors.Is(err, context.DeadlineExceeded)
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
