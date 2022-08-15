package service

import (
	"bytepower_room/base"
	"bytepower_room/utility"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

var errDataFormatError = errors.New("data format is invalid")
var errLoadKeysLockFailed = errors.New("do not get load lock")

var (
	ErrEmptyHashTag      = errors.New("hash tag is empty")
	ErrAccessAfterRecord = errors.New("hash tag is accessed after recording")
)

func newParseError(err error) error {
	return fmt.Errorf("parse value error, %w", err)
}

const (
	loadAndSaveStepSize = 100

	HashTagStatusLoaded                = "L"
	HashTagStatusNotExisted            = "N"
	HashTagMetaInfoStatusFieldName     = "status"
	HashTagMetaInfoAccessTimeFieldName = "at"
	HashTagMetaInfoWriteTimeFieldName  = "wt"
	HashTagMetaInfoVersionFieldName    = "v"
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
	err := tag.meta.SetAsCleaned()
	if err != nil {
		return err
	}
	if len(keys) > 0 {
		_, err = tag.dep.Redis.Del(contextTODO, keys...).Result()
	}
	return err
}

func (tag HashTag) CleanKeysV2(accessedAt time.Time, keys ...string) (int64, error) {
	var n int64 = 0
	if err := tag.acquireLoadLock(); err != nil {
		return n, err
	}
	defer tag.releaseLoadLock()
	t, err := tag.meta.GetAccessTime()
	if t.After(accessedAt) {
		return n, ErrAccessAfterRecord
	}
	if err := tag.meta.SetAsCleaned(); err != nil {
		return n, err
	}
	if len(keys) > 0 {
		n, err = tag.dep.Redis.Del(contextTODO, keys...).Result()
	}
	return n, err
}

func (tag HashTag) GetLoadStatus() (string, error) {
	return tag.meta.GetLoadStatus()
}

func (tag HashTag) NeedToLoad(ctx context.Context) (bool, error) {
	ctx, span := base.GetTracer().Start(ctx, utility.FuncName())
	defer span.End()
	span.SetAttributes(base.MakeCodeAttributes()...)
	status, err := tag.meta.GetLoadStatus()
	if err != nil {
		return false, err
	}
	return status != HashTagStatusLoaded, nil
}

func (tag HashTag) Load(ctx context.Context, timeout time.Duration) (bool, int, error) {
	ctx, span := base.GetTracer().Start(ctx, utility.FuncName())
	defer span.End()
	span.SetAttributes(base.MakeCodeAttributes()...)
	if err := tag.acquireLoadLock(); err != nil {
		return false, 0, err
	}
	defer tag.releaseLoadLock()
	needToLoad, err := tag.NeedToLoad(ctx)
	if err != nil {
		return false, 0, err
	}
	if !needToLoad {
		return false, 0, nil
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	count, err := tag.loadKeys(ctx)
	if err != nil {
		return true, 0, err
	}
	return true, count, nil
}

func (tag HashTag) loadKeys(ctx context.Context) (int, error) {
	ctx, span := base.GetTracer().Start(ctx, utility.FuncName())
	defer span.End()
	span.SetAttributes(base.MakeCodeAttributes()...)
	startTime := time.Now()
	count := 0
	model, err := loadDataByIDWithContext(ctx, tag.dep.DB, tag.name)
	if err != nil {
		recordLoadDBError(tag.dep.Logger, tag.name, time.Since(startTime), err)
		return count, err
	}
	if model == nil {
		recordLoadDBRecordNotFound(tag.dep.Metric, tag.name, time.Since(startTime))
		return count, nil
	}
	recordLoadDBSuccess(tag.dep.Logger, tag.name, time.Since(startTime))
	startTime = time.Now()
	for key, value := range model.Value {
		if err := loadKeyToRedis(ctx, tag.dep.Redis, key, value); err != nil {
			recordLoadIntoRedisError(tag.dep.Logger, tag.dep.Metric, tag.name, time.Since(startTime), count, err)
			return count, err
		}
		count += 1
	}
	recordLoadIntoRedisSuccess(tag.dep.Logger, tag.dep.Metric, tag.name, time.Since(startTime), count)
	return count, nil
}

func getHashTagLockKey(hashTag string) string {
	return fmt.Sprintf("{%s}:_l", hashTag)
}

func (tag HashTag) lockKey() string {
	return getHashTagLockKey(tag.name)
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
	result, err := meta.dep.Redis.HGet(contextTODO, meta.metaKey, HashTagMetaInfoStatusFieldName).Result()
	// error is redis.Nil means that either metaKey or metaKey.status field does not exist.
	if errors.Is(err, redis.Nil) {
		return HashTagStatusNotExisted, nil
	}
	return result, err
}

func (meta HashTagMetaInfo) UpdateAccessTime(accessTime time.Time, accessMode base.HashTagAccessMode) error {
	if accessTime.IsZero() {
		return errors.New("access time is empty")
	}
	values := map[string]interface{}{
		HashTagMetaInfoStatusFieldName:     HashTagStatusLoaded,
		HashTagMetaInfoAccessTimeFieldName: utility.TimestampInMS(accessTime),
	}
	if accessMode == base.HashTagAccessModeWrite {
		values[HashTagMetaInfoWriteTimeFieldName] = utility.TimestampInMS(accessTime)
	}
	_, err := meta.dep.Redis.TxPipelined(contextTODO, func(pipeliner redis.Pipeliner) error {
		pipeliner.HSet(contextTODO, meta.metaKey, values)
		if accessMode == base.HashTagAccessModeWrite {
			pipeliner.HIncrBy(contextTODO, meta.metaKey, HashTagMetaInfoVersionFieldName, 1)
		} else {
			pipeliner.HSetNX(contextTODO, meta.metaKey, HashTagMetaInfoVersionFieldName, 0)
		}
		return nil
	})
	return err
}

func (meta HashTagMetaInfo) SetAsCleaned() error {
	_, err := meta.dep.Redis.Del(contextTODO, meta.metaKey).Result()
	return err
}

func (meta HashTagMetaInfo) GetAccessTime() (time.Time, error) {
	r, err := meta.dep.Redis.HGet(contextTODO, meta.metaKey, HashTagMetaInfoAccessTimeFieldName).Result()
	if err != nil {
		return time.Time{}, err
	}
	ts, err := strconv.ParseInt(r, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	seconds, nanoSeconds := utility.GetSecondsAndNanoSecondsFromTsInMs(ts)
	return time.Unix(seconds, nanoSeconds), nil
}

func Load(ctx context.Context, dep base.Dependency, tagName string, accessTime time.Time, accessMode base.HashTagAccessMode) error {
	ctx, span := base.GetTracer().Start(ctx, utility.FuncName())
	defer span.End()
	span.SetAttributes(base.MakeCodeAttributes()...)
	if tagName == "" {
		return nil
	}
	hashTag, err := NewHashTag(tagName, dep)
	if err != nil {
		return err
	}
	hashTagCacheService := base.GetHashTagLoadedCache()
	_, loaded := hashTagCacheService.Get(tagName)
	if loaded {
		hashTagCacheService.Set(tagName, true, 0)
		return hashTag.meta.UpdateAccessTime(accessTime, accessMode)
	}
	loadRetryTimes := base.GetServerConfig().LoadKey.GetRetryTimes()
	loadRetryInterval := base.GetServerConfig().LoadKey.GetRetryInterval()
	loadTimeout := base.GetServerConfig().LoadKey.GetLoadTimeout()
	for i := 0; i < loadRetryTimes; i++ {
		needToLoad, needToLoadErr := hashTag.NeedToLoad(ctx)
		if needToLoadErr != nil {
			recordLoadKeyCheckNeedToLoadError(dep.Logger, dep.Metric, tagName, needToLoadErr)
			return needToLoadErr
		}
		if !needToLoad {
			hashTagCacheService.Set(tagName, true, 0)
			return hashTag.meta.UpdateAccessTime(accessTime, accessMode)
		}
		startTime := time.Now()
		loaded, count, loadErr := hashTag.Load(ctx, loadTimeout)
		if loadErr != nil {
			err = loadErr
			if errors.Is(err, errLoadKeysLockFailed) {
				recordLoadKeyRetryLockFailed(dep.Logger, dep.Metric, tagName, i+1, count)
				time.Sleep(loadRetryInterval)
				continue
			}
			if errors.Is(err, context.DeadlineExceeded) {
				recordLoadKeyRetryTimeoutError(dep.Logger, dep.Metric, tagName, err, i+1, count)
				time.Sleep(loadRetryInterval)
				continue
			}
			recordLoadKeyError(dep.Logger, dep.Metric, tagName, err, time.Since(startTime), count)
			return err
		}
		if loaded {
			recordLoadKeySuccess(dep.Logger, dep.Metric, tagName, time.Since(startTime), count)
		}
		hashTagCacheService.Set(tagName, true, 0)
		return hashTag.meta.UpdateAccessTime(accessTime, accessMode)
	}
	return err
}

func loadKeyToRedis(ctx context.Context, client *redis.ClusterClient, key string, value RedisValue) error {
	ctx, span := base.GetTracer().Start(ctx, utility.FuncName())
	defer span.End()
	span.SetAttributes(base.MakeCodeAttributes()...)
	ttl := value.TTL(time.Now())
	if ttl == 0 {
		return nil
	}
	dataType := value.Type
	if !utility.StringSliceContains(supportedRedisDataTypes, dataType) {
		return fmt.Errorf("data type %s is not supported", dataType)
	}
	if dataType == stringType {
		expiration := time.Duration(0)
		if ttl > 0 {
			expiration = ttl
		}
		_, err := client.Set(ctx, key, value.Value, expiration).Result()
		return err
	}
	var size int
	if dataType == hashType || value.Type == zsetType {
		size = loadAndSaveStepSize * 2
	} else {
		size = loadAndSaveStepSize
	}
	slices, err := utility.ConvertJSONArrayIntoSlices(value.Value, size)
	if err != nil {
		return newParseError(err)
	}
	switch dataType {
	case setType:
		return loadSetToRedis(ctx, client, key, slices, ttl)
	case listType:
		return loadListToRedis(ctx, client, key, slices, ttl)
	case hashType:
		return loadHashToRedis(ctx, client, key, slices, ttl)
	case zsetType:
		return loadZSetToRedis(ctx, client, key, slices, ttl)
	}
	return nil
}

func loadSetToRedis(ctx context.Context, client *redis.ClusterClient, key string, slices [][]interface{}, ttl time.Duration) error {
	if ttl == 0 {
		return nil
	}
	pipeline := client.Pipeline()
	pipeline.Del(ctx, key)
	for _, slice := range slices {
		pipeline.SAdd(ctx, key, slice...)
	}
	if ttl > 0 {
		pipeline.Expire(ctx, key, ttl)
	}
	_, err := pipeline.Exec(ctx)
	return err
}

func loadListToRedis(ctx context.Context, client *redis.ClusterClient, key string, slices [][]interface{}, ttl time.Duration) error {
	if ttl == 0 {
		return nil
	}
	pipeline := client.Pipeline()
	pipeline.Del(ctx, key)
	for _, slice := range slices {
		pipeline.RPush(ctx, key, slice...)
	}
	if ttl > 0 {
		pipeline.Expire(ctx, key, ttl)
	}
	_, err := pipeline.Exec(ctx)
	return err
}

func loadHashToRedis(ctx context.Context, client *redis.ClusterClient, key string, slices [][]interface{}, ttl time.Duration) error {
	if ttl == 0 {
		return nil
	}
	pipeline := client.Pipeline()
	pipeline.Del(ctx, key)
	for _, slice := range slices {
		if len(slice)%2 != 0 {
			return errDataFormatError
		}
		pipeline.HSet(ctx, key, slice...)
	}
	if ttl > 0 {
		pipeline.Expire(ctx, key, ttl)
	}
	_, err := pipeline.Exec(ctx)
	return err
}

func loadZSetToRedis(ctx context.Context, client *redis.ClusterClient, key string, slices [][]interface{}, ttl time.Duration) error {
	if ttl == 0 {
		return nil
	}
	pipeline := client.Pipeline()
	pipeline.Del(ctx, key)
	for _, slice := range slices {
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
	if ttl > 0 {
		pipeline.Expire(ctx, key, ttl)
	}
	_, err := pipeline.Exec(ctx)
	return err
}
