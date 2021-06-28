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

var ErrEmptyHashTag = errors.New("hash tag is empty")

func newParseError(err error) error {
	return fmt.Errorf("parse value error, %w", err)
}

const (
	loadAndSaveStepSize = 100

	HashTagStatusLoaded                = "L"
	HashTagStatusCleaned               = "C"
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
	if err := tag.meta.SetAsCleaned(); err != nil {
		return err
	}
	_, err := tag.dep.Redis.Del(contextTODO, keys...).Result()
	return err
}

func (tag HashTag) CleanKeysV2(accessedAt time.Time, keys ...string) error {
	if err := tag.acquireLoadLock(); err != nil {
		return err
	}
	defer tag.releaseLoadLock()
	t, err := tag.meta.GetAccessTime()
	if t.After(accessedAt) {
		return nil
	}
	if err := tag.meta.SetAsCleaned(); err != nil {
		return err
	}
	_, err = tag.dep.Redis.Del(contextTODO, keys...).Result()
	return err
}

func (tag HashTag) GetLoadStatus() (string, error) {
	return tag.meta.GetLoadStatus()
}

func (tag HashTag) NeedToLoad() (bool, error) {
	status, err := tag.meta.GetLoadStatus()
	if err != nil {
		return false, err
	}
	if status == HashTagStatusLoaded {
		return false, nil
	}
	if status == HashTagStatusNotExisted {
		return false, nil
	}
	return true, nil
}

func (tag HashTag) Load(timeout time.Duration) (bool, int, error) {
	if err := tag.acquireLoadLock(); err != nil {
		return false, 0, err
	}
	defer tag.releaseLoadLock()
	needToLoad, err := tag.NeedToLoad()
	if err != nil {
		return false, 0, err
	}
	if !needToLoad {
		return false, 0, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	count, err := tag.loadKeys(ctx)
	if err != nil {
		return true, 0, err
	}
	return true, count, nil
}

func (tag HashTag) loadKeys(ctx context.Context) (int, error) {
	startTime := time.Now()
	count := 0
	model, err := loadDataByIDWithContext(ctx, tag.dep.DB, tag.name)
	if err != nil {
		recordLoadDBError(tag.dep.Logger, tag.name, time.Since(startTime), err)
		return count, err
	}
	recordLoadDBSuccess(tag.dep.Logger, tag.name, time.Since(startTime))
	if model == nil {
		recordLoadDBRecordNotFound(tag.dep.Logger, tag.dep.Metric, tag.name)
		return count, nil
	}
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
		pipeliner.HIncrBy(contextTODO, meta.metaKey, HashTagMetaInfoVersionFieldName, 1)
		return nil
	})
	return err
}

func (meta HashTagMetaInfo) SetAsCleaned() error {
	_, err := meta.dep.Redis.HSet(
		contextTODO, meta.metaKey, HashTagMetaInfoStatusFieldName,
		HashTagStatusCleaned).Result()
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

func Load(tagName string, accessTime time.Time, accessMode base.HashTagAccessMode) error {
	if tagName == "" {
		return nil
	}
	dep := base.GetServerDependency()
	hashTag, err := NewHashTag(tagName, dep)
	if err != nil {
		return err
	}
	loadRetryTimes := base.GetServerConfig().LoadKey.GetRetryTimes()
	loadRetryInterval := base.GetServerConfig().LoadKey.GetRetryInterval()
	loadTimeout := base.GetServerConfig().LoadKey.GetLoadTimeout()
	for i := 0; i < loadRetryTimes; i++ {
		needToLoad, needToLoadErr := hashTag.NeedToLoad()
		if needToLoadErr != nil {
			recordLoadKeyCheckNeedToLoadError(dep.Logger, dep.Metric, tagName, needToLoadErr)
			return needToLoadErr
		}
		if !needToLoad {
			return hashTag.meta.UpdateAccessTime(accessTime, accessMode)
		}
		startTime := time.Now()
		loaded, count, loadErr := hashTag.Load(loadTimeout)
		if loadErr != nil {
			err = loadErr
			if isRetryLoadError(err) {
				time.Sleep(loadRetryInterval)
				recordLoadKeyRetryError(dep.Logger, dep.Metric, tagName, err, i+1, count)
				continue
			}
			recordLoadKeyError(dep.Logger, dep.Metric, tagName, err, time.Since(startTime), count)
			return err
		}
		if loaded {
			recordLoadKeySuccess(dep.Logger, dep.Metric, tagName, time.Since(startTime), count)
		}
		return hashTag.meta.UpdateAccessTime(accessTime, accessMode)
	}
	return err
}

func loadKeyToRedis(ctx context.Context, client *redis.ClusterClient, key string, value RedisValue) error {
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

func isRetryLoadError(err error) bool {
	return errors.Is(err, errLoadKeysLockFailed) || errors.Is(err, context.DeadlineExceeded)
}
