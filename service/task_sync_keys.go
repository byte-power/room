package service

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"bytepower_room/utility"
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/ratelimit"
)

const SyncKeysTaskName = "sync_keys"

var errTaskPanic = errors.New("task panic")

// find keys to sync
// select * from table where status = "syncing";
// update table set status = "synced", syncedAt = time.Now() where hash_tag = "xxx" and version = xx
func SyncKeysTask(dep base.Dependency, config base.SyncKeyTaskConfig) {
	startTime := time.Now()
	logTaskStart(dep.Logger, SyncKeysTaskName, startTime, log.Any("config", config))

	count := 1000
	var err error
	var lastModel *roomHashTagKeys
	lastTableIndex := 0
	defer func() {
		if panicInfo := recover(); panicInfo != nil {
			info := make(map[string]string)
			if lastModel != nil {
				info["hash_tag"] = lastModel.HashTag
				info["table_index"] = fmt.Sprintf("%d", lastTableIndex)
			}
			info["info"] = fmt.Sprintf("%+v", panicInfo)
			info["stack"] = string(debug.Stack())
			recordTaskError(
				dep.Logger, dep.Metric, SyncKeysTaskName,
				errTaskPanic, "panic", info,
			)
		} else if err == nil {
			recordTaskSuccess(dep.Logger, dep.Metric, SyncKeysTaskName, time.Since(startTime))
		}
	}()
	ratelimitBucket := ratelimit.New(config.RateLimitPerSecond)
	writtenAt := startTime.Add(-config.NoWrittenDuration)
	conditions := [][]dbWhereCondition{
		{
			dbWhereCondition{column: "status", operator: "=?", parameter: HashTagKeysStatusNeedSynced},
			dbWhereCondition{column: "written_at", operator: "<=?", parameter: writtenAt},
		}, {
			dbWhereCondition{column: "status", operator: "=?", parameter: HashTagKeysStatusNeedSynced},
			dbWhereCondition{column: "written_at", operator: "is ?", parameter: nil},
		},
	}
	for _, condition := range conditions {
		tableIndex := 0
		for {
			index, models, loadErr := loadHashTagKeysModelsByCondition(dep.DB, count, tableIndex, condition...)
			// dbWhereCondition{column: "status", operator: "=?", parameter: HashTagKeysStatusNeedSynced},
			// dbWhereCondition{column: "written_at", operator: "<=?", parameter: writtenAt})
			if loadErr != nil {
				recordTaskError(dep.Logger, dep.Metric, SyncKeysTaskName, loadErr, "load_hash_tag_keys", nil)
				err = loadErr
				return
			}
			if len(models) == 0 {
				break
			}
			tableIndex = index
			lastTableIndex = index
			processCount := 0
			for _, model := range models {
				ratelimitBucket.Take()
				lastModel = model
				if err = syncRoomData(dep.DB, dep.Redis, model, time.Now(), config.UpSertTryTimes); err != nil {
					if isRetryErrorForUpdateInTx(err) {
						recordTaskError(
							dep.Logger, dep.Metric,
							SyncKeysTaskName, err,
							"sync_keys.retry_error",
							map[string]string{
								"hash_tag": model.HashTag,
								"keys":     strings.Join(model.Keys, " "),
							},
						)
						continue
					}
					recordTaskError(
						dep.Logger, dep.Metric, SyncKeysTaskName,
						err, "sync_room_data",
						map[string]string{"hash_tag": model.HashTag, "keys": strings.Join(model.Keys, " ")},
					)
					return
				}
				ratelimitBucket.Take()
				sizeInfo, err := getHashTagKeySizeInfo(dep.Redis, model.HashTag, model.Keys)
				if err != nil {
					recordTaskError(
						dep.Logger, dep.Metric, SyncKeysTaskName,
						err, "get_hash_tag_keys_size",
						map[string]string{"hash_tag": model.HashTag, "keys": strings.Join(model.Keys, " ")},
					)
				} else {
					recordHashTagKeysSizeInfo(dep.Logger, dep.Metric, sizeInfo, config)
				}

				processCount += 1
			}
			conditionStrs := make([]string, 0, len(condition))
			for _, cond := range condition {
				conditionStrs = append(conditionStrs, cond.string())
			}
			dep.Logger.Info(
				"sync_keys",
				log.String("task", SyncKeysTaskName),
				log.Int("count", processCount),
				log.Int("table_index", tableIndex),
				log.String("condition", strings.Join(conditionStrs, " and ")),
			)
			metricName := fmt.Sprintf("%s.success.sync_hash_tag", SyncKeysTaskName)
			dep.Metric.MetricCount(metricName, processCount)
		}
	}
}

func syncRoomData(db *base.DBCluster, redisCluster *redis.ClusterClient, model *roomHashTagKeys, t time.Time, tryTimes int) error {
	if err := syncHashTagKeys(db, redisCluster, model.HashTag, model.Keys, tryTimes); err != nil {
		return err
	}
	if err := model.SetStatusAsSynced(db, t); err != nil {
		return err
	}
	return nil
}

func syncHashTagKeys(db *base.DBCluster, redisCluster *redis.ClusterClient, hashTag string, keys []string, tryTimes int) error {
	value := make(map[string]RedisValue)
	for _, key := range keys {
		v, err := getValueFromRedis(redisCluster, key)
		if err != nil {
			return err
		}
		if !v.IsZero() {
			value[key] = v
		}
	}
	err := upsertRoomDataValue(db, hashTag, value, tryTimes)
	if err != nil {
		return err
	}
	return nil
}

const redisKeyNotExist = "none"

func getValueFromRedis(redisCluster *redis.ClusterClient, key string) (RedisValue, error) {
	currentTime := time.Now()

	// Get redis key type.
	keyType, err := redisCluster.Type(contextTODO, key).Result()
	if err != nil {
		return RedisValue{}, err
	}
	if keyType == redisKeyNotExist {
		return RedisValue{}, nil
	}

	keyValue, err := serializeValue(redisCluster, keyType, key)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return RedisValue{}, nil
		}
		return RedisValue{}, err
	}

	ttl, err := redisCluster.PTTL(contextTODO, key).Result()
	if err != nil {
		return RedisValue{}, err
	}
	// Key does not exist.
	if ttl == -2 {
		return RedisValue{}, nil
	}

	value := RedisValue{Type: keyType, Value: keyValue}

	if ttl > 0 {
		value.ExpireTs = utility.TimestampInMS(currentTime.Add(ttl))
	}
	return value, nil
}

func serializeValue(redisCluster *redis.ClusterClient, keyType, key string) (string, error) {
	value, err := getValueByKeyFromRedis(redisCluster, keyType, key)
	if err != nil {
		return "", err
	}
	switch keyType {
	case stringType:
		return value[0], nil
	default:
		v, err := json.Marshal(value)
		if err != nil {
			return "", err
		}
		return string(v), nil
	}
}

func getValueByKeyFromRedis(redisCluster *redis.ClusterClient, keyType, key string) ([]string, error) {
	var result []string
	var err error
	switch keyType {
	case stringType:
		value, stringErr := redisCluster.Get(contextTODO, key).Result()
		err = stringErr
		result = []string{value}
	case listType, hashType, zsetType, setType:
		result, err = serializeNonStringValue(redisCluster, key, keyType)
	default:
		err = fmt.Errorf("not supported key type: %s", keyType)
	}
	return result, err
}

func serializeNonStringValue(redisCluster *redis.ClusterClient, key, keyType string) ([]string, error) {
	// list type
	if keyType == listType {
		items, err := redisCluster.LRange(contextTODO, key, 0, -1).Result()
		if err != nil {
			return nil, err
		}
		return items, nil
	}

	// set, hash, zset type
	var scanStep int64 = 100
	var scan func(context.Context, string, uint64, string, int64) *redis.ScanCmd
	if keyType == hashType {
		scan = redisCluster.HScan
	} else if keyType == setType {
		scan = redisCluster.SScan
	} else if keyType == zsetType {
		scan = redisCluster.ZScan
	} else {
		return nil, fmt.Errorf("data type %s is not supported", keyType)
	}

	var items []string
	var cursor uint64 = 0
	for {
		itemsInScan, c, err := scan(contextTODO, key, cursor, "", scanStep).Result()
		if err != nil {
			return nil, err
		}
		items = append(items, itemsInScan...)
		cursor = c
		if cursor == 0 {
			break
		}
	}
	if len(items) == 0 {
		return items, redis.Nil
	}
	return items, nil
}

type hashTagKeysSizeInfo struct {
	hashTag      string
	memoryBytes  int64
	keysSizeInfo []keySizeInfo
}

func (info hashTagKeysSizeInfo) String() string {
	var builder strings.Builder
	fmt.Fprintf(&builder, "hash_tag=%s, memoryBytes=%d, keys=[", info.hashTag, info.memoryBytes)
	for _, keyInfo := range info.keysSizeInfo {
		fmt.Fprintf(&builder, " (%s) ", keyInfo.String())
	}
	builder.WriteString("]")
	return builder.String()
}

type keySizeInfo struct {
	key         string
	memoryBytes int64
	itemCount   int64
}

func (info keySizeInfo) isEmpty() bool {
	return info.key == "" || info.memoryBytes == 0 || info.itemCount == 0
}

func (info keySizeInfo) String() string {
	return fmt.Sprintf("key=%s, memoryBytes=%d, itemCount=%d", info.key, info.memoryBytes, info.itemCount)
}

const memoryUsageSample = 10

func getHashTagKeySizeInfo(client *redis.ClusterClient, hashTag string, keys []string) (hashTagKeysSizeInfo, error) {
	hashTagInfo := hashTagKeysSizeInfo{hashTag: hashTag, keysSizeInfo: make([]keySizeInfo, 0)}
	for _, key := range keys {
		keyInfo, err := getKeySizeInfo(client, key)
		if err != nil {
			return hashTagKeysSizeInfo{}, err
		}
		if !keyInfo.isEmpty() {
			hashTagInfo.keysSizeInfo = append(hashTagInfo.keysSizeInfo, keyInfo)
			hashTagInfo.memoryBytes += keyInfo.memoryBytes
		}
	}
	return hashTagInfo, nil
}

func getKeySizeInfo(client *redis.ClusterClient, key string) (keySizeInfo, error) {
	ctx := context.TODO()
	typ, err := client.Type(ctx, key).Result()
	if err != nil {
		return keySizeInfo{}, err
	}
	if typ == redisKeyNotExist {
		return keySizeInfo{}, nil
	}
	itemCount, err := getItemCount(client, key, typ)
	if err != nil {
		return keySizeInfo{}, err
	}
	memorySize, err := client.MemoryUsage(ctx, key, memoryUsageSample).Result()
	if err != nil {
		return keySizeInfo{}, err
	}
	return keySizeInfo{key: key, memoryBytes: memorySize, itemCount: itemCount}, nil
}

func getItemCount(client *redis.ClusterClient, key, typ string) (int64, error) {
	ctx := context.TODO()
	switch typ {
	case stringType:
		return 0, nil
	case listType:
		return client.LLen(ctx, key).Result()
	case hashType:
		return client.HLen(ctx, key).Result()
	case zsetType:
		return client.ZCard(ctx, key).Result()
	case setType:
		return client.SCard(ctx, key).Result()
	default:
		return 0, fmt.Errorf("unsupported type %s", typ)
	}
}

const (
	metricHashTagBytes                 = "size_monitor.hash_tag_bytes"
	metricHashTagKeys                  = "size_monitor.hash_tag_keys"
	metricHashTagBytesOverLimit        = "size_monitor.hash_tag_bytes_over_limit"
	metricHashTagKeyCountOverLimit     = "size_monitor.hash_tag_key_count_over_limit"
	metircHashTagKeyBytesOverLimit     = "size_monitor.hash_tag_key_bytes_over_limit"
	metricHashTagKeyItemCountOverLimit = "size_monitor.hash_tag_key_item_count_over_limit"
)

func recordHashTagKeysSizeInfo(
	logger *log.Logger, metric *base.MetricClient,
	hashTagSizeInfo hashTagKeysSizeInfo,
	config base.SyncKeyTaskConfig) {

	metric.MetricGauge(metricHashTagBytes, hashTagSizeInfo.memoryBytes)
	metric.MetricGauge(metricHashTagKeys, len(hashTagSizeInfo.keysSizeInfo))
	logSubject := "hash_tag_info"
	if hashTagSizeInfo.memoryBytes >= config.HashTagSizeLimitBytes {
		metric.MetricGauge(metricHashTagBytesOverLimit, hashTagSizeInfo.memoryBytes)
		logger.Info(metricHashTagBytesOverLimit, log.String(logSubject, hashTagSizeInfo.String()))
	}
	if len(hashTagSizeInfo.keysSizeInfo) >= int(config.HashTagKeyCountLimit) {
		metric.MetricGauge(metricHashTagKeyCountOverLimit, len(hashTagSizeInfo.keysSizeInfo))
		logger.Info(metricHashTagKeyCountOverLimit, log.String(logSubject, hashTagSizeInfo.String()))
	}
	for _, info := range hashTagSizeInfo.keysSizeInfo {
		if info.memoryBytes >= config.KeySizeLimitBytes {
			metric.MetricGauge(metircHashTagKeyBytesOverLimit, info.memoryBytes)
			logger.Info(metircHashTagKeyBytesOverLimit, log.String("key", info.key), log.String(logSubject, hashTagSizeInfo.String()))
		}
		if info.itemCount >= config.KeyItemCountLimit {
			metric.MetricGauge(metricHashTagKeyItemCountOverLimit, info.itemCount)
			logger.Info(metricHashTagKeyItemCountOverLimit, log.String("key", info.key), log.String(logSubject, hashTagSizeInfo.String()))
		}
	}
}
