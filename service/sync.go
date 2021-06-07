package service

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"bytepower_room/commands"
	"bytepower_room/utility"
	"bytes"
	"context"
	"strings"

	"errors"
	"fmt"
	"time"

	"github.com/go-pg/pg/v10"
	"github.com/go-redis/redis/v8"
)

const redisKeyNotExist = "none"

func recordTaskSuccess(taskName string, d time.Duration) {
	recordTaskSuccessLog(taskName, d)
	recordTaskSuccessMetric(taskName, d)
}

func recordTaskSuccessLog(taskName string, d time.Duration) {
	logger := base.GetTaskLogger()
	logger.Info(
		"task success",
		log.String("task", taskName),
		log.String("duration", d.String()),
	)
}

func recordTaskSuccessMetric(taskName string, d time.Duration) {
	metric := base.GetTaskMetricService()
	metricName := fmt.Sprintf("%s.success", taskName)
	metric.MetricIncrease(metricName)
	if d != time.Duration(0) {
		durationMetricName := fmt.Sprintf("%s.duration", metricName)
		metric.MetricTimeDuration(durationMetricName, d)
	}
}

func recordTaskError(taskName string, err error, reason string, ctxInfo map[string]string) {
	recordTaskErrorLog(taskName, err, reason, ctxInfo)
	recordTaskErrorMetric(taskName, reason)
}

func recordTaskErrorLog(taskName string, err error, reason string, ctxInfo map[string]string) {
	logger := base.GetTaskLogger()
	logPairs := make([]log.LogPair, 0)
	logPairs = append(logPairs, log.String("task", taskName))
	if reason != "" {
		logPairs = append(logPairs, log.String("reason", reason))
	}
	for key, value := range ctxInfo {
		logPairs = append(logPairs, log.String(key, value))
	}
	if err != nil {
		logPairs = append(logPairs, log.Error(err))
	}
	logger.Error("task error", logPairs...)
}

func recordTaskErrorMetric(taskName string, reasons ...string) {
	metric := base.GetTaskMetricService()
	metricName := fmt.Sprintf("%s.error", taskName)
	metric.MetricIncrease(metricName)
	for _, reason := range reasons {
		errorMetricName := fmt.Sprintf("%s.%s", metricName, reason)
		metric.MetricIncrease(errorMetricName)
	}
}

func logTaskStart(taskName string, startTime time.Time) {
	logger := base.GetTaskLogger()
	logger.Info(
		"start task",
		log.String("task", taskName),
		log.String("start_time", startTime.String()),
	)
}

const syncRecordsProcessMessageIntervalDuration = 10 * time.Millisecond

// SyncRecordsTask syncs accessed and written keys to Record Database
// 1. Get access record file names from SQS
// 2. Download S3 file
// 3. Parse access record file line by line
// 4. Update record database
// This task should be idempotent:
// 1. every SQS message could be processed more than one time
// 2. every S3 file could be processed more than one time
// 3. every record could be processed more than one time
func SyncRecordsTask() error {
	taskName := "sync_records"
	startTime := time.Now()
	logTaskStart(taskName, startTime)
	metric := base.GetTaskMetricService()
	logger := base.GetTaskLogger()
	s3Config := base.GetServerConfig().SyncService.S3

	// Receive messages from SQS.
	sqsConfig := base.GetServerConfig().SyncService.SQS
	service, err := base.NewSQSService(sqsConfig)
	if err != nil {
		recordTaskError(taskName, err, "new_sqs_service", nil)
		return err
	}
	for {
		messages, err := service.ReceiveMessages()
		if err != nil {
			recordTaskError(taskName, err, "receive_sqs_message", nil)
			return err
		}

		if len(messages) == 0 {
			break
		}

		for _, message := range messages {
			time.Sleep(syncRecordsProcessMessageIntervalDuration)
			logger.Info(
				"start process sqs message",
				log.String("task", taskName),
				log.String("message", message.String()),
			)
			s3MetaInfoSlice, err := base.GetS3MetaInfoFromNotificationMessage(message)
			if err != nil {
				recordTaskError(
					taskName, err, "parse_sqs_message",
					map[string]string{"message": message.String()})

				if err = service.DeleteMessage(message); err != nil {
					recordTaskError(
						taskName, err, "delete_sqs_message",
						map[string]string{"message": message.String()})
				}
				continue
			}
			if len(s3MetaInfoSlice) == 0 {
				recordTaskError(
					taskName, nil, "sqs_message_invalid_format",
					map[string]string{"message": message.String()})

				// delete invalid format message
				if err = service.DeleteMessage(message); err != nil {
					recordTaskError(
						taskName, err, "delete_sqs_message",
						map[string]string{"message": message.String()})
				}
				continue
			}
			fileContents, err := downloadS3Files(s3Config, s3MetaInfoSlice...)
			if err != nil {
				recordTaskError(
					taskName, err, "download_s3_file",
					map[string]string{"message": message.String()})
				continue
			}
			for _, metaInfo := range s3MetaInfoSlice {
				logger.Info(
					"download s3 file success",
					log.String("task", taskName),
					log.String("bucket", metaInfo.Bucket),
					log.String("key", metaInfo.Key),
				)
			}
			accessedMap, writtenMap, errIndex, err := processAccessFiles(fileContents)
			if err != nil {
				recordTaskError(
					taskName, err, "process_access_file",
					map[string]string{
						"bucket":   s3MetaInfoSlice[errIndex].Bucket,
						"key":      s3MetaInfoSlice[errIndex].Key,
						"messsage": message.String()})
				continue
			}

			accessedModels := accessedMapToModels(accessedMap)
			writtenModels := writtenMapToModels(writtenMap)
			if err := bulkUpsertAccessedRecordModelsV2(accessedModels...); err != nil {
				recordTaskError(
					taskName, err, "bulk_upsert_access_record",
					map[string]string{"message": message.String()})
				continue
			}
			if err := bulkUpsertWrittenRecordModels(writtenModels...); err != nil {
				recordTaskError(
					taskName, err, "bulk_upsert_written_record",
					map[string]string{"message": message.String()})
				continue
			}
			if err := service.DeleteMessage(message); err != nil {
				recordTaskError(
					taskName, err, "delete_sqs_message",
					map[string]string{"message": message.String()})
				continue
			}
			logger.Info(
				"sync records success",
				log.String("task", taskName),
				log.String("sqs_message", message.String()),
				log.Int("s3_files", len(fileContents)),
				log.Int("accessed_record", len(accessedMap)),
				log.Int("written_record", len(writtenMap)))

			metric.MetricIncrease(fmt.Sprintf("%s.success.sqs_message", taskName))
			metric.MetricCount(fmt.Sprintf("%s.success.s3_files", taskName), len(fileContents))
			metric.MetricCount(fmt.Sprintf("%s.success.accessed_record", taskName), len(accessedMap))
			metric.MetricCount(fmt.Sprintf("%s.success.written_record", taskName), len(writtenMap))
		}
	}
	recordTaskSuccess(taskName, time.Since(startTime))
	return nil
}

func mergeMapsByTime(maps ...map[string]time.Time) map[string]time.Time {
	result := maps[0]
	for _, m := range maps[1:] {
		for key, t := range m {
			if v, ok := result[key]; !ok {
				result[key] = t
			} else if t.After(v) {
				result[key] = t
			}
		}
	}
	return result
}

func accessedMapToModels(m map[string]time.Time) []*roomAccessedRecordModelV2 {
	currentTime := time.Now()
	models := make([]*roomAccessedRecordModelV2, len(m))
	index := 0
	for hashTag, t := range m {
		model := &roomAccessedRecordModelV2{HashTag: hashTag, AccessedAt: t, CreatedAt: currentTime}
		models[index] = model
		index++
	}
	return models
}

func writtenMapToModels(m map[string]time.Time) []*roomWrittenRecordModel {
	currentTime := time.Now()
	models := make([]*roomWrittenRecordModel, len(m))
	index := 0
	for key, t := range m {
		model := &roomWrittenRecordModel{Key: key, WrittenAt: t, CreatedAt: currentTime}
		models[index] = model
		index++
	}
	return models
}

func downloadS3Files(s3Config base.S3Config, metaInfos ...base.S3MetaInfo) ([][]byte, error) {
	contents := make([][]byte, len(metaInfos))
	for index, metaInfo := range metaInfos {
		s3Service, err := base.NewS3Service(s3Config, metaInfo.Bucket)
		if err != nil {
			return nil, err
		}
		content, err := s3Service.GetContentByKey(metaInfo.Key)
		if err != nil {
			return nil, err
		}
		contents[index] = content
	}
	return contents, nil
}

func processAccessFiles(contents [][]byte) (map[string]time.Time, map[string]time.Time, int, error) {
	accessedMaps := make([]map[string]time.Time, len(contents))
	writtenMaps := make([]map[string]time.Time, len(contents))
	for index, content := range contents {
		accessedMap, writtenMap, err := processAccessFile(content)
		if err != nil {
			return nil, nil, index, err
		}
		accessedMaps[index] = accessedMap
		writtenMaps[index] = writtenMap
	}
	accessedMap := mergeMapsByTime(accessedMaps...)
	writtenMap := mergeMapsByTime(writtenMaps...)
	return accessedMap, writtenMap, 0, nil
}

func processAccessFile(content []byte) (map[string]time.Time, map[string]time.Time, error) {
	accessedMap := make(map[string]time.Time)
	writtenMap := make(map[string]time.Time)
	accessEventsInBytes := bytes.Split(content, []byte("\n"))
	for _, eventInBytes := range accessEventsInBytes {
		eventInBytes = bytes.Trim(eventInBytes, "\n \t")
		// Skip empty lines
		if len(eventInBytes) == 0 {
			continue
		}
		event, err := processAccessEvent(eventInBytes)
		if err != nil {
			return nil, nil, err
		}
		if event.AccessMode == base.HashTagAccessModeWrite {
			writtenMap[event.HashTag] = getLaterTime(writtenMap[event.HashTag], event.AccessTime)
		}
		hashTag := commands.ExtractHashTagFromKey(event.HashTag)
		if hashTag != "" {
			accessedMap[hashTag] = getLaterTime(accessedMap[hashTag], event.AccessTime)
		}
	}
	return accessedMap, writtenMap, nil
}

func getLaterTime(t1, t2 time.Time) time.Time {
	if t1.After(t2) {
		return t1
	}
	return t2
}

func processAccessEvent(eventBytes []byte) (*base.Event, error) {
	event := &base.Event{}
	if err := json.Unmarshal(eventBytes, event); err != nil {
		return nil, err
	}
	if event.AccessMode == "" {
		return nil, base.ErrEventAccessModeEmpty
	}
	if event.HashTag == "" {
		return nil, base.ErrEventHashKeyEmpty
	}
	return event, nil
}

const syncKeysIntervalDuration = 10 * time.Millisecond

// SyncKeysTask syncs written keys to room databse
// 1. Get key and written time from record database
// 2. Sync to room database
// 3. Remove records from record database
func SyncKeysTask(upsertTryTimes int) error {
	startTime := time.Now()
	logger := base.GetTaskLogger()
	taskName := "sync_keys"
	dep := base.GetTaskDependency()
	logTaskStart(taskName, startTime)
	metric := base.GetTaskMetricService()
	count := 100
	for {
		models, err := loadWrittenRecordModels(count)
		if err != nil {
			recordTaskError(taskName, err, "load_written_record", nil)
			return err
		}
		if len(models) == 0 {
			break
		}
		updatedCount := 0
		deletedCount := 0
		for _, model := range models {
			time.Sleep(syncKeysIntervalDuration)
			key := model.Key
			hashTag, err := NewHashTag(commands.ExtractHashTagFromKey(key), dep)
			if err != nil {
				if errors.Is(err, ErrEmptyHashTag) {
					if err := deleteRoomWrittenRecordModel(dep.WrittenRecordDB, key, model.WrittenAt); err != nil {
						recordTaskError(
							taskName, err, "delete_written_record",
							map[string]string{
								"hash_tag":   hashTag.Name(),
								"key":        key,
								"written_at": model.WrittenAt.String(),
							},
						)
						return err
					}
					continue
				}
				recordTaskError(
					taskName, err, "new_hash_tag",
					map[string]string{
						"hash_tag": hashTag.Name(),
						"key":      key,
					},
				)
				return err
			}
			status, err := hashTag.GetLoadStatus()
			if err != nil {
				return err
			}
			if status != HashTagStatusLoaded {
				recordTaskError(taskName, nil, "load_status_not_loaded", map[string]string{"key": model.Key})
				//TODO: need to update
				if err := Load(hashTag.Name(), time.Now(), base.HashTagAccessModeRead); err != nil {
					recordTaskError(
						taskName, err, "load_hash_tag",
						map[string]string{"hash_tag": hashTag.Name()},
					)
					return err
				}
				continue
			}
			value, err := getValueFromRedis(key)
			if err != nil {
				recordTaskError(
					taskName, err, "get_value_from_redis",
					map[string]string{
						"hash_tag": hashTag.Name(),
						"key":      key,
					},
				)
				return err
			}
			if value.IsZero() {
				if err := deleteRoomData(dep.DB, hashTag.Name(), key); err != nil {
					recordTaskError(
						taskName, err, "delete_room_data_key",
						map[string]string{
							"hash_key": hashTag.Name(),
							"key":      key,
						},
					)
					return err
				}
				deletedCount += 1
			} else {
				if err := upsertRoomData(dep.DB, hashTag.Name(), key, value, upsertTryTimes); err != nil {
					recordTaskError(
						taskName, err, "upsert_room_data_key",
						map[string]string{
							"hash_key": hashTag.Name(),
							"key":      key,
							"value":    value.String(),
						},
					)
					return err
				}
				updatedCount += 1
			}
			if err := deleteRoomWrittenRecordModel(dep.WrittenRecordDB, key, model.WrittenAt); err != nil {
				recordTaskError(
					taskName, err, "delete_written_record",
					map[string]string{
						"hash_tag":   hashTag.Name(),
						"key":        key,
						"written_at": model.WrittenAt.String(),
					},
				)
				return err
			}
		}
		logger.Info(
			"sync keys success",
			log.String("task", taskName),
			log.Int("updated", updatedCount),
			log.Int("deleted", deletedCount),
			log.Int("count", len(models)),
		)
		metric.MetricCount(fmt.Sprintf("%s.success.updated", taskName), updatedCount)
		metric.MetricCount(fmt.Sprintf("%s.success.deleted", taskName), deletedCount)
		metric.MetricCount(fmt.Sprintf("%s.success.count", taskName), len(models))
	}
	recordTaskSuccess(taskName, time.Since(startTime))
	return nil
}

func deleteWrittenRecordModels(models []*roomWrittenRecordModel) error {
	clusterModels, err := getClusterModelsFromWrittenRecordModels(models...)
	if err != nil {
		return err
	}
	for _, clusterModel := range clusterModels {
		keys := make([]string, len(clusterModel.models))
		for index, model := range clusterModel.models {
			keys[index] = model.Key
		}
		_, err := clusterModel.client.Model((*roomWrittenRecordModel)(nil)).
			Table(clusterModel.tableName).
			Where("key in (?)", pg.In(keys)).Delete()
		if err != nil {
			return err
		}
	}
	return nil
}

func getValueFromRedis(key string) (RedisValue, error) {
	redisClient := base.GetRedisCluster()
	currentTime := time.Now()

	// Get redis key type.
	keyType, err := redisClient.Type(contextTODO, key).Result()
	if err != nil {
		return RedisValue{}, err
	}
	if keyType == redisKeyNotExist {
		return RedisValue{}, nil
	}

	keyValue, err := serializeValue(keyType, key)
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return RedisValue{}, nil
		}
		return RedisValue{}, err
	}

	ttl, err := redisClient.PTTL(contextTODO, key).Result()
	if err != nil {
		return RedisValue{}, err
	}
	// Key does not exist.
	if ttl == -2 {
		return RedisValue{}, nil
	}

	value := RedisValue{
		Type: keyType, Value: keyValue,
		SyncedTs: utility.TimestampInMS(currentTime),
	}

	if ttl > 0 {
		value.ExpireTs = utility.TimestampInMS(currentTime.Add(ttl))
	}
	return value, nil
}

func serializeValue(keyType, key string) (string, error) {
	value, err := getValueByKeyFromRedis(keyType, key)
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

func getValueByKeyFromRedis(keyType, key string) ([]string, error) {
	redisClient := base.GetRedisCluster()
	var result []string
	var err error
	switch keyType {
	case stringType:
		value, stringErr := redisClient.Get(contextTODO, key).Result()
		err = stringErr
		result = []string{value}
	case listType, hashType, zsetType, setType:
		result, err = serializeNonStringValue(key, keyType)
	default:
		err = fmt.Errorf("not supported key type: %s", keyType)
	}
	return result, err
}

func serializeNonStringValue(key, keyType string) ([]string, error) {
	redisClient := base.GetRedisCluster()
	// list type
	if keyType == listType {
		items, err := redisClient.LRange(contextTODO, key, 0, -1).Result()
		if err != nil {
			return nil, err
		}
		return items, nil
	}

	// set, hash, zset type
	var scanStep int64 = 100
	var scan func(context.Context, string, uint64, string, int64) *redis.ScanCmd
	if keyType == hashType {
		scan = redisClient.HScan
	} else if keyType == setType {
		scan = redisClient.SScan
	} else if keyType == zsetType {
		scan = redisClient.ZScan
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

const cleanKeysIntervalDuration = 10 * time.Millisecond

// CleanKeysTask cleans inactive keys in redis cluster
// 1. Get key and access time from record database
// 2. Clean key and metaKey from redis cluster
// 3. Remove records from record database
func CleanKeysTask(inactiveDuration time.Duration) error {
	startTime := time.Now()
	logger := base.GetTaskLogger()
	taskName := "clean_keys"
	count := 100
	inactiveTime := time.Now().Add(-1 * inactiveDuration)
	metric := base.GetTaskMetricService()
	dep := base.GetTaskDependency()
	logTaskStart(taskName, startTime)
	excludedHashTags := utility.NewStringSet()
	for {
		time.Sleep(cleanKeysIntervalDuration)
		models, err := loadAccessedRecordModels(count, inactiveTime, excludedHashTags.ToSlice())
		if err != nil {
			recordTaskError(taskName, err, "load_accessed_record", nil)
			return err
		}
		if len(models) == 0 {
			break
		}
		var deletedModels []*roomAccessedRecordModelV2
		for _, recordModel := range models {
			hashTag := recordModel.HashTag
			model, err := loadDataByID(dep.DB, recordModel.HashTag)
			if err != nil {
				recordTaskError(taskName, err, "query_data", map[string]string{"hash_tag": hashTag})
				return err
			}
			canBeCleaned := true
			cleanedKeys := []string{}
			for key, value := range model.Value {
				shouldBeSynced, err := isKeyShouldBeSynced(key, value)
				if err != nil {
					recordTaskError(
						taskName, err, "check_should_be_synced",
						map[string]string{"key": key, "hash_tag": hashTag})
					return err
				}
				if shouldBeSynced {
					excludedHashTags.Add(recordModel.HashTag)
					recordTaskError(
						taskName, err, "should_be_synced",
						map[string]string{"key": key, "hash_tag": hashTag})
					canBeCleaned = false
					break
				}
				cleanedKeys = append(cleanedKeys, key)
			}
			if !canBeCleaned {
				continue
			}
			if err := cleanInactiveKeys(model.HashTag, cleanedKeys...); err != nil {
				recordTaskError(
					taskName, err, "clean_key",
					map[string]string{"keys": strings.Join(cleanedKeys, ","), "hash_tag": hashTag})
				return err
			}
			logger.Info(
				"clean keys success",
				log.String("task", taskName),
				log.String("hash_tag", hashTag),
				log.String("keys", strings.Join(cleanedKeys, ",")),
			)
			deletedModels = append(deletedModels, recordModel)
		}
		if err := deleteAccessedRecordModels(deletedModels); err != nil {
			recordTaskError(taskName, err, "delete_accessed_record", nil)
			return err
		}
		metric.MetricCount(fmt.Sprintf("%s.success.count", taskName), len(deletedModels))
	}
	logger.Info(
		"excluded hash_tag count",
		log.String("task", taskName),
		log.Int("count", excludedHashTags.Len()),
	)
	metric.MetricCount(fmt.Sprintf("%s.exclude_hashtag.count", taskName), excludedHashTags.Len())
	recordTaskSuccess(taskName, time.Since(startTime))
	return nil
}

func isKeyShouldBeSynced(key string, value RedisValue) (bool, error) {
	isKeyInvalid := value.IsZero() || value.IsExpired(time.Now())

	redisClient := base.GetRedisCluster()
	keyType, err := redisClient.Type(contextTODO, key).Result()
	if err != nil {
		return false, err
	}

	logger := base.GetTaskLogger()
	if keyType == redisKeyNotExist && isKeyInvalid {
		return false, nil
	}
	if keyType == redisKeyNotExist || isKeyInvalid {
		logger.Info(
			"should be synced key",
			log.String("key_type", keyType),
			log.String("key", key),
			log.String("db_value", value.String()),
		)
		return true, nil
	}

	valueInRedis, err := getValueByKeyFromRedis(keyType, key)
	if err != nil {
		// If key does not exist, it means key is changed or expired after type check.
		if errors.Is(err, redis.Nil) {
			logger.Info(
				"should be synced key",
				log.String("key", key),
				log.String("redis_value", "nil"),
				log.String("db_value", value.String()),
			)
			return true, nil
		}
		return false, err
	}
	isEqual, err := isValueEqual(keyType, valueInRedis, value)
	if err != nil {
		return false, err
	}
	if !isEqual {
		logger.Info(
			"should be synced key",
			log.String("key", key),
			log.String("key_type", keyType),
			log.String("redis_value", fmt.Sprint(valueInRedis)),
			log.String("db_value", value.String()),
		)
	}
	return !isEqual, nil
}

func isValueEqual(keyType string, valueInRedis []string, value RedisValue) (bool, error) {
	var equal bool
	var err error
	if keyType != value.Type {
		return false, nil
	}
	equalFuncs := map[string]func(s1, s2 []string) bool{
		hashType: isTwoHashEqual,
		listType: isTwoListEqual,
		setType:  isTwoSetEqual,
		zsetType: isTwoZSetEqual,
	}
	valueInDB := value.Value
	switch keyType {
	case stringType:
		equal = valueInRedis[0] == valueInDB
	case hashType, listType, zsetType, setType:
		var items []string
		err = json.Unmarshal([]byte(valueInDB), &items)
		equal = equalFuncs[keyType](valueInRedis, items)
	}
	return equal, err
}

func isTwoListEqual(s1, s2 []string) bool {
	return utility.IsTwoStringSliceEqual(s1, s2)
}

func isTwoHashEqual(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}
	m1 := make(map[string]string)
	m2 := make(map[string]string)
	index := 0
	for index < len(s1) {
		f1 := s1[index]
		v1 := s1[index+1]
		m1[f1] = v1
		f2 := s2[index]
		v2 := s2[index+1]
		m2[f2] = v2
		index += 2
	}
	return utility.IsTwoStringMapEqual(m1, m2)
}

func isTwoSetEqual(s1, s2 []string) bool {
	return utility.IsTwoStringSliceContainsSameElement(s1, s2)
}

func isTwoZSetEqual(s1, s2 []string) bool {
	return isTwoHashEqual(s1, s2)
}

func cleanInactiveKeys(hashTag string, keys ...string) error {
	tag, err := NewHashTag(hashTag, base.GetTaskDependency())
	if err != nil {
		return err
	}
	return tag.CleanKeys(keys...)
}

func deleteAccessedRecordModels(models []*roomAccessedRecordModelV2) error {
	clusterModels, err := getClusterModelsFromAccessedRecordModels(models...)
	if err != nil {
		return err
	}
	for _, clusterModel := range clusterModels {
		hashTags := make([]string, len(clusterModel.models))
		for index, model := range clusterModel.models {
			hashTags[index] = model.HashTag
		}
		_, err := clusterModel.client.Model((*roomAccessedRecordModelV2)(nil)).
			Table(clusterModel.tableName).
			Where("hash_tag in (?)", pg.In(hashTags)).Delete()
		if err != nil {
			return err
		}
	}
	return nil
}
