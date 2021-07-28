package service

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"fmt"
	"strings"
	"time"
)

const SyncKeysTaskName = "sync_keys_v2"

// find keys to sync
// select * from table where status = "syncing";
// update table set status = "synced", syncedAt = time.Now() where hash_tag = "xxx" and version = xx
func SyncKeysTaskV2(upsertTryTimes int, noWrittenDuration time.Duration) {
	startTime := time.Now()
	logTaskStart(
		SyncKeysTaskName, startTime,
		log.Int("upsert_try_times", upsertTryTimes),
		log.String("no_written_duration", noWrittenDuration.String()),
	)

	count := 1000
	dep := base.GetTaskDependency()
	var err error
	defer func() {
		if err == nil {
			recordTaskSuccess(SyncKeysTaskName, time.Since(startTime))
		}
	}()
	writtenAt := startTime.Add(-noWrittenDuration)
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
		for {
			models, loadErr := loadHashTagKeysModelsByCondition(dep.DB, count, condition...)
			// dbWhereCondition{column: "status", operator: "=?", parameter: HashTagKeysStatusNeedSynced},
			// dbWhereCondition{column: "written_at", operator: "<=?", parameter: writtenAt})
			if loadErr != nil {
				recordTaskErrorV2(dep.Logger, dep.Metric, SyncKeysTaskName, loadErr, "load_hash_tag_keys", nil)
				err = loadErr
				return
			}
			if len(models) == 0 {
				break
			}
			processCount := 0
			for _, model := range models {
				if err = syncRoomData(dep.DB, model, time.Now(), upsertTryTimes); err != nil {
					recordTaskErrorV2(
						dep.Logger, dep.Metric, SyncKeysTaskName,
						err, "sync_room_data",
						map[string]string{"hash_tag": model.HashTag, "keys": strings.Join(model.Keys, " ")},
					)
					if isRetryErrorForUpdateInTx(err) {
						continue
					}
					return
				}
				processCount += 1
			}
			metricName := fmt.Sprintf("%s.success.sync_hash_tag", SyncKeysTaskName)
			dep.Metric.MetricCount(metricName, processCount)
			dep.Logger.Info(metricName, log.Int("count", processCount))
		}
	}
}

func syncRoomData(db *base.DBCluster, model *roomHashTagKeys, t time.Time, tryTimes int) error {
	if err := syncHashTagKeys(db, model.HashTag, model.Keys, tryTimes); err != nil {
		return err
	}
	if err := model.SetStatusAsSynced(db, t); err != nil {
		return err
	}
	return nil
}

func syncHashTagKeys(db *base.DBCluster, hashTag string, keys []string, tryTimes int) error {
	value := make(map[string]RedisValue)
	for _, key := range keys {
		v, err := getValueFromRedis(key)
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
