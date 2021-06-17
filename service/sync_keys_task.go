package service

import (
	"bytepower_room/base"
	"errors"
	"strings"
	"time"
)

const SyncKeysTaskName = "sync_keys_v2"

// find keys to sync
// select * from table where status = "syncing";
// update table set status = "synced", syncedAt = time.Now() where hash_tag = "xxx" and version = xx
func SyncKeys(noWrittenDuration time.Duration) {
	count := 1000
	dep := base.GetTaskDependency()
	syncedCount := 1
	startTime := time.Now()
	var err error
	defer func() {
		if err == nil {
			recordTaskSuccess(SyncKeysTaskName, time.Since(startTime))
		}
	}()
	for {
		writtenAt := time.Now().Add(-noWrittenDuration)
		models, loadErr := loadNeedToSyncHashTagKeysModels(dep.DB, writtenAt, count)
		if loadErr != nil {
			recordTaskErrorV2(dep.Logger, dep.Metric, SyncKeysTaskName, loadErr, "load_hash_tag_keys", nil)
			err = loadErr
			return
		}
		if len(models) == 0 {
			break
		}
		for _, model := range models {
			if err = syncRoomData(dep.DB, model, time.Now()); err != nil {
				recordTaskErrorV2(
					dep.Logger, dep.Metric, SyncKeysTaskName,
					err, "sync_room_data",
					map[string]string{"hash_tag": model.HashTag, "keys": strings.Join(model.Keys, " ")},
				)
				if isRetryErrorForSync(err) {
					continue
				}
				return
			}
			syncedCount += 1
		}
	}
}

func syncRoomData(db *base.DBCluster, model *roomHashTagKeys, t time.Time) error {
	if err := syncHashTagKeys(db, model.HashTag, model.Keys); err != nil {
		return err
	}
	if err := model.SetStatusAsSynced(db, t); err != nil {
		return err
	}
	return nil
}

func syncHashTagKeys(db *base.DBCluster, hashTag string, keys []string) error {
	retryTimes := 3
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
	err := upsertRoomDataValue(db, hashTag, value, retryTimes)
	if err != nil {
		return err
	}
	return nil
}

func isRetryErrorForSync(err error) bool {
	return errors.Is(err, errNoRowsUpdated)
}
