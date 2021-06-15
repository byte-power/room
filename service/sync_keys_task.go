package service

import (
	"bytepower_room/base"
	"errors"
	"strings"
	"time"
)

const SyncKeysTaskName = "sync_keys"

// find keys to sync
// select * from table where status = "syncing";
// update table set status = "synced", syncedAt = time.Now() where hash_tag = "xxx" and version = xx
func SyncKeys() {
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
		models, loadErr := loadNeedToSyncHashTagKeysModels(dep.DB, count)
		if loadErr != nil {
			recordTaskError2(dep.Logger, dep.Metric, SyncKeysTaskName, loadErr, "load_hash_tag_keys", nil)
			err = loadErr
			return
		}
		if len(models) == 0 {
			break
		}
		for _, model := range models {
			if err = syncRoomData(dep.DB, model.HashTag, model.Keys, time.Now()); err != nil {
				recordTaskError2(
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

func syncRoomData(db *base.DBCluster, hashTag string, keys []string, t time.Time) error {
	if err := syncHashTagKeys(db, hashTag, keys); err != nil {
		return err
	}
	model := &roomHashTagKeys{HashTag: hashTag}
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
