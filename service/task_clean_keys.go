package service

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-pg/pg/v10"
	"go.uber.org/ratelimit"
)

const CleanKeysTaskName = "clean_keys_v2"

// find keys to clean
// select * from table where status != "cleaned" and accessed_at < ?;
// update table set status = "cheaned" where hash_tag = "xxx" and version = "xxx"
func CleanKeysTaskV2(inactiveDuration time.Duration, rateLimitPerSecond int) {
	startTime := time.Now()
	logTaskStart(
		CleanKeysTaskName,
		startTime,
		log.String("inactive_duration", inactiveDuration.String()),
		log.Int("limit", rateLimitPerSecond),
	)

	count := 100
	accessedAt := startTime.Add(-inactiveDuration)
	dep := base.GetTaskDependency()
	var err error
	defer func() {
		if err == nil {
			recordTaskSuccess(CleanKeysTaskName, time.Since(startTime))
		}
	}()
	excludedHashTags := make([]string, 0)
	ratelimitBucket := ratelimit.New(rateLimitPerSecond)
	tableIndex := 0
	for {
		conditions := []dbWhereCondition{
			{column: "status", operator: "=?", parameter: HashTagKeysStatusSynced},
			{column: "accessed_at", operator: "<=?", parameter: accessedAt},
		}
		if len(excludedHashTags) > 0 {
			conditions = append(conditions, dbWhereCondition{column: "hash_tag", operator: "not in (?)", parameter: pg.In(excludedHashTags)})
		}
		index, models, loadErr := loadHashTagKeysModelsByCondition(dep.DB, count, tableIndex, conditions...)
		if loadErr != nil {
			err = loadErr
			recordTaskErrorV2(
				dep.Logger, dep.Metric, CleanKeysTaskName,
				err, "load_hash_tag_keys", nil)
			return
		}
		if len(models) == 0 {
			break
		}
		if tableIndex < index {
			excludedHashTags = make([]string, 0)
			tableIndex = index
		}
		processHashTagCount := 0
		processKeyCount := 0
		for _, model := range models {
			ratelimitBucket.Take()
			keyCount, cleanKeysErr := cleanHashTagKeys(dep, model)
			err = cleanKeysErr
			if err != nil {
				recordTaskErrorV2(
					dep.Logger, dep.Metric,
					CleanKeysTaskName, err,
					"clean_keys",
					map[string]string{
						"hash_tag": model.HashTag,
						"keys":     strings.Join(model.Keys, " "),
					})
				if errors.Is(err, ErrAccessAfterRecord) || errors.Is(err, errLoadKeysLockFailed) || isRetryErrorForUpdateInTx(err) {
					recordTaskErrorV2(
						dep.Logger, dep.Metric,
						CleanKeysTaskName, err,
						"clean_keys.conflict",
						map[string]string{
							"hash_tag": model.HashTag,
							"keys":     strings.Join(model.Keys, " "),
						},
					)
					excludedHashTags = append(excludedHashTags, model.HashTag)
					continue
				}
				return
			}
			processHashTagCount = processHashTagCount + 1
			processKeyCount = processKeyCount + int(keyCount)
		}
		conditionStrs := make([]string, 0, len(conditions))
		for _, cond := range conditions {
			conditionStrs = append(conditionStrs, cond.string())
		}
		dep.Logger.Info(
			"clean_keys",
			log.String("task", CleanKeysTaskName),
			log.Int("hash_tag_count", processHashTagCount),
			log.Int("key_count", processKeyCount),
			log.Int("table_index", tableIndex),
			log.String("condition", strings.Join(conditionStrs, " and ")),
		)
		dep.Metric.MetricCount(fmt.Sprintf("%s.success.clean_hashtag", CleanKeysTaskName), processHashTagCount)
		dep.Metric.MetricCount(fmt.Sprintf("%s.success.clean_key", CleanKeysTaskName), processKeyCount)
	}
}

func cleanHashTagKeys(dep base.Dependency, model *roomHashTagKeys) (int64, error) {
	tag, err := NewHashTag(model.HashTag, dep)
	if err != nil {
		return 0, err
	}
	n, err := tag.CleanKeysV2(model.AccessedAt, model.Keys...)
	if err != nil {
		return 0, err
	}
	err = model.SetStatusAsCleaned(dep.DB, time.Now())
	if err != nil {
		dep.Logger.Error(
			"clean_keys.set_hash_tag_keys_model_status",
			log.String("hash_tag", model.HashTag),
			log.String("keys", strings.Join(model.Keys, " ")),
			log.Error(err))
		return 0, err
	}
	return n, nil
}
