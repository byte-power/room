package service

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-pg/pg/v10"
)

const CleanKeysTaskName = "clean_keys_v2"

// find keys to clean
// select * from table where status != "cleaned" and accessed_at < ?;
// update table set status = "cheaned" where hash_tag = "xxx" and version = "xxx"
func CleanKeysTaskV2(inactiveDuration time.Duration) {
	startTime := time.Now()
	logTaskStart(CleanKeysTaskName, startTime, log.String("inactive_duration", inactiveDuration.String()))

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
	for {
		conditions := []dbWhereCondition{
			{column: "status", operator: "=?", parameter: HashTagKeysStatusSynced},
			{column: "accessed_at", operator: "<=?", parameter: accessedAt},
		}
		if len(excludedHashTags) > 0 {
			conditions = append(conditions, dbWhereCondition{column: "hash_tag", operator: "not in (?)", parameter: pg.In(excludedHashTags)})
		}
		models, loadErr := loadHashTagKeysModelsByCondition(dep.DB, count, conditions...)
		if loadErr != nil {
			recordTaskErrorV2(
				dep.Logger, dep.Metric, CleanKeysTaskName,
				loadErr, "load_hash_tag_keys", nil)
			err = loadErr
			return
		}
		if len(models) == 0 {
			break
		}
		processCount := 0
		for _, model := range models {
			if err = cleanHashTagKeys(dep, model); err != nil {
				recordTaskErrorV2(
					dep.Logger, dep.Metric,
					CleanKeysTaskName, err,
					"clean_keys",
					map[string]string{
						"hash_tag": model.HashTag,
						"keys":     strings.Join(model.Keys, " "),
					})
				if errors.Is(err, ErrAccessAfterRecord) {
					dep.Metric.MetricIncrease(fmt.Sprintf("%s.error.excluded_hash_tag", CleanKeysTaskName))
					excludedHashTags = append(excludedHashTags, model.HashTag)
					continue
				}
				return
			}
			processCount = processCount + 1
		}
		dep.Logger.Info("clean_keys", log.String("task", CleanKeysTaskName), log.Int("count", processCount))
		dep.Metric.MetricCount(fmt.Sprintf("%s.clean_hashtag.count", CleanKeysTaskName), processCount)
	}
}

func cleanHashTagKeys(dep base.Dependency, model *roomHashTagKeys) error {
	tag, err := NewHashTag(model.HashTag, dep)
	if err != nil {
		return err
	}
	err = tag.CleanKeysV2(model.AccessedAt, model.Keys...)
	if err != nil {
		return err
	}
	//TODO: return error may mean keys are accessed at the same time.
	err = model.SetStatusAsCleaned(dep.DB, time.Now())
	if err != nil {
		return err
	}
	return nil
}
