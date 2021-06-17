package service

import (
	"bytepower_room/base"
	"strings"
	"time"
)

const CleanKeysTaskName = "clean_keys_v2"

// find keys to clean
// select * from table where status != "cleaned" and accessed_at < ?;
// update table set status = "cheaned" where hash_tag = "xxx" and version = "xxx"
func CleanKeys(inactiveDuration time.Duration) {
	count := 100
	startTime := time.Now()
	accessedAt := time.Now().Add(-inactiveDuration)
	dep := base.GetTaskDependency()
	var processCount int
	var err error
	defer func() {
		if err == nil {
			recordTaskSuccess(CleanKeysTaskName, time.Since(startTime))
		}
	}()
	for {
		models, loadErr := loadHashTagKeysModelsByCondition(
			dep.DB, count,
			dbWhereCondition{column: "status", operator: "!=", parameter: HashTagKeysStatusCleaned},
			dbWhereCondition{column: "accessed_at", operator: "<=", parameter: accessedAt},
		)
		if loadErr != nil {
			recordTaskErrorV2(
				dep.Logger, dep.Metric, CleanKeysTaskName,
				loadErr, "load_hash_tag_keys", nil)
			err = loadErr
			return
		}
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
				return
			}
		}
		processCount = processCount + 1
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
