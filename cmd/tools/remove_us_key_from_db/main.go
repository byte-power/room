package main

import (
	"bytepower_room/base"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/go-pg/pg/v10"
	"github.com/spf13/pflag"
	"go.uber.org/ratelimit"
)

type RedisValue struct {
	Type     string `json:"type"`
	Value    string `json:"value"`
	ExpireTs int64  `json:"expire_ts"`
}

func (v RedisValue) String() string {
	return fmt.Sprintf(
		"[RedisValue:type=%s,value=%s,expire_ts=%d]",
		v.Type, v.Value, v.ExpireTs)
}

type roomDataModelV2 struct {
	tableName struct{} `pg:"_"`

	HashTag   string                `pg:"hash_tag,pk"`
	Value     map[string]RedisValue `pg:"value"`
	DeletedAt time.Time             `pg:"deleted_at"`
	CreatedAt time.Time             `pg:"created_at"`
	UpdatedAt time.Time             `pg:"updated_at"`
	Version   int                   `pg:"version"`
}

func (model *roomDataModelV2) ShardingKey() string {
	return model.HashTag
}

func (model *roomDataModelV2) GetTablePrefix() string {
	return "room_data_v2"
}

var (
	configPath = pflag.StringP("config", "c", "config.yaml", "config file path")
	dryRun     = pflag.BoolP("dryrun", "d", true, "is dry run or not")
	rate       = pflag.IntP("rate", "r", 100, "redis server and room access rate per second, default is 100")
)

var errNoRowsUpdated = errors.New("no row is updated")

const scriptName = "remove_user_segment_key_from_database"

func parseAndCheckCommandOptions() error {
	pflag.Parse()
	if configPath == nil || *configPath == "" {
		return errors.New("parameter config should be set")
	}
	if dryRun == nil {
		return errors.New("parameter dryrun should be set")
	}
	if rate == nil || *rate == 0 {
		return errors.New("parameter rate should be set")
	}
	return nil
}

func main() {
	logger := log.New(os.Stdout, fmt.Sprintf("%s ", scriptName), log.LstdFlags)
	if err := parseAndCheckCommandOptions(); err != nil {
		logger.Fatalf("command options error %s\n", err)
	}
	if err := base.InitRoomServer(*configPath); err != nil {
		logger.Fatalf("init service error %s\n", err)
	}
	limit := ratelimit.New(*rate)
	db := base.GetServerDependency().DB
	roomDataTableShardingCount := db.GetShardingCount()
	roomDataTablePrefix := (&roomDataModelV2{}).GetTablePrefix()
	totalProcessCount := 0
	totalSkipCount := 0
	startTime := time.Now()
	logger.Printf(
		"start to process, config=%s, dryrun=%t, rate=%d, table_sharding_count=%d, table_prefix=%s\n",
		*configPath, *dryRun, *rate, roomDataTableShardingCount, roomDataTablePrefix)
	for index := 0; index < roomDataTableShardingCount; index++ {
		processCount, skipCount, err := processTableByIndex(db, logger, limit, index)
		if err != nil {
			logger.Fatalf("process error table index %d, %s\n", index, err)
		}
		logger.Printf("process success, process count %d, skip count %d, table_index %d\n", processCount, skipCount, index)
		totalProcessCount += processCount
		totalSkipCount += skipCount
	}
	logger.Printf(
		"process success, total process count %d, total skip count %d, duration %s\n",
		totalProcessCount, totalSkipCount, time.Since(startTime))
}

func processTableByIndex(db *base.DBCluster, logger *log.Logger, limit ratelimit.Limiter, index int) (int, int, error) {
	startID := ""
	loadCount := 100
	processCount := 0
	skipCount := 0
	for {
		limit.Take()
		roomDataModels, id, err := loadRoomDataModels(db, index, startID, loadCount)
		if err != nil {
			return 0, 0, fmt.Errorf("load_model error %w", err)
		}
		for _, roomDataModel := range roomDataModels {
			processed, err := processModel(db, limit, roomDataModel)
			if err != nil {
				if errors.Is(err, errNoRowsUpdated) {
					logger.Printf(
						"process error no row updated, index %d, hashTag %s\n",
						index, roomDataModel.HashTag)
					continue
				}
				return 0, 0, err
			}
			if processed {
				logger.Printf("process hash_tag %s, index %d\n", roomDataModel.HashTag, index)
				processCount += 1
			} else {
				logger.Printf("skip hash_tag %s, index %d\n", roomDataModel.HashTag, index)
				skipCount += 1
			}
		}
		startID = id
		if len(roomDataModels) < loadCount {
			break
		}
	}
	return processCount, skipCount, nil
}

func loadRoomDataModels(db *base.DBCluster, tableIndex int, startID string, count int) ([]*roomDataModelV2, string, error) {
	var models []*roomDataModelV2
	tablePrefix := (&roomDataModelV2{}).GetTablePrefix()
	query, err := db.Models(&models, tablePrefix, tableIndex)
	if err != nil {
		return nil, "", err
	}
	if startID != "" {
		query.Where("hash_tag>?", startID)
	}
	err = query.OrderExpr("hash_tag ASC").Limit(count).Select()
	if err != nil {
		if errors.Is(err, pg.ErrNoRows) {
			return nil, "", nil
		}
		return nil, "", err
	}
	if len(models) == 0 {
		return nil, "", nil
	}
	return models, models[len(models)-1].HashTag, nil
}

func processModel(db *base.DBCluster, limit ratelimit.Limiter, model *roomDataModelV2) (bool, error) {
	key := findUserSegmentKey(model.Value)
	if key == "" {
		return false, nil
	}
	if *dryRun {
		return true, nil
	}
	err := removeUserSegmentKeyFromDatabase(db, limit, model, key)
	return true, err
}

func findUserSegmentKey(values map[string]RedisValue) string {
	for key := range values {
		if isUserSegmentKey(key) {
			return key
		}
	}
	return ""
}

func isUserSegmentKey(key string) bool {
	parts := strings.Split(key, ":")
	if len(parts) != 4 {
		return false
	}
	appID := parts[0]
	moduleName := parts[1]
	uidWithBraces := parts[2]
	suffix := parts[3]
	return len(appID) == 7 &&
		moduleName == "user_segment" &&
		strings.HasPrefix(uidWithBraces, "{UU") &&
		strings.HasSuffix(uidWithBraces, "}") &&
		suffix == "data"
}

func removeUserSegmentKeyFromDatabase(db *base.DBCluster, limit ratelimit.Limiter, model *roomDataModelV2, key string) error {
	var err error
	retryTimes := 3
	for i := 0; i < retryTimes; i++ {
		limit.Take()
		err = _removeUserSegmentKeyFromDatabase(db, model, key)
		if err == nil {
			return nil
		}
		if errors.Is(err, errNoRowsUpdated) {
			continue
		}
		return err
	}
	return err
}

func _removeUserSegmentKeyFromDatabase(db *base.DBCluster, model *roomDataModelV2, key string) error {
	query, err := db.Model(model)
	if err != nil {
		return err
	}
	currentTime := time.Now()
	result, err := query.Set("value=value-?", key).
		Set("updated_at=?", currentTime).
		Set("version=?", model.Version+1).
		WherePK().
		Where("version=?", model.Version).
		Update()
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return errNoRowsUpdated
	}
	return nil
}
