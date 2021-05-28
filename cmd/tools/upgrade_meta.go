package main

import (
	"bytepower_room/base"
	"bytepower_room/service"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/go-pg/pg/v10"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/pflag"
)

var configPath = pflag.StringP("config", "c", "config.yaml", "config file path")
var dryRun = pflag.BoolP("dryrun", "d", true, "dry run")

func main() {
	startTime := time.Now()
	rand.Seed(startTime.UnixNano())

	pflag.Parse()
	if configPath == nil {
		fmt.Printf("config path parameter should not be empty")
		return
	}
	if dryRun == nil {
		fmt.Printf("dryrun parameter should not be empty")
		return
	}
	fmt.Printf("start to run at %s, configPath=%s, dryRun=%t\n", startTime, *configPath, *dryRun)
	if err := base.InitSyncService(*configPath); err != nil {
		fmt.Printf("init service error:%s\n", err.Error())
		return
	}
	client := base.GetRedisCluster()
	var cursor uint64 = 0
	var step int64 = 100
	totalProcessedCount := 0
	for {
		keys, c, err := client.Scan(context.Background(), cursor, "", step).Result()
		if err != nil {
			fmt.Printf("scan error:%s\n", err.Error())
			return
		}
		cursor = c
		count, err := processKeys(*dryRun, keys...)
		if err != nil {
			fmt.Printf("process error:%s\n", err.Error())
			return
		}
		totalProcessedCount += count
		if cursor == 0 {
			break
		}
	}
	fmt.Printf("finish process, count:%d, duraiton=%s\n", totalProcessedCount, time.Since(startTime).String())
}

func processKeys(dryRun bool, keys ...string) (int, error) {
	processedCount := 0
	for _, key := range keys {
		if isMetaKey(key) {
			fmt.Printf("skip key:%s, it is a meta key\n", key)
			continue
		}
		if isNewMetaKey(key) {
			fmt.Printf("skip key:%s, it is a new meta key\n", key)
			continue
		}
		isMetaValid, err := hasValidMetaKey(key)
		if err != nil {
			return 0, fmt.Errorf("hasValidMetaKey: %s, %w", key, err)
		}
		if !isMetaValid {
			fmt.Printf("skip key:%s, do not have meta key\n", key)
			continue
		}
		//process key
		if !dryRun {
			if err := setNewMeta(key); err != nil {
				return 0, fmt.Errorf("setNewMeta: %s, %w", key, err)
			}
			if err := insertAccessedRecordByKey(key); err != nil {
				return 0, fmt.Errorf("insertAccessRecordByKey: %s, %w", key, err)
			}
			if err := insertWrittenRecordByKey(key); err != nil {
				return 0, fmt.Errorf("insertWrittenRecordByKey: %s, %w", key, err)
			}
		}
		fmt.Printf("process key:%s\n", key)
		processedCount += 1
	}
	return processedCount, nil
}

func getMetaKey(key string) string {
	return key + ":_meta"
}

func isMetaKey(key string) bool {
	return strings.HasSuffix(key, ":_meta")
}

func isNewMetaKey(key string) bool {
	return strings.HasSuffix(key, ":_m")
}

func hasValidMetaKey(key string) (bool, error) {
	metaKey := getMetaKey(key)
	client := base.GetRedisCluster()
	status, err := client.HGet(context.Background(), metaKey, "loaded").Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, fmt.Errorf("hasValidMetaKey %s, %w", key, err)
	}
	return status == "1", nil
}

func setNewMeta(key string) error {
	hashTag := extractHashTagFromKey(key)
	if hashTag == "" {
		fmt.Printf("hashTag is empty:%s\n", hashTag)
		return nil
	}
	newMetaKey := getHashTagMetaKey(hashTag)
	client := base.GetRedisCluster()
	_, err := client.HSet(
		context.Background(), newMetaKey, service.HashTagMetaInfoStatusFieldName,
		service.HashTagStatusLoaded).Result()
	return err
}

func extractHashTagFromKey(key string) string {
	leftBraceIndex := strings.Index(key, "{")
	if leftBraceIndex == -1 {
		return ""
	}
	rightBraceIndex := strings.Index(key[leftBraceIndex:], "}")
	if rightBraceIndex > 1 {
		return key[leftBraceIndex+1 : leftBraceIndex+rightBraceIndex]
	}
	return ""
}

func getHashTagMetaKey(hashTag string) string {
	return fmt.Sprintf("{%s}:_m", hashTag)
}

type roomAccessedRecordModelV2 struct {
	tableName struct{} `pg:"_"`

	HashTag    string    `pg:"hash_tag,pk"`
	AccessedAt time.Time `pg:"accessed_at"`
	CreatedAt  time.Time `pg:"created_at"`
}

func (model *roomAccessedRecordModelV2) ShardingKey() string {
	return model.HashTag
}

func (model *roomAccessedRecordModelV2) GetTablePrefix() string {
	return "room_accessed_record_v2"
}

// 5 days in second
const MAX_SECONDS_DRIFT = 5 * 24 * 3600

var baseTime = time.Now()

func insertAccessedRecordByKey(key string) error {
	hashTag := extractHashTagFromKey(key)
	driftSeconds := rand.Int63n(MAX_SECONDS_DRIFT)
	accessedTime := baseTime.Add(time.Duration(-driftSeconds) * time.Second)
	model := &roomAccessedRecordModelV2{HashTag: hashTag, AccessedAt: accessedTime, CreatedAt: accessedTime}
	db := base.GetAccessedRecordDBCluster()
	query, err := db.Model(model)
	if err != nil {
		return err
	}
	_, err = query.Insert()
	if err != nil {
		var pgErr pg.Error
		if errors.As(err, &pgErr) && pgErr.IntegrityViolation() {
			return nil
		}
		return err
	}
	return nil
}

type roomWrittenRecordModel struct {
	tableName struct{} `pg:"_"`

	Key       string    `pg:"key,pk"`
	WrittenAt time.Time `pg:"written_at"`
	CreatedAt time.Time `pg:"created_at"`
}

func (model *roomWrittenRecordModel) ShardingKey() string {
	return model.Key
}

func (model *roomWrittenRecordModel) GetTablePrefix() string {
	return "room_written_record"
}

func insertWrittenRecordByKey(key string) error {
	driftSeconds := rand.Int63n(MAX_SECONDS_DRIFT)
	writtenTime := baseTime.Add(time.Duration(-driftSeconds) * time.Second)
	model := &roomWrittenRecordModel{Key: key, WrittenAt: writtenTime, CreatedAt: writtenTime}
	db := base.GetWrittenRecordDBCluster()
	query, err := db.Model(model)
	if err != nil {
		return err
	}
	_, err = query.Insert()
	if err != nil {
		var pgErr pg.Error
		if errors.As(err, &pgErr) && pgErr.IntegrityViolation() {
			return nil
		}
		return err
	}
	return nil
}
