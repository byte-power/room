package main

import (
	"bytepower_room/base"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/pflag"
)

var configPath = pflag.StringP("config", "c", "config.yaml", "config file path")

func main() {
	pflag.Parse()
	if err := base.InitServices(*configPath); err != nil {
		panic(err)
	}
	client := base.GetRedisCluster()
	var cursor uint64 = 0
	var step int64 = 100
	totalProcessedCount := 0
	startTime := time.Now()
	for {
		keys, c, err := client.Scan(context.Background(), cursor, "", step).Result()
		if err != nil {
			fmt.Printf("scan error:%s\n", err.Error())
		}
		if c == 0 {
			break
		}
		cursor = c
		count, err := processKeys(keys...)
		if err != nil {
			fmt.Printf("process error:%s\n", err.Error())
			return
		}
		totalProcessedCount += count
	}
	fmt.Printf("finish process, count:%d, duraiton=%s\n", totalProcessedCount, time.Since(startTime).String())
}

func processKeys(keys ...string) (int, error) {
	processedCount := 0
	for _, key := range keys {
		if isMetaKey(key) {
			fmt.Printf("skip meta key:%s\n", key)
			continue
		}
		isMetaValid, err := hasValidMetaKey(key)
		if err != nil {
			return 0, err
		}
		if !isMetaValid {
			continue
		}
		//process key
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

func hasValidMetaKey(key string) (bool, error) {
	metaKey := getMetaKey(key)
	client := base.GetRedisCluster()
	status, err := client.HGet(context.Background(), metaKey, "loaded").Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return false, nil
		}
		return false, err
	}
	return status == "1", nil
}
