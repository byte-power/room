package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/pflag"
)

var dryRun = pflag.BoolP("dryrun", "d", true, "dry run")

func main() {
	pflag.Parse()
	options := &redis.ClusterOptions{
		Addrs: []string{
			"",
		},
	}
	client := redis.NewClusterClient(options)
	var cursor uint64 = 0
	var stopCursor uint64 = 0
	var count int64 = 100
	var processCount uint64 = 0
	var skipCount uint64 = 0
	startTime := time.Now()
	fmt.Printf("start process at %s, dryrun=%t\n", startTime.String(), *dryRun)
	defer func() {
		fmt.Printf("stop process, count=%d, skip=%d, duration=%s\n", processCount, skipCount, time.Since(startTime).String())
	}()
	for {
		keys, c, err := client.Scan(context.Background(), cursor, "*:_meta", count).Result()
		if err != nil {
			fmt.Printf("scan error %s\n", err.Error())
			return
		}
		for _, key := range keys {
			valid, err := isValidMeta(client, key)
			if err != nil {
				fmt.Printf("process key %s error %s\n", key, err.Error())
				return
			}
			if valid {
				fmt.Printf("prepare to process key %s\n", key)
				if !*dryRun {
					_, err := client.Del(context.Background(), key).Result()
					if err != nil {
						fmt.Printf("process key %s error %s\n", key, err.Error())
						return
					}
					fmt.Printf("process key success %s\n", key)
				}
				processCount += 1
			} else {
				skipCount += 1
				fmt.Printf("skip key %s\n", key)
			}
		}
		fmt.Printf("scan cursor %d\n", c)
		if c == stopCursor {
			break
		}
		cursor = c
	}
}

func isValidMeta(client *redis.ClusterClient, key string) (bool, error) {
	if !strings.HasSuffix(key, ":_meta") {
		return false, nil
	}
	value, err := client.HGet(context.Background(), key, "loaded").Result()
	if err != nil {
		return false, err
	}
	return value == "1", nil
}
