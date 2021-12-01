package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/pflag"
	"go.uber.org/ratelimit"
)

const (
	scanCount  = 100
	scriptName = "remove_user_segment_key_from_redis"
)

var (
	redisClusterAddr = pflag.StringP("redis_cluster_addr", "r", "", "redis cluster address, in host:port format")
	roomAddr         = pflag.StringP("room_addr", "m", "", "room server address, in host:port format")
	serviceLimit     = pflag.IntP("limit", "l", 100, "redis server and room server access rate per second, default is 100")
	dryRun           = pflag.BoolP("dryrun", "d", true, "is dry run or not")
	roomLimit        ratelimit.Limiter
)

func parseAndCheckCommandOptions() error {
	pflag.Parse()
	if redisClusterAddr == nil || *redisClusterAddr == "" {
		return errors.New("parameter redis_cluster_addr should be set")
	}
	if roomAddr == nil || *roomAddr == "" {
		return errors.New("parameter room_addr should be set")
	}
	if dryRun == nil {
		return errors.New("parameter dryrun should be set")
	}
	if serviceLimit == nil || *serviceLimit == 0 {
		return errors.New("parameter limit should be set")
	}
	return nil
}

func main() {
	logger := log.New(os.Stdout, fmt.Sprintf("%s ", scriptName), log.LstdFlags)
	if err := parseAndCheckCommandOptions(); err != nil {
		logger.Fatalf("command options error %s\n", err)
	}
	// roomLimit is shared by all goroutines
	roomLimit = ratelimit.New(*serviceLimit)
	logger.Printf(
		"start task, redis_cluster=%s, room=%s, dryrun=%t, limit=%d\n",
		*redisClusterAddr, *roomAddr, *dryRun, *serviceLimit)
	opts := &redis.ClusterOptions{
		Addrs: []string{*redisClusterAddr},
	}
	startTime := time.Now()
	clusterClient := redis.NewClusterClient(opts)
	err := clusterClient.ForEachMaster(context.TODO(), removeUserSegmentKeyFromRedis)
	if err != nil {
		logger.Fatalf("process redis error %s\n", err)
	}
	logger.Printf("finish task, duration=%s\n", time.Since(startTime).String())
}

func removeUserSegmentKeyFromRedis(ctx context.Context, client *redis.Client) error {
	serverAddr := client.Options().Addr
	logPrefix := getServerLogPrefix(serverAddr)
	logFileName := getLogFileName(serverAddr)
	f, err := os.Create(logFileName)
	if err != nil {
		return fmt.Errorf("open log file %s error %w", logFileName, err)
	}
	defer f.Close()
	logger := log.New(f, logPrefix, log.LstdFlags)
	roomClient := redis.NewClient(&redis.Options{Addr: *roomAddr})
	startTime := time.Now()
	limit := ratelimit.New(*serviceLimit)
	var cursor uint64 = 0
	totalKeyCount := 0
	skipKeyCount := 0
	processKeyCount := 0
	logger.Printf("start to process, room server %s, dryrun %t limit %d\n", *roomAddr, *dryRun, *serviceLimit)
	for {
		limit.Take()
		logger.Printf("scan cursor %d\n", cursor)
		keys, c, err := client.Scan(context.TODO(), cursor, "", scanCount).Result()
		if err != nil {
			logger.Printf("scan error: %s\n", err)
			return err
		}
		for _, key := range keys {
			if isUserSegmentKey(key) {
				processKeyCount += 1
				logger.Printf("prepare to process key: %s\n", key)
				if *dryRun {
					continue
				}
				roomLimit.Take()
				_, err := roomClient.Del(ctx, key).Result()
				if err != nil {
					logger.Printf("process error %s\n", err)
					return err
				}
				logger.Printf("process key: %s\n", key)
			} else {
				logger.Printf("skip key: %s\n", key)
				skipKeyCount += 1
			}
		}
		totalKeyCount += len(keys)
		if c == 0 {
			break
		}
		cursor = c
	}
	logger.Printf(
		"scan finish, %d keys, skip %d keys, process %d keys, scan duration %s\n",
		totalKeyCount, skipKeyCount, processKeyCount, time.Since(startTime).String())
	return nil
}

func getServerLogPrefix(addr string) string {
	return fmt.Sprintf("%s [server %s]", scriptName, addr)
}

func getLogFileName(addr string) string {
	return fmt.Sprintf("%s_%s.log", scriptName, addr)
}

func isUserSegmentKey(key string) bool {
	parts := strings.Split(key, ":")
	if len(parts) == 4 {
		appID := parts[0]
		moduleName := parts[1]
		uidWithBraces := parts[2]
		suffix := parts[3]
		return len(appID) == 7 &&
			moduleName == "user_segment" &&
			strings.HasPrefix(uidWithBraces, "{UU") &&
			strings.HasSuffix(uidWithBraces, "}") &&
			(suffix == "data" || suffix == "id" || suffix == "changes")
	}
	if len(parts) == 3 {
		appIDWithBraces := parts[0]
		moduleName := parts[1]
		suffix := parts[2]
		return len(appIDWithBraces) == 9 &&
			strings.HasPrefix(appIDWithBraces, "{") &&
			strings.HasSuffix(appIDWithBraces, "}") &&
			moduleName == "user_segment" &&
			suffix == "segment"
	}
	return false
}
