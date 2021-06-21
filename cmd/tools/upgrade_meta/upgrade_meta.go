package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/spf13/pflag"
)

var dryRun = pflag.BoolP("dryrun", "d", true, "dry run")
var serverAddrs = pflag.StringArrayP("redis", "s", []string{}, "redis server addreses")

type Result struct {
	server       string
	processCount int64
	skipCount    int64
	err          error
}

func (result Result) String() string {
	if result.err != nil {
		return fmt.Sprintf("server %s: error:%s process %d keys, skip %d keys\n", result.server, result.err.Error(), result.processCount, result.skipCount)
	} else {
		return fmt.Sprintf("server %s: process %d keys, skip %d keys\n", result.server, result.processCount, result.skipCount)
	}
}

func parseAndCheckCommandOptions() error {
	pflag.Parse()
	if serverAddrs == nil || len(*serverAddrs) == 0 {
		return errors.New("redis addrs is invalid")
	}
	if dryRun == nil {
		return errors.New("dryrun is not set")
	}
	return nil
}

func main() {
	if err := parseAndCheckCommandOptions(); err != nil {
		panic(err)
	}
	startTime := time.Now()
	logger := log.New(os.Stdout, "", log.LstdFlags)
	logger.Printf(
		"start process at %s, dryrun=%t, redis=%s\n",
		startTime.String(), *dryRun, strings.Join(*serverAddrs, " "))

	wg := &sync.WaitGroup{}
	ch := make(chan Result, len(*serverAddrs))
	for _, server := range *serverAddrs {
		wg.Add(1)
		go scanServer(server, *dryRun, ch, logger, wg)
	}

	wg.Wait()
	close(ch)
	var totalProcessCount int64 = 0
	var totalSkipCount int64 = 0
	for result := range ch {
		logger.Println(result.String())
		totalProcessCount += result.processCount
		totalSkipCount += result.skipCount
	}
	logger.Printf("total process %d keys, skip %d keys, duration=%s\n", totalProcessCount, totalSkipCount, time.Since(startTime).String())
}

func scanServer(server string, dryRun bool, ch chan<- Result, logger *log.Logger, wg *sync.WaitGroup) {
	options := &redis.Options{
		Addr: server,
	}
	client := redis.NewClient(options)
	keyPattern := "}:_m"
	var count int64 = 100
	result := Result{server: server}
	var cursor uint64 = 0
	var stopCursor uint64 = 0
	logger.Printf("start to scan redis %s\n", server)
loop:
	for {
		keys, c, err := client.Scan(context.Background(), cursor, keyPattern, count).Result()
		if err != nil {
			result.err = err
			ch <- result
			logger.Printf("scan error %s\n", err.Error())
			break loop
		}
		for _, key := range keys {
			if isMetaKey(key) {
				logger.Printf("prepare to process key %s\n", key)
				if !dryRun {
					fieldCount, err := processKey(client, key)
					if err != nil {
						result.err = err
						ch <- result
						logger.Printf("process key %s new_key %s error %s\n", key, newKey, err.Error())
						break loop
					}
				}
				// }
				// 	newKey := getNewUserRatingKey(key)
				// 	if !dryRun {
				// 		err := processKey(key, newKey)
				// 		logger.Printf("process key success %s %s\n", key, newKey)
				// 	}
				// 	result.processCount += 1
			} else {
				result.skipCount += 1
				logger.Printf("skip key %s\n", key)
			}
		}
		logger.Printf("scan cursor %d\n", c)
		if c == stopCursor {
			ch <- result
			break loop
		}
		cursor = c
		time.Sleep(100 * time.Millisecond)
	}
	wg.Done()
}

//key format: {xxx}:_m
func isMetaKey(key string) bool {
	parts := strings.Split(key, ":")
	if len(parts) != 2 {
		return false
	}
	hashTagWithBrace := parts[0]
	suffix := parts[1]
	if !strings.HasPrefix(hashTagWithBrace, "{") || !strings.HasSuffix(hashTagWithBrace, "}") {
		return false
	}
	return suffix == "_m"
}

func processKey(client *redis.Client, key string, ts int) (int, error) {
	scriptSrc := `
		if redis.call("hlen", KEYS[1]) ~= 1 {
			return 0
		}
		return redis.call("hset", KEYS[1], "at", ARGV[1], "wt", ARGV[1], "v", 0)
	`
	script := redis.NewScript(scriptSrc)
	result, err := script.Run(context.TODO(), client, []string{key}, ts).Result()
	if err != nil {
		return 0, err
	}
	return result.(int), nil
}
