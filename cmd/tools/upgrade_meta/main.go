package main

import (
	"bytepower_room/base"
	"bytepower_room/utility"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-pg/pg/v10"
	"github.com/go-redis/redis/v8"
	"github.com/spf13/pflag"
)

var dryRun = pflag.BoolP("dryrun", "d", true, "dry run")
var configPath = pflag.StringP("config", "c", "", "config file path")
var serverAddrs = pflag.StringArrayP("redis", "s", []string{}, "redis server addreses")

type Result struct {
	server       string
	processCount int64
	skipCount    int64
	err          error
}

func (result Result) String() string {
	if result.err != nil {
		return fmt.Sprintf(
			"server:%s error:%s process_keys:%d skip_keys:%d\n",
			result.server, result.err.Error(),
			result.processCount, result.skipCount)
	} else {
		return fmt.Sprintf(
			"server:%s process_keys:%d skip_keys:%d\n",
			result.server, result.processCount, result.skipCount)
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
	if configPath == nil || *configPath == "" {
		return errors.New("config is not set")
	}
	return nil
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

func main() {
	if err := parseAndCheckCommandOptions(); err != nil {
		panic(err)
	}
	if err := base.InitSyncService(*configPath); err != nil {
		panic(err)
	}
	startTime := time.Now()
	logger := log.New(os.Stdout, "", log.LstdFlags)
	logger.Printf(
		"start process at %s, dryrun=%t, redis=%s\n",
		startTime.String(), *dryRun, strings.Join(*serverAddrs, " "))

	wg := &sync.WaitGroup{}
	ch := make(chan Result, len(*serverAddrs))
	db := base.GetAccessedRecordDBCluster()
	for _, server := range *serverAddrs {
		wg.Add(1)
		go scanServer(server, *dryRun, ch, logger, wg, db)
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
	logger.Printf("total process %d keys, skip %d keys, duration %s\n", totalProcessCount, totalSkipCount, time.Since(startTime).String())
}

func scanServer(server string, dryRun bool, ch chan<- Result, logger *log.Logger, wg *sync.WaitGroup, db *base.DBCluster) {
	options := &redis.Options{
		Addr: server,
	}
	client := redis.NewClient(options)
	keyPattern := "*}:_m"
	var count int64 = 100
	result := Result{server: server}
	var cursor uint64 = 0
	var stopCursor uint64 = 0
	defer func() {
		ch <- result
		wg.Done()
	}()
	logger.Printf("start to scan redis %s\n", server)
loop:
	for {
		keys, c, err := client.Scan(context.Background(), cursor, keyPattern, count).Result()
		if err != nil {
			result.err = err
			logger.Printf("scan error %s\n", err.Error())
			break loop
		}
		for _, key := range keys {
			needToProcess, err := isKeyNeededToProcess(logger, client, key)
			if err != nil {
				result.err = err
				logger.Printf("check need process error key %s error %s\n", key, err.Error())
				break loop
			}
			if needToProcess {
				logger.Printf("prepare to process key %s\n", key)
				hashTag := extractHashTagFromKey(key)
				model, err := loadAccessedRecordModelByID(db, hashTag)
				if err != nil {
					result.err = err
					logger.Printf("load access record error hash_tag %s error %s\n", hashTag, err.Error())
					break loop
				}
				var ts int64
				if model != nil {
					ts = utility.TimestampInMS(model.AccessedAt)
				} else {
					logger.Printf("access record not found error hash_tag %s\n", hashTag)
					ts = utility.TimestampInMS(time.Now())
				}
				if !dryRun {
					fieldCount, err := processKey(client, key, ts)
					if err != nil {
						result.err = err
						logger.Printf("process key %s error %s\n", key, err.Error())
						break loop
					}
					logger.Printf("process key success %s, field %d\n", key, fieldCount)
				}
				result.processCount += 1
			} else {
				result.skipCount += 1
				logger.Printf("skip key %s\n", key)
			}
		}
		logger.Printf("server %s scan cursor %d\n", server, c)
		if c == stopCursor {
			break loop
		}
		cursor = c
		time.Sleep(100 * time.Millisecond)
	}
}

func isKeyNeededToProcess(logger *log.Logger, client *redis.Client, key string) (bool, error) {
	isValidKey := isMetaKey(key)
	if !isValidKey {
		return false, nil
	}
	value, err := client.HGetAll(context.TODO(), key).Result()
	if err != nil {
		return false, err
	}
	logger.Printf("get meta key value %s %+v\n", key, value)
	if len(value) != 1 {
		return false, nil
	}
	status, ok := value["status"]
	if !ok {
		return false, nil
	}
	return status == "L", nil
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

func processKey(client *redis.Client, key string, ts int64) (int64, error) {
	scriptSrc := `
		if redis.call("hlen", KEYS[1]) ~= 1 then
			return 0
		end
		return redis.call("hset", KEYS[1], "at", ARGV[1], "wt", ARGV[1], "v", 0)
	`
	script := redis.NewScript(scriptSrc)
	result, err := script.Run(context.TODO(), client, []string{key}, ts).Result()
	if err != nil {
		return 0, err
	}
	return result.(int64), nil
}

func loadAccessedRecordModelByID(db *base.DBCluster, hashTag string) (*roomAccessedRecordModelV2, error) {
	model := &roomAccessedRecordModelV2{HashTag: hashTag}
	query, err := db.Model(model)
	if err != nil {
		return nil, err
	}
	err = query.WherePK().Select()
	if err != nil {
		if errors.Is(err, pg.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return model, nil
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
