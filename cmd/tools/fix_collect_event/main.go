package main

import (
	"bufio"
	"bytepower_room/base"
	"bytepower_room/service"
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/spf13/pflag"
)

var configPath = pflag.StringP("config", "c", "config.yaml", "config file path")
var eventLogFile = pflag.StringP("event", "e", "event.log", "event log file")
var dryRun = pflag.BoolP("dryrun", "d", true, "dry run")

type EventRecord struct {
	Event string `json:"event"`
}

func main() {
	pflag.Parse()
	if configPath == nil {
		panic("config not found")
	}
	if eventLogFile == nil {
		panic("event log file not found")
	}
	if dryRun == nil {
		panic("dryrun not set")
	}
	logger := log.New(os.Stdout, "fix_collect_event ", log.LstdFlags)
	logger.Printf("start to process, config=%s, event_log=%s, dryrun=%t\n", *configPath, *eventLogFile, *dryRun)
	events, err := parseEventsFromFile(*eventLogFile)
	if err != nil {
		logger.Printf("parse_file_error:%s\n", err.Error())
		return
	}
	if err = base.InitCollectEvent(*configPath); err != nil {
		panic(err)
	}

	successCount := 0
	failedCount := 0

	for _, event := range events {
		logger.Printf(
			"start_save_event:%s, keys=%v, at_is_zero=%t, wt_is_zero=%t\n",
			event.String(), event.Keys, event.AccessTime.IsZero(), event.WriteTime.IsZero())
		if !*dryRun {
			err = service.SaveEvent(context.TODO(), event, time.Now())
			if err != nil {
				failedCount += 1
				logger.Printf("save_event_error:%s, event:%s\n", err.Error(), event.String())
			} else {
				successCount += 1
				logger.Printf("save_event_success event:%s\n", event.String())
			}
		}
	}
	log.Printf("finish process, success=%d, fail=%d\n", successCount, failedCount)
}

func parseEventsFromFile(filename string) ([]base.HashTagEvent, error) {
	events := make([]base.HashTagEvent, 0)
	f, err := os.Open(filename)
	if err != nil {
		return events, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var record EventRecord
		err = json.Unmarshal(scanner.Bytes(), &record)
		if err != nil {
			return events, err
		}
		var event base.HashTagEvent
		err = json.Unmarshal([]byte(record.Event), &event)
		if err != nil {
			return events, err
		}
		if err = event.Check(); err != nil {
			return events, err
		}
		events = append(events, event)
	}
	return events, scanner.Err()
}
