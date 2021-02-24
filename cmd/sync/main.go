package main

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"bytepower_room/service"
	"fmt"
	"time"

	"github.com/byte-power/gorich/task"
	"github.com/spf13/pflag"
)

var configPath = pflag.StringP("config", "c", "config.yaml", "config file path")

func main() {
	pflag.Parse()
	if configPath == nil {
		panic("config not found")
	}
	if err := base.InitSyncService(*configPath); err != nil {
		panic(err)
	}
	syncRecordTaskConfig := base.GetServerConfig().SyncService.SyncRecordTask
	syncRecordTask := "sync_records"
	if !syncRecordTaskConfig.Off {
		task.Periodic(syncRecordTask, service.SyncRecordsTask).EveryMinutes(syncRecordTaskConfig.IntervalMinutes).AtSecondInMinute(20)
	}

	syncKeyTaskConfig := base.GetServerConfig().SyncService.SyncKeyTask
	syncKeyTask := "sync_keys"
	if !syncKeyTaskConfig.Off {
		task.Periodic(syncKeyTask, service.SyncKeysTask).EveryMinutes(syncKeyTaskConfig.IntervalMinutes).AtSecondInMinute(20)
	}

	cleanKeyTaskConfig := base.GetServerConfig().SyncService.CleanKeyTask
	cleanKeyTask := "clean_keys"
	if !cleanKeyTaskConfig.Off {
		cleanKeyTaskInterval := cleanKeyTaskConfig.IntervalMinutes
		inactiveDuration := cleanKeyTaskConfig.InactiveDuration
		task.Periodic(cleanKeyTask, service.CleanKeysTask, inactiveDuration).EveryMinutes(cleanKeyTaskInterval).AtSecondInMinute(20)
	}
	go monitorScheduler()
	task.StartScheduler()
}

func monitorScheduler() {
	logger := base.GetTaskLogger()
	for {
		count := task.JobCount()
		logger.Info("job_count", log.Int("count", count))
		// handle all job stats
		allJobStats := task.JobStats()
		for jobName, jobStats := range allJobStats {
			for _, stat := range jobStats {
				if !stat.IsSuccess {
					logger.Info(
						"job stats",
						log.String("name", jobName),
						log.String("stat", fmt.Sprint(stat.ToMap())),
					)
				}
			}
		}
		time.Sleep(5 * time.Second)
	}
}
