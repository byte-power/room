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
	coordinatorConfig := base.GetServerConfig().SyncService.Coordinator
	coordinator := task.NewCoordinatorFromRedisCluster(coordinatorConfig.Name, coordinatorConfig.Addrs)
	syncRecordTaskConfig := base.GetServerConfig().SyncService.SyncRecordTask
	syncRecordTask := "sync_records"
	if !syncRecordTaskConfig.Off {
		job, err := task.Periodic(syncRecordTask, service.SyncRecordsTask).EveryMinutes(syncRecordTaskConfig.IntervalMinutes).AtSecondInMinute(20)
		if err != nil {
			panic(err)
		}
		job.SetCoordinate(coordinator)
	}

	syncKeyTaskConfig := base.GetServerConfig().SyncService.SyncKeyTask
	syncKeyTask := "sync_keys"
	if !syncKeyTaskConfig.Off {
		job, err := task.Periodic(syncKeyTask, service.SyncKeysTask, syncKeyTaskConfig.UpSertTryTimes).EveryMinutes(syncKeyTaskConfig.IntervalMinutes).AtSecondInMinute(20)
		if err != nil {
			panic(err)
		}
		job.SetCoordinate(coordinator)
	}

	cleanKeyTaskConfig := base.GetServerConfig().SyncService.CleanKeyTask
	cleanKeyTask := "clean_keys"
	if !cleanKeyTaskConfig.Off {
		cleanKeyTaskInterval := cleanKeyTaskConfig.IntervalMinutes
		inactiveDuration := cleanKeyTaskConfig.InactiveDuration
		job, err := task.Periodic(cleanKeyTask, service.CleanKeysTask, inactiveDuration).EveryMinutes(cleanKeyTaskInterval).AtSecondInMinute(20)
		if err != nil {
			panic(err)
		}
		job.SetCoordinate(coordinator)
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
				if !stat.IsSuccess && !task.IsCoordinateError(stat.Err) {
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
