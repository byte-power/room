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

	syncKeyTaskConfigV2 := base.GetServerConfig().SyncService.SyncKeyTaskV2
	syncKeyTaskV2 := service.SyncKeysTaskName
	if !syncKeyTaskConfigV2.Off {
		upsertTryTimes := syncKeyTaskConfigV2.UpSertTryTimes
		noWrittenDuration := syncKeyTaskConfigV2.NoWrittenDuration
		rateLimitPerSecond := syncKeyTaskConfigV2.RateLimitPerSecond
		job, err := task.Periodic(syncKeyTaskV2, service.SyncKeysTaskV2, upsertTryTimes, noWrittenDuration, rateLimitPerSecond).
			EveryMinutes(syncKeyTaskConfigV2.IntervalMinutes).AtSecondInMinute(20)
		if err != nil {
			panic(err)
		}
		job.SetCoordinate(coordinator)
	}

	cleanKeyTaskConfigV2 := base.GetServerConfig().SyncService.CleanKeyTaskV2
	cleanKeyTaskV2 := service.CleanKeysTaskName
	if !cleanKeyTaskConfigV2.Off {
		cleanKeyTaskInterval := cleanKeyTaskConfigV2.IntervalMinutes
		inactiveDuration := cleanKeyTaskConfigV2.InactiveDuration
		rateLimtPerSecond := cleanKeyTaskConfigV2.RateLimitPerSecond
		job, err := task.Periodic(cleanKeyTaskV2, service.CleanKeysTaskV2, inactiveDuration, rateLimtPerSecond).
			EveryMinutes(cleanKeyTaskInterval).AtSecondInMinute(20)
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
