package main

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"bytepower_room/service"
	"errors"
	"fmt"
	"time"

	"github.com/byte-power/gorich/task"
	"github.com/spf13/pflag"
)

var configPath = pflag.StringP("config", "c", "config.yaml", "config file path")
var versionFlag = pflag.BoolP("version", "v", false, "service version")
var version string

func main() {
	pflag.Parse()
	if *versionFlag {
		fmt.Println(version)
		return
	}

	if configPath == nil {
		panic("config not found")
	}
	if err := base.InitRoomTask(*configPath); err != nil {
		panic(err)
	}
	dep := base.GetTaskDependency()
	coordinatorConfig := base.GetTaskConfig().Coordinator
	coordinator := task.NewCoordinatorFromRedisCluster(coordinatorConfig.Name, coordinatorConfig.Addrs)

	syncKeyTaskConfig := base.GetTaskConfig().SyncKeyTask
	syncKeyTask := service.SyncKeysTaskName
	if !syncKeyTaskConfig.Off {
		upsertTryTimes := syncKeyTaskConfig.UpSertTryTimes
		noWrittenDuration := syncKeyTaskConfig.NoWrittenDuration
		rateLimitPerSecond := syncKeyTaskConfig.RateLimitPerSecond
		job, err := task.Periodic(syncKeyTask, service.SyncKeysTask, dep, upsertTryTimes, noWrittenDuration, rateLimitPerSecond).
			EveryMinutes(syncKeyTaskConfig.IntervalMinutes).AtSecondInMinute(20)
		if err != nil {
			panic(err)
		}
		job.SetCoordinate(coordinator)
	}

	cleanKeyTaskConfig := base.GetTaskConfig().CleanKeyTask
	cleanKeyTask := service.CleanKeysTaskName
	if !cleanKeyTaskConfig.Off {
		cleanKeyTaskInterval := cleanKeyTaskConfig.IntervalMinutes
		inactiveDuration := cleanKeyTaskConfig.InactiveDuration
		rateLimtPerSecond := cleanKeyTaskConfig.RateLimitPerSecond
		job, err := task.Periodic(cleanKeyTask, service.CleanKeysTask, dep, inactiveDuration, rateLimtPerSecond).
			EveryMinutes(cleanKeyTaskInterval).AtSecondInMinute(20)
		if err != nil {
			panic(err)
		}
		job.SetCoordinate(coordinator)
	}
	go monitorScheduler(dep.Logger)
	task.StartScheduler()
}

func monitorScheduler(logger *log.Logger) {
	for {
		count := task.JobCount()
		logger.Info("job_count", log.Int("count", count))
		// handle all job stats
		allJobStats := task.JobStats()
		for jobName, jobStats := range allJobStats {
			for _, stat := range jobStats {
				if !stat.IsSuccess && !canErrorBeIgnored(stat.Err) {
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

func canErrorBeIgnored(err error) bool {
	return task.IsCoordinateError(err) || errors.Is(err, task.ErrJobTimeout)
}
