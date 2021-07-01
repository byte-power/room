package main

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"bytepower_room/service"
	"fmt"
	"os"
	"os/signal"
	"syscall"

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
	config := base.GetServerConfig().CollectEventService
	logger := base.GetTaskLogger()
	metric := base.GetTaskMetricService()
	db := base.GetDBCluster()
	collectEventService, err := service.NewCollectEventService(config, logger, metric, db)
	if err != nil {
		panic(err)
	}
	collectEventService.Run()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	sig := <-signalCh

	logger.Info(
		fmt.Sprintf("signal received, closing %s ...", service.CollectEventServiceName),
		log.String("signal", sig.String()))

	collectEventService.Stop()
	logger.Info(fmt.Sprintf("close %s success", service.CollectEventServiceName))
}
