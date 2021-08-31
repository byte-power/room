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
	if err := base.InitCollectEvent(*configPath); err != nil {
		panic(err)
	}
	dep := base.GetCollectEventDependency()
	if err := dep.Check(); err != nil {
		panic(err)
	}

	config := base.GetCollectEventConfig()
	collectEventService, err := service.NewCollectEventService(config, dep.Logger, dep.Metric, dep.DB)
	if err != nil {
		panic(err)
	}
	dep.Logger.Info("init_collect_event_service", log.String("config", fmt.Sprintf("%+v", *collectEventService.Config())))

	collectEventService.Run()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	sig := <-signalCh

	dep.Logger.Info(
		fmt.Sprintf("signal received, closing %s ...", service.CollectEventServiceName),
		log.String("signal", sig.String()))

	collectEventService.Stop()
	dep.Logger.Info(fmt.Sprintf("close %s success", service.CollectEventServiceName))
}
