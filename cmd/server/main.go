package main

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"bytepower_room/service"
	"fmt"
	_ "net/http/pprof"
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
	if err := base.InitRoomServer(*configPath); err != nil {
		panic(err)
	}

	base.StartServices()
	dep := base.GetServerDependency()
	logger := dep.Logger
	config := base.GetServerConfig()
	roomService, err := service.NewRoomService(config, dep)
	if err != nil {
		panic(err)
	}
	roomService.Run()
	logger.Info("service has started")

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	sig := <-signalCh
	logger.Info("signal received, closing service...", log.String("signal", sig.String()))
	roomService.Stop()
	logger.Info("room server is closed")
	logger.Info("room server is stopped, try to stop other related services...")
	base.StopServices()
}
