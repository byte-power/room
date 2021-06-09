package main

import (
	"bytepower_room/base"
	"bytepower_room/service"

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
	service.ReportEvents()
}
