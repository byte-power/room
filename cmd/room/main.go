package main

import (
	"bytepower_room/base"
	"bytepower_room/service"
	"fmt"
	_ "net/http/pprof"

	"github.com/spf13/pflag"
)

var configPath = pflag.StringP("config", "c", "config.yaml", "config file path")
var version = pflag.BoolP("version", "v", false, "room version")
var roomVersion = "1.1.0"

func main() {
	pflag.Parse()
	if *version {
		fmt.Println(roomVersion)
		return
	}
	if err := base.InitRoomService(*configPath); err != nil {
		panic(err)
	}

	service.StartServer()
	logger := base.GetServerLogger()
	logger.Info("room server is stopped, try to stop other related services...")
	base.StopServices()

}
