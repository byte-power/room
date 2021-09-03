package service

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"bytepower_room/commands"
	"bytepower_room/utility"
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gogf/greuse"
	jsoniter "github.com/json-iterator/go"
	"github.com/tidwall/redcon"
)

var contextTODO = context.TODO()

var connectionTotal int64

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func newInternalError(err error) error {
	return fmt.Errorf("ERR internal error, %w", err)
}

func newLoadError(err error) error {
	return fmt.Errorf("ERR load data error, %w", err)
}

func newInvalidKeyError(key string) error {
	return fmt.Errorf("ERR key %s is not valid", key)
}

var errInvalidResponse = errors.New("ERR invalid command response")

type RoomService struct {
	config       *base.RoomServerConfig
	dep          base.Dependency
	address      string
	server       *redcon.Server
	pprofAddress string
	pprofServer  *http.Server
	pid          int
}

func NewRoomService(config *base.RoomServerConfig, dep base.Dependency, host string, port int) (*RoomService, error) {
	if err := config.Check(); err != nil {
		return nil, err
	}
	if err := dep.Check(); err != nil {
		return nil, err
	}
	if host == "" {
		return nil, errors.New("host should not be empty")
	}
	if port <= 0 {
		return nil, errors.New("port should be greater than 0")
	}

	roomService := &RoomService{
		config:       config,
		dep:          dep,
		address:      fmt.Sprintf("%s:%d", host, port),
		pprofAddress: fmt.Sprintf("%s:%d", host, port+10000),
		pid:          os.Getpid()}
	return roomService, nil
}

func (service *RoomService) Run() {
	service.dep.Logger.Info("starting room server...", log.String("address", service.address))
	service.server = redcon.NewServer(service.address, service.connServeHandler, service.connAcceptHandler, service.connCloseHandler)
	service.server.AcceptError = service.connAcceptErrorHandler
	listener, err := greuse.Listen("tcp", service.address)
	if err != nil {
		service.dep.Logger.Error("start room server failed", log.Error(err))
		panic(err)
	}
	go func() {
		if err := service.server.Serve(listener); err != nil {
			service.dep.Logger.Error("serve room server failed", log.Error(err))
			panic(err)
		}
	}()

	// start pprof server
	if service.config.EnablePProf {
		service.dep.Logger.Info("starting pprof server...", log.String("address", service.pprofAddress))
		service.pprofServer = &http.Server{Handler: nil}
		listener, err := greuse.Listen("tcp", service.pprofAddress)
		if err != nil {
			service.dep.Logger.Error("start pprof server failed", log.Error(err))
			panic(err)
		}
		go func() {
			if err := service.pprofServer.Serve(listener); err != nil && err != http.ErrServerClosed {
				service.dep.Logger.Error("serve pprof server failed", log.Error(err))
				panic(err)
			}
		}()
	}
}

func (service *RoomService) Stop() {
	if err := service.server.Close(); err != nil {
		service.dep.Logger.Error("close room server error", log.Error(err))
	}
	if service.pprofServer != nil {
		if err := service.pprofServer.Close(); err != nil {
			service.dep.Logger.Error("close pprof server error", log.Error(err))
		}
	}
}

func (service *RoomService) connAcceptHandler(conn redcon.Conn) bool {
	service.dep.Metric.MetricIncrease("connection.accept")
	service.dep.Metric.MetricGauge("connection.total", atomic.AddInt64(&connectionTotal, 1))
	service.dep.Metric.MetricGauge("transaction.total", transactionManager.transactionCount())
	service.dep.Logger.Debug(
		"accept incomming request",
		log.String("local_addr", conn.NetConn().LocalAddr().String()),
		log.String("remote_addr", conn.RemoteAddr()),
		log.Int("transaction_count", transactionManager.transactionCount()),
		log.Int("pid", service.pid),
	)
	return true
}

func (service *RoomService) connAcceptErrorHandler(err error) {
	service.dep.Metric.MetricIncrease("error.accept")
	service.dep.Logger.Error("accept error", log.Error(err))
}

func (service *RoomService) connServeHandler(conn redcon.Conn, cmd redcon.Command) {
	serveStartTime := time.Now()
	args := make([]string, len(cmd.Args))
	for index, arg := range cmd.Args {
		args[index] = string(arg)
	}

	redisCluster := service.dep.Redis
	logger := service.dep.Logger
	metric := service.dep.Metric
	metric.MetricIncrease("process.command")

	logger.Debug(
		"start to exec command",
		log.String("command", strings.Join(args, " ")),
		log.String("remote_addr", conn.RemoteAddr()),
		log.String("local_addr", conn.NetConn().LocalAddr().String()),
	)

	// Parse command
	command, err := commands.ParseCommand(args)
	if err != nil {
		metric.MetricIncrease("error.parse_command")
		logger.Error(
			"parse command error",
			log.String("command", strings.Join(args, " ")),
			log.Error(err))
		conn.WriteError(err.Error())
		return
	}

	// Pre Porcess related keys
	if err = preProcessCommand(service.dep, command, serveStartTime); err != nil {
		metric.MetricIncrease("error.pre_process")
		logger.Error(
			"preprocess command error",
			log.String("command", command.String()),
			log.Error(err),
		)
		conn.WriteError(err.Error())
		return
	}

	transaction, err := getTransactionIfNeeded(service.dep, conn, command)
	if err != nil {
		metric.MetricIncrease("error.get_transaction")
		logger.Error(
			"get transaction error",
			log.String("command", command.String()),
			log.Error(err),
		)
		conn.WriteError(err.Error())
		return
	}

	var result commands.RESPData
	if transaction != nil {
		metric.MetricIncrease("process.transaction")
		startTime := time.Now()
		result = transaction.Process(command)
		if command.Name() == "exec" {
			metric.MetricTimeDuration("process.transaction.duration", time.Since(startTime))
		}
		if transaction.IsClosed() {
			transactionManager.removeTransaction(conn, commands.TransactionCloseReasonTxClosed)
		}
	} else {
		metric.MetricIncrease("process.single_command")
		startTime := time.Now()
		result = commands.ExecuteCommand(redisCluster, command)
		metric.MetricTimeDuration("process.single_connamd.duration", time.Since(startTime))
	}
	writeDataToConnection(conn, result)
	startTime := time.Now()
	if err := sendCommandEvents(command, serveStartTime); err != nil {
		metric.MetricIncrease("error.send_event")
		logger.Error(
			"send event error",
			log.String("command", strings.Join(args, " ")),
			log.Error(err),
		)
	}
	metric.MetricTimeDuration("process.send_event.duration", time.Since(startTime))
	duration := time.Since(serveStartTime)
	logger.Debug(
		"end to exec command",
		log.String("command", command.String()),
		log.String("result", result.String()),
		log.String("duration", duration.String()),
	)
	metric.MetricTimeDuration("process.command.duration", duration)
}

func isTransactionNeeded(command commands.Commander) bool {
	transactionCommands := []string{"watch", "multi"}
	return utility.StringSliceContains(transactionCommands, command.Name())
}

func preProcessCommand(dep base.Dependency, command commands.Commander, accessTime time.Time) error {
	logger := dep.Logger

	hashTag, err := commands.CheckAndGetCommandKeysHashTag(command)
	if err != nil {
		return err
	}
	if err := Load(dep, hashTag, accessTime, commands.GetCommnadKeysAccessMode(command)); err != nil {
		logger.Error(
			"load hash_tag error",
			log.String("command", command.String()),
			log.String("hash_tag", hashTag),
			log.Error(err),
		)
		return newLoadError(err)
	}
	return nil
}

func getTransactionIfNeeded(dep base.Dependency, conn redcon.Conn, command commands.Commander) (*commands.Transaction, error) {
	logger := dep.Logger
	metric := dep.Metric
	transaction := transactionManager.getTransaction(conn)
	if transaction == nil {
		if isTransactionNeeded(command) {
			transaction = commands.NewTransaction(dep)
			transactionManager.addTransaction(conn, transaction)
			metric.MetricIncrease("transaction.new")
			logger.Debug(
				"create transaction",
				log.String("command", command.Name()),
				log.String("remote_addr", conn.RemoteAddr()),
				log.String("local_addr", conn.NetConn().LocalAddr().String()),
			)
		}
	}
	return transaction, nil
}

func writeDataToConnection(conn redcon.Conn, data commands.RESPData) {
	switch data.DataType {
	case commands.SimpleStringRespType:
		conn.WriteString(utility.AnyToString(data.Value))
	case commands.BulkStringRespType:
		conn.WriteBulkString(utility.AnyToString(data.Value))
	case commands.ErrorRespType:
		err, ok := data.Value.(error)
		if !ok {
			conn.WriteError(errInvalidResponse.Error())
		} else {
			conn.WriteError(err.Error())
		}
	case commands.IntegerRespType:
		num, ok := data.Value.(int64)
		if !ok {
			conn.WriteError(errInvalidResponse.Error())
		} else {
			conn.WriteInt64(num)
		}
	case commands.NilRespType:
		conn.WriteNull()
	case commands.ArrayRespType:
		array, ok := data.Value.([]commands.RESPData)
		if !ok {
			conn.WriteError(errInvalidResponse.Error())
		} else {
			conn.WriteArray(len(array))
			for _, item := range array {
				writeDataToConnection(conn, item)
			}
		}
	}
}

func sendCommandEvents(command commands.Commander, accessTime time.Time) error {
	hashTagEventService := base.GetHashTagEventService()
	hashTag, err := commands.CheckAndGetCommandKeysHashTag(command)
	if err != nil {
		return err
	}
	if hashTag == "" {
		return nil
	}
	keys := append(command.ReadKeys(), command.WriteKeys()...)
	if err := hashTagEventService.SendEvent(hashTag, keys, commands.GetCommnadKeysAccessMode(command), accessTime); err != nil {
		return err
	}
	return nil
}

func (server *RoomService) connCloseHandler(conn redcon.Conn, err error) {
	metric := server.dep.Metric
	metric.MetricIncrease("connection.close")
	transactionManager.removeTransaction(conn, commands.TransactionCloseReasonConnClosed)
	transactionCount := transactionManager.transactionCount()
	logger := server.dep.Logger
	if err == nil {
		logger.Debug(
			"connection is closed",
			log.String("remote_addr", conn.RemoteAddr()),
			log.String("local_addr", conn.NetConn().LocalAddr().String()),
			log.Int("transaction_count", transactionCount),
		)
	} else {
		logger.Error(
			"connection is closed",
			log.String("remote_addr", conn.RemoteAddr()),
			log.String("local_addr", conn.NetConn().LocalAddr().String()),
			log.Int("transaction_count", transactionCount),
			log.Error(err),
		)
	}
	metric.MetricGauge("connection.total", atomic.AddInt64(&connectionTotal, -1))
	metric.MetricGauge("transaction.total", transactionCount)
}
