package service

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"bytepower_room/commands"
	"bytepower_room/utility"
	"context"
	"errors"
	"fmt"
	stdLog "log"
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
	stopCh       chan bool
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
		pid:          os.Getpid(),
		stopCh:       make(chan bool),
	}
	return roomService, nil
}

func (service *RoomService) Run() {
	service.logWithAddressAndPid(log.LevelInfo, "server.start")
	service.server = redcon.NewServer(service.address, service.connServeHandler, service.connAcceptHandler, service.connCloseHandler)
	logger := stdLog.New(os.Stdout, "room redcon ", stdLog.LstdFlags)
	service.server.SetLogger(logger)
	service.server.AcceptError = service.connAcceptErrorHandler
	listener, err := greuse.Listen("tcp", service.address)
	if err != nil {
		service.logWithAddressAndPid(log.LevelError, "error.server.listen", log.Error(err))
		panic(err)
	}
	go func() {
		if err := service.server.Serve(listener); err != nil {
			service.logWithAddressAndPid(log.LevelError, "error.server.serve", log.Error(err))
			panic(err)
		}
	}()

	go service.monitorConnections()

	// start pprof server
	if service.config.EnablePProf {
		service.logWithAddressAndPid(log.LevelInfo, "server.pprof_start")
		service.pprofServer = &http.Server{Handler: nil}
		listener, err := greuse.Listen("tcp", service.pprofAddress)
		if err != nil {
			service.logWithAddressAndPid(log.LevelError, "error.server.pprof_listen", log.Error(err))
			panic(err)
		}
		go func() {
			if err := service.pprofServer.Serve(listener); err != nil && err != http.ErrServerClosed {
				service.logWithAddressAndPid(log.LevelError, "error.server.pprof_serve", log.Error(err))
				panic(err)
			}
		}()
	}
}

func (service *RoomService) monitorConnections() {
	metric := service.dep.Metric
	ticker := time.NewTicker(service.config.MonitorConnectionInterval)
	defer func() {
		ticker.Stop()
	}()
loop:
	for {
		select {
		case <-ticker.C:
			connectionCount := service.server.OpenConnectionCount()
			transactionCount := transactionManager.transactionCount()
			service.logWithAddressAndPid(
				log.LevelInfo, "connection.info",
				log.Int("connection_count", connectionCount),
				log.Int64("total_connection_count", atomic.LoadInt64(&connectionTotal)),
				log.Int("transaction_count", transactionCount),
			)
			metric.MetricGauge("connection.total", connectionCount)
			metric.MetricGauge("transaction.total", transactionCount)
		case <-service.stopCh:
			break loop
		}
	}
}

func (service *RoomService) Stop(waitDuration time.Duration) {
	closeResults, err := service.server.Close(waitDuration)
	if err != nil {
		service.logWithAddressAndPid(log.LevelError, "error.server.close", log.Error(err))
		service.dep.Metric.MetricIncrease("error.close.server")
	} else if len(closeResults.Errs) != 0 {
		service.logWithAddressAndPid(
			log.LevelError,
			"error.server.connection.close.count",
			log.Int("error_count", len(closeResults.Errs)),
			log.Int("success_count", closeResults.Count),
		)
		service.dep.Metric.MetricCount("error.close.connection", len(closeResults.Errs))
		for _, err := range closeResults.Errs {
			service.logWithAddressAndPid(log.LevelError, "error.server.connection.close", log.Error(err))
		}
	} else {
		service.logWithAddressAndPid(log.LevelInfo, "server.connection.close.count", log.Int("count", closeResults.Count))
	}
	if service.pprofServer != nil {
		if err := service.pprofServer.Close(); err != nil {
			service.logWithAddressAndPid(log.LevelError, "error.server.pprof_close", log.Error(err))
		}
	}
	close(service.stopCh)
}

func (service *RoomService) connAcceptHandler(conn redcon.Conn) bool {
	service.dep.Metric.MetricIncrease("connection.accept")
	connectionCount := atomic.AddInt64(&connectionTotal, 1)
	service.logWithAddressAndPid(
		log.LevelDebug, "connection.accept",
		log.String("local_addr", conn.NetConn().LocalAddr().String()),
		log.String("remote_addr", conn.RemoteAddr()),
		log.Int("transaction_count", transactionManager.transactionCount()),
		log.Int64("connection_count", connectionCount),
	)
	return true
}

func (service *RoomService) connAcceptErrorHandler(err error) {
	service.dep.Metric.MetricIncrease("error.accept")
	service.logWithAddressAndPid(log.LevelError, "error.accept", log.Error(err))
}

func (service *RoomService) connServeHandler(conn redcon.Conn, cmds []redcon.Command) {
	serveStartTime := time.Now()

	redisCluster := service.dep.Redis
	metric := service.dep.Metric

	cmdCount := len(cmds)
	toBeExecutedCommandBatch := commands.NewCommandBatch()
	allCommands := make([]commands.Commander, 0, cmdCount)
	results := make([]commands.RESPData, cmdCount)

	metric.MetricCount("receive.command", cmdCount)
	metric.MetricGauge("command.batch.total", cmdCount)

	for index, cmd := range cmds {
		command, err := service.preProcessCommand(cmd, serveStartTime)
		if err != nil {
			metric.MetricIncrease("error.pre_process")
			service.logWithAddressAndPid(
				log.LevelError, "error.pre_process",
				log.String("command", string(cmd.Raw)),
				log.Error(err),
			)
			results[index] = commands.ConvertErrorToRESPData(err)
			transaction := transactionManager.getTransaction(conn)
			if transaction != nil {
				metric.MetricIncrease("error.in_transaction")
				transactionManager.removeTransaction(conn, commands.TransactionCloseReasonInvalidCommand)
			}
			continue
		}
		service.logWithAddressAndPid(
			log.LevelDebug,
			"receive.command",
			log.String("command", command.String()),
		)

		allCommands = append(allCommands, command)
		transaction := getTransactionIfNeeded(service.dep, conn, command)
		if transaction != nil && (transaction.IsStarted() || isTransactionCommand(command)) {
			resultMap := toBeExecutedCommandBatch.Execute(context.TODO(), redisCluster)
			for index, result := range resultMap {
				results[index] = result
			}
			toBeExecutedCommandBatch = commands.NewCommandBatch()
			startTime := time.Now()
			results[index] = transaction.Process(command)
			if transaction.IsClosed() {
				transactionManager.removeTransaction(conn, commands.TransactionCloseReasonTxClosed)
				metric.MetricIncrease(fmt.Sprintf("process.transaction.by_%s", command.Name()))
				metric.MetricTimeDuration(fmt.Sprintf("process.transaction.by_%s.duration", command.Name()), time.Since(startTime))
			}
		} else {
			toBeExecutedCommandBatch.AddCommand(index, command)
		}
	}
	resultMap := toBeExecutedCommandBatch.Execute(context.TODO(), redisCluster)
	for index, result := range resultMap {
		results[index] = result
	}
	for _, result := range results {
		writeDataToConnection(conn, result)
	}
	service.sendEvents(allCommands, serveStartTime)
	service.recordCommands(allCommands, results, serveStartTime)
	if service.server.IsServerClosing() && conn.InTx() {
		service.logWithAddressAndPid(
			log.LevelInfo,
			"conn in tx, cannot close",
			log.Any("conn", conn),
		)
	}
}

func (service *RoomService) preProcessCommand(cmd redcon.Command, serveStartTime time.Time) (commands.Commander, error) {
	args := make([]string, 0, len(cmd.Args))
	for _, arg := range cmd.Args {
		args = append(args, string(arg))
	}

	// Parse command
	command, err := commands.ParseCommand(args)
	if err != nil {
		return nil, err
	}

	// Pre Porcess related keys
	if err = preProcessCommand(service.dep, command, serveStartTime); err != nil {
		return nil, err
	}
	return command, nil
}

func (service *RoomService) sendEvents(cmds []commands.Commander, serveStartTime time.Time) {
	startTime := time.Now()
	metric := service.dep.Metric
	for _, command := range cmds {
		if err := sendCommandEvents(command, serveStartTime); err != nil {
			metric.MetricIncrease("error.send_event")
			service.logWithAddressAndPid(
				log.LevelError, "error.send_event",
				log.String("command", command.String()),
				log.Error(err),
			)
		}
	}
	metric.MetricTimeDuration("process.send_event.duration", time.Since(startTime))
}

func (service *RoomService) recordCommands(cmds []commands.Commander, results []commands.RESPData, serveStartTime time.Time) {
	duration := time.Since(serveStartTime)
	if service.config.IsDebug {
		commandsStrSlice := make([]string, 0, len(cmds))
		for _, command := range cmds {
			commandsStrSlice = append(commandsStrSlice, command.String())
		}
		resultsStrSlice := make([]string, 0, len(results))
		for _, result := range results {
			resultsStrSlice = append(resultsStrSlice, result.String())
		}
		service.logWithAddressAndPid(
			log.LevelDebug, "commands.end",
			log.String("commands", strings.Join(commandsStrSlice, " ")),
			log.String("results", strings.Join(resultsStrSlice, " ")),
			log.String("duration", duration.String()),
		)
	}
	service.dep.Metric.MetricTimeDuration("process.commands.duration", duration)
}

func isTransactionNeeded(command commands.Commander) bool {
	transactionCommands := []string{"watch", "multi"}
	return utility.StringSliceContains(transactionCommands, command.Name())
}

func isTransactionCommand(command commands.Commander) bool {
	transactionCommands := []string{"watch", "unwatch", "multi", "exec", "discard"}
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

func getTransactionIfNeeded(dep base.Dependency, conn redcon.Conn, command commands.Commander) *commands.Transaction {
	logger := dep.Logger
	metric := dep.Metric
	transaction := transactionManager.getTransaction(conn)
	if transaction == nil {
		if isTransactionNeeded(command) {
			transaction = commands.NewTransaction(dep, conn)
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
	return transaction
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
	case commands.NilArrayRespType:
		conn.WriteRaw([]byte("*-1\r\n"))
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

func (service *RoomService) connCloseHandler(conn redcon.Conn, err error) {
	metric := service.dep.Metric
	metric.MetricIncrease("connection.close")
	transactionManager.removeTransaction(conn, commands.TransactionCloseReasonConnClosed)
	transactionCount := transactionManager.transactionCount()
	connectionCount := atomic.AddInt64(&connectionTotal, -1)
	if err == nil {
		service.logWithAddressAndPid(
			log.LevelDebug, "connection.close",
			log.String("remote_addr", conn.RemoteAddr()),
			log.String("local_addr", conn.NetConn().LocalAddr().String()),
			log.Int("transaction_count", transactionCount),
			log.Int64("connection_count", connectionCount),
		)
	} else {
		metric.MetricIncrease("error.conn_close")
		service.logWithAddressAndPid(
			log.LevelError, "error.conn_close",
			log.String("remote_addr", conn.RemoteAddr()),
			log.String("local_addr", conn.NetConn().LocalAddr().String()),
			log.Int("transaction_count", transactionCount),
			log.Int64("connection_count", connectionCount),
			log.Error(err),
		)
	}
}

func (service *RoomService) logWithAddressAndPid(level log.Level, subject string, logPairs ...log.LogPair) {
	pairs := append(
		logPairs,
		log.String("address", service.address),
		log.Int("pid", service.pid),
	)
	service.dep.Logger.Log(level, subject, pairs...)
}
