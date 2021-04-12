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
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
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

func StartServer() {
	config := base.GetServerConfig().Server
	logger := base.GetServerLogger()

	logger.Info("starting room server...", log.String("url", config.URL))
	server := redcon.NewServer(config.URL, connServeHandler, connAcceptHandler, connCloseHandler)
	server.AcceptError = connAcceptErrorHandler
	listener, err := greuse.Listen("tcp", config.URL)
	if err != nil {
		logger.Error("start room server failed", log.Error(err))
		panic(err)
	}
	go func() {
		if err := server.Serve(listener); err != nil {
			logger.Error("serve room server failed", log.Error(err))
			panic(err)
		}
	}()

	// start pprof server
	var pprofServer *http.Server
	if config.PProfURL != "" {
		logger.Info("starting pprof server...", log.String("url", config.PProfURL))
		pprofServer = &http.Server{Handler: nil}
		listener, err := greuse.Listen("tcp", config.PProfURL)
		if err != nil {
			logger.Error("start pprof server failed", log.Error(err))
			panic(err)
		}
		go func() {
			if err := pprofServer.Serve(listener); err != nil && err != http.ErrServerClosed {
				logger.Error("serve pprof server failed", log.Error(err))
				panic(err)
			}
		}()
	}

	logger.Info("service has started")

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	sig := <-signalCh
	logger.Info("signal received, closing service...", log.String("signal", sig.String()))
	if err := server.Close(); err != nil {
		logger.Error("close room server error", log.Error(err))
	}
	if pprofServer != nil {
		if err := pprofServer.Close(); err != nil {
			logger.Error("close pprof server error", log.Error(err))
		}
	}
	logger.Info("room server is closed")
}

func connAcceptHandler(conn redcon.Conn) bool {
	metric := base.GetMetricService()
	metric.MetricIncrease("connection.accept")
	metric.MetricGauge("connection.total", atomic.AddInt64(&connectionTotal, 1))
	metric.MetricGauge("transaction.total", transactionManager.transactionCount())
	logger := base.GetServerLogger()
	logger.Debug(
		"accept incomming request",
		log.String("local_addr", conn.NetConn().LocalAddr().String()),
		log.String("remote_addr", conn.RemoteAddr()),
		log.Int("transaction_count", transactionManager.transactionCount()),
	)
	return true
}

func connAcceptErrorHandler(err error) {
	base.GetMetricService().MetricIncrease("error.accept")
	base.GetServerLogger().Error("accept error", log.Error(err))
}

func connServeHandler(conn redcon.Conn, cmd redcon.Command) {
	serveStartTime := time.Now()
	args := make([]string, len(cmd.Args))
	for index, arg := range cmd.Args {
		args[index] = string(arg)
	}

	logger := base.GetServerLogger()
	metric := base.GetMetricService()
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
	if err = preProcessCommand(command); err != nil {
		metric.MetricIncrease("error.pre_process")
		logger.Error(
			"preprocess command error",
			log.String("command", command.String()),
			log.Error(err),
		)
		conn.WriteError(err.Error())
		return
	}

	transaction, err := getTransactionIfNeeded(conn, command)
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
		result = transaction.Process(command)
		if transaction.IsClosed() {
			transactionManager.removeTransaction(conn, "transaction_is_closed")
		}
	} else {
		metric.MetricIncrease("process.single_command")
		result = commands.ExecuteCommand(command)
	}
	writeDataToConnection(conn, result)
	if err := sendCommandEvents(command); err != nil {
		metric.MetricIncrease("error.send_event")
		logger.Error(
			"send event error",
			log.String("command", strings.Join(args, " ")),
			log.Error(err),
		)
	}
	logger.Debug(
		"end to exec command",
		log.String("command", command.String()),
		log.String("result", result.String()),
	)
	duration := time.Since(serveStartTime)
	logger.Info("execute command", log.String("command", command.String()), log.String("duration", duration.String()))
	metric.MetricTimeDuration("process.command.duration", duration)
}

func isTransactionNeeded(command commands.Commander) bool {
	transactionCommands := []string{"watch", "multi"}
	return utility.StringSliceContains(transactionCommands, command.Name())
}

func preProcessCommand(command commands.Commander) error {
	logger := base.GetServerLogger()
	for _, key := range append(command.ReadKeys(), command.WriteKeys()...) {
		if err := preProcessKey(key); err != nil {
			logger.Error(
				"preprocess key error",
				log.String("command", command.String()),
				log.String("key", key),
				log.Error(err),
			)
			return err
		}
	}
	return nil
}

func preProcessKey(key string) error {
	// if !iskeyValid(key) {
	// 	return newInvalidKeyError(key)
	// }
	if err := loadKey(key); err != nil {
		return newLoadError(err)
	}
	return nil
}

func getTransactionIfNeeded(conn redcon.Conn, command commands.Commander) (*commands.Transaction, error) {
	logger := base.GetServerLogger()
	transaction := transactionManager.getTransaction(conn)
	metric := base.GetMetricService()
	if transaction == nil {
		if isTransactionNeeded(command) {
			metric.MetricIncrease("transaction.new")
			tx, err := commands.NewTransaction(append(command.ReadKeys(), command.WriteKeys()...)...)
			if err != nil {
				metric.MetricIncrease("error.transaction.new")
				logger.Error(
					"new transaction error",
					log.String("command", command.String()),
					log.Error(err),
				)
				return nil, err
			}
			transaction = tx
			logger.Debug(
				"create transaction",
				log.String("command", command.Name()),
				log.String("remote_addr", conn.RemoteAddr()),
				log.String("local_addr", conn.NetConn().LocalAddr().String()),
			)
			transactionManager.addTransaction(conn, transaction)
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

func iskeyValid(key string) bool {
	leftBraceIndex := strings.Index(key, "{")
	rightBraceIndex := strings.Index(key, "}")
	return (leftBraceIndex != -1) && (rightBraceIndex != -1) && (leftBraceIndex < rightBraceIndex)
}

func isKeyNeedLoad(key string) (bool, error) {
	redisClient := base.GetRedisCluster()
	commands, err := redisClient.Pipelined(
		context.TODO(),
		func(pipe redis.Pipeliner) error {
			pipe.Exists(context.TODO(), key)
			pipe.HGet(context.TODO(), getMetaKey(key), "loaded")
			return nil
		})
	if err != nil && !errors.Is(err, redis.Nil) {
		return false, newInternalError(err)
	}
	existed := commands[0].(*redis.IntCmd).Val()
	loaded := commands[1].(*redis.StringCmd).Val()
	if existed == 1 {
		// this happens when key is migrated from redis to room
		if loaded != "1" {
			if err := setLoadedMeta(key); err != nil {
				return false, newInternalError(err)
			}
		}
		return false, nil
	}
	return loaded != "1", nil
}

func getMetaKey(key string) string {
	return key + ":_meta"
}

func sendCommandEvents(command commands.Commander) error {
	eventService := base.GetEventService()
	for _, key := range command.ReadKeys() {
		if err := eventService.SendReadEvent(key, time.Now()); err != nil {
			return err
		}
	}
	for _, key := range command.WriteKeys() {
		if err := eventService.SendWriteEvent(key, time.Now()); err != nil {
			return err
		}
	}
	return nil
}

func connCloseHandler(conn redcon.Conn, err error) {
	metric := base.GetMetricService()
	metric.MetricIncrease("connection.close")
	transactionManager.removeTransaction(conn, "close_connection")
	transactionCount := transactionManager.transactionCount()
	logger := base.GetServerLogger()
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
