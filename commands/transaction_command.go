package commands

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"errors"

	"github.com/go-redis/redis/v8"
)

type TransactionCloseReason string

const (
	TransactionCloseReasonTxClosed                 TransactionCloseReason = "transaction is closed"
	TransactionCloseReasonConnClosed               TransactionCloseReason = "connection is closed"
	TransactionCloseReasonDiscard                  TransactionCloseReason = "execute discard command"
	TransactionCloseReasonUnwatch                  TransactionCloseReason = "execute unwatch command"
	TransactionCloseReasonExec                     TransactionCloseReason = "execute exec command"
	TransactionCloseReasonReset                    TransactionCloseReason = "reset old transaction"
	TransactionCloseReasonResetInWatch             TransactionCloseReason = "reset old transaction in watch command"
	TransactionCloseReasonResetInExec              TransactionCloseReason = "reset old transaction in exec command"
	TransactionCloseReasonWatchedKeysNotInSameSlot TransactionCloseReason = "watched keys not in the same slot"
)

type TransactionStatus string

const (
	TransactionStatusInited  TransactionStatus = "inited"
	TransactionStatusStarted TransactionStatus = "started"
	TransactionStatusClosed  TransactionStatus = "closed"
)

type Transaction struct {
	tx          *redis.Tx
	watchedKeys []string
	keys        []string
	status      TransactionStatus
	commands    []redis.Cmder
}

func NewTransaction() *Transaction {
	return &Transaction{status: TransactionStatusInited}
}

var errTxKeysNotInSameSlot = errors.New("ERR keys in transaction should be in the same slot")

func newRedisTransaction(redisCluster *redis.ClusterClient, keys ...string) (*redis.Tx, error) {
	if len(keys) == 0 {
		return redisCluster.NewTransation(contextTODO, "")
	}
	if !redis.AreKeysInSameSlot(keys...) {
		return nil, errTxKeysNotInSameSlot
	}
	return redisCluster.NewTransation(contextTODO, keys[0])
}

func (transaction *Transaction) multi() RESPData {
	if transaction.isStarted() {
		return RESPData{DataType: ErrorRespType, Value: errors.New("ERR MULTI calls can not be nested")}
	}
	transaction.status = TransactionStatusStarted
	return RESPData{DataType: SimpleStringRespType, Value: "OK"}
}

func (transaction *Transaction) reset(reason TransactionCloseReason, status TransactionStatus) error {
	if transaction.tx != nil {
		if err := transaction.tx.Close(contextTODO); err != nil {
			recordTransactionCloseError(err, reason)
			return err
		}
		transaction.tx = nil
	}
	transaction.watchedKeys = make([]string, 0)
	transaction.keys = make([]string, 0)
	transaction.commands = make([]redis.Cmder, 0)
	transaction.status = status
	return nil
}

func (transaction *Transaction) watch(redisCluster *redis.ClusterClient, keys ...string) RESPData {
	if transaction.isStarted() {
		return RESPData{DataType: ErrorRespType, Value: errors.New("ERR WATCH inside MULTI is not allowed")}
	}
	if len(keys) == 0 {
		return convertErrorToRESPData(newWrongNumberOfArgumentsError("watch"))
	}

	if transaction.tx != nil {
		if len(transaction.watchedKeys) != 0 && !redis.AreKeysInSameSlot(append(transaction.watchedKeys, keys...)...) {
			if err := transaction.reset(TransactionCloseReasonResetInWatch, TransactionStatusInited); err != nil {
				return convertErrorToRESPData(err)
			}
		}
	}

	if transaction.tx == nil {
		tx, err := newRedisTransaction(redisCluster, keys...)
		if err != nil {
			if err == errTxKeysNotInSameSlot {
				transaction.Close(TransactionCloseReasonWatchedKeysNotInSameSlot)
			}
			return convertErrorToRESPData(err)
		}
		transaction.tx = tx
	}

	if _, err := transaction.tx.Watch(contextTODO, keys...).Result(); err != nil {
		return convertErrorToRESPData(err)
	}
	transaction.watchedKeys = append(transaction.watchedKeys, keys...)
	transaction.status = TransactionStatusInited
	return RESPData{DataType: SimpleStringRespType, Value: "OK"}
}

func (transaction *Transaction) addCommand(redisCluster *redis.ClusterClient, command Commander) RESPData {
	var result RESPData
	if transaction.isStarted() {
		transaction.commands = append(transaction.commands, command.Cmd())
		transaction.keys = append(transaction.keys, append(command.ReadKeys(), command.WriteKeys()...)...)
		result = RESPData{DataType: SimpleStringRespType, Value: "QUEUED"}
	} else {
		result = ExecuteCommand(redisCluster, command)
	}
	return result
}

func (transaction *Transaction) exec(redisCluster *redis.ClusterClient) RESPData {
	if !transaction.isStarted() {
		return convertErrorToRESPData(errors.New("ERR EXEC without MULTI"))
	}
	defer func() {
		transaction.Close(TransactionCloseReasonExec)
	}()
	if !redis.AreKeysInSameSlot(transaction.keys...) {
		return convertErrorToRESPData(errTxKeysNotInSameSlot)
	}
	if len(transaction.watchedKeys) != 0 && !redis.AreKeysInSameSlot(append(transaction.keys, transaction.watchedKeys...)...) {
		if transaction.tx != nil {
			if err := transaction.tx.Close(contextTODO); err != nil {
				recordTransactionCloseError(err, TransactionCloseReasonResetInExec)
			}
			transaction.tx = nil
			transaction.watchedKeys = make([]string, 0)
		}
	}

	if transaction.tx == nil {
		tx, err := newRedisTransaction(redisCluster, transaction.keys...)
		if err != nil {
			return convertErrorToRESPData(err)
		}
		transaction.tx = tx
	}

	pipeline := transaction.tx.TxPipeline()
	for _, cmd := range transaction.commands {
		if err := pipeline.Process(contextTODO, cmd); err != nil {
			return convertErrorToRESPData(err)
		}
	}

	commands, err := pipeline.Exec(contextTODO)
	if err != nil {
		return convertErrorToRESPData(err)
	}

	result := RESPData{DataType: ArrayRespType}
	value := make([]RESPData, 0)
	for _, command := range commands {
		r := convertCmdResultToRESPData(command)
		value = append(value, r)
	}
	result.Value = value
	return result
}

func (transaction *Transaction) Close(reason TransactionCloseReason) error {
	if transaction.IsClosed() {
		return nil
	}
	return transaction.reset(reason, TransactionStatusClosed)
}

func (transaction *Transaction) IsClosed() bool {
	return transaction.status == TransactionStatusClosed
}

func (transaction *Transaction) isStarted() bool {
	return transaction.status == TransactionStatusStarted
}

func (transaction *Transaction) Status() TransactionStatus {
	return transaction.status
}

func (transaction *Transaction) discard() RESPData {
	if !transaction.isStarted() {
		return convertErrorToRESPData(errors.New("ERR DISCARD without MULTI"))
	}
	if err := transaction.Close(TransactionCloseReasonDiscard); err != nil {
		return convertErrorToRESPData(err)
	}
	return RESPData{DataType: SimpleStringRespType, Value: "OK"}
}

func (transaction *Transaction) unwatch(redisCluster *redis.ClusterClient) RESPData {
	if transaction.isStarted() {
		command, _ := NewUnwatchCommand([]string{"unwatch"})
		return transaction.addCommand(redisCluster, command)
	}
	if err := transaction.Close(TransactionCloseReasonUnwatch); err != nil {
		return convertErrorToRESPData(err)
	}
	return RESPData{DataType: SimpleStringRespType, Value: "OK"}
}

func (transaction *Transaction) Process(redisCluster *redis.ClusterClient, command Commander) RESPData {
	var result RESPData
	switch command.Name() {
	case "watch":
		result = transaction.watch(redisCluster, command.ReadKeys()...)
	case "multi":
		result = transaction.multi()
	case "exec":
		result = transaction.exec(redisCluster)
	case "discard":
		result = transaction.discard()
	case "unwatch":
		result = transaction.unwatch(redisCluster)
	default:
		result = transaction.addCommand(redisCluster, command)
	}
	return result
}

type WatchCommand struct {
	keys []string
	commonCommand
}

func NewWatchCommand(args []string) (Commander, error) {
	command := &WatchCommand{}
	command.init(args)
	if len(args) < 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.keys = args[1:]
	return command, nil
}

func (command *WatchCommand) Cmd() redis.Cmder {
	return redis.NewStatusCmd(contextTODO, command.argsToInterfaceSlice()...)
}

func (command *WatchCommand) ReadKeys() []string {
	return command.keys
}

func (command *WatchCommand) WriteKeys() []string {
	return []string{}
}

type MultiCommand struct {
	commonCommand
}

func NewMultiCommand(args []string) (Commander, error) {
	command := &MultiCommand{}
	command.init(args)
	if len(args) != 1 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	return command, nil
}

func (command *MultiCommand) Cmd() redis.Cmder {
	return redis.NewStatusCmd(contextTODO, command.name)
}

type ExecCommand struct {
	commonCommand
}

func NewExecCommand(args []string) (Commander, error) {
	command := &ExecCommand{}
	command.init(args)
	if len(args) != 1 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	return command, nil
}

func (command *ExecCommand) Cmd() redis.Cmder {
	return redis.NewCmd(contextTODO, command.name)
}

type DiscardCommand struct {
	commonCommand
}

func NewDiscardCommand(args []string) (Commander, error) {
	command := &DiscardCommand{}
	command.init(args)
	if len(args) != 1 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	return command, nil
}

func (command *DiscardCommand) Cmd() redis.Cmder {
	return redis.NewStatusCmd(contextTODO, command.name)
}

type UnwatchCommand struct {
	commonCommand
}

func NewUnwatchCommand(args []string) (Commander, error) {
	command := &UnwatchCommand{}
	command.init(args)
	if len(args) != 1 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	return command, nil
}

func (command *UnwatchCommand) Cmd() redis.Cmder {
	return redis.NewStatusCmd(contextTODO, command.name)
}

func recordTransactionCloseError(err error, reason TransactionCloseReason) {
	dep := base.GetServerDependency()
	dep.Logger.Error(
		"transaction close error",
		log.String("reason", string(reason)),
		log.Error(err),
	)
	dep.Metric.MetricIncrease("error.transaction.close")
}
