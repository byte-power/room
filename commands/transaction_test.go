package commands

import (
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func testCloseTransaction(t *testing.T, transactions ...*Transaction) {
	for _, transaction := range transactions {
		err := transaction.Close("")
		assert.Nil(t, err)
	}
}

func TestTransactionInit(t *testing.T) {
	transaction := NewTransaction()
	assert.Equal(t, TransactionStatusInited, transaction.Status())
	testCloseTransaction(t, transaction)
}

// tested commands:
// watch {a}1 {a}2
func TestTransactionWatch(t *testing.T) {
	transaction := NewTransaction()
	keys := []string{"{a}1", "{a}2"}
	command, _ := NewWatchCommand(append([]string{"watch"}, keys...))
	result := transaction.Process(command)
	assert.Equal(t, RESPData{DataType: SimpleStringRespType, Value: "OK"}, result)
	assert.Equal(t, TransactionStatusInited, transaction.Status())
	testCloseTransaction(t, transaction)
}

// tested commands:
// watch {a}1 {b}1
func TestTransactionWatchKeysCrossSlots(t *testing.T) {
	transaction := NewTransaction()
	keys := []string{"{a}1", "{b}1"}
	command, _ := NewWatchCommand(append([]string{"watch"}, keys...))
	result := transaction.Process(command)
	assert.Equal(t, RESPData{DataType: ErrorRespType, Value: errTxKeysNotInSameSlot}, result)
	assert.True(t, transaction.IsClosed(), true)
}

// test commands:
// watch {a}1 {a}2
// watch {a}3 {a}4
// watch {b}1 {b}2
func TestTransactionMultipleWatches(t *testing.T) {
	transaction := NewTransaction()
	keys1 := []string{"{a}1", "{a}2"}
	command, _ := NewWatchCommand(append([]string{"watch"}, keys1...))
	transaction.Process(command)
	assert.Equal(t, transaction.watchedKeys, keys1)

	keys2 := []string{"{a}3", "{a}4"}
	command, _ = NewWatchCommand(append([]string{"watch"}, keys2...))
	transaction.Process(command)
	assert.Equal(t, transaction.watchedKeys, append(keys1, keys2...))

	keys3 := []string{"{b}1", "{b}2"}
	command, _ = NewWatchCommand(append([]string{"watch"}, keys3...))
	transaction.Process(command)
	assert.Equal(t, transaction.watchedKeys, keys3)
	testCloseTransaction(t, transaction)
}

// test commands:
// multi
func TestMulti(t *testing.T) {
	transaction := NewTransaction()
	command, _ := NewMultiCommand([]string{"multi"})
	result := transaction.Process(command)
	assert.Equal(t, RESPData{DataType: SimpleStringRespType, Value: "OK"}, result)
	assert.Equal(t, TransactionStatusStarted, transaction.Status())
	testCloseTransaction(t, transaction)
}

// test commands:
// multi
// watch {a}1 {a}2
func TestWatchInMulti(t *testing.T) {
	transaction := NewTransaction()
	command, _ := NewMultiCommand([]string{"multi"})
	transaction.Process(command)
	keys := []string{"{a}1", "{a}2"}
	command, _ = NewWatchCommand(append([]string{"watch"}, keys...))
	result := transaction.Process(command)
	assert.Equal(t, ErrorRespType, result.DataType)
	testCloseTransaction(t, transaction)
}

// test commands:
// exec
func TestExecWithoutMulti(t *testing.T) {
	transaction := NewTransaction()
	command, _ := NewExecCommand([]string{"exec"})
	result := transaction.Process(command)
	assert.Equal(t, ErrorRespType, result.DataType)
	assert.Equal(t, TransactionStatusInited, transaction.Status())
}

// test commands:
// watch {a}1 {a}1
// exec
func TestExecWithoutMultiAfterWatch(t *testing.T) {
	transaction := NewTransaction()
	watchedKeys := []string{"{a}1", "{a}2"}
	command, _ := NewWatchCommand(append([]string{"watch"}, watchedKeys...))
	transaction.Process(command)

	command, _ = NewExecCommand([]string{"exec"})
	result := transaction.Process(command)
	assert.Equal(t, ErrorRespType, result.DataType)
	assert.Equal(t, TransactionStatusInited, transaction.Status())
	assert.Equal(t, 2, len(transaction.watchedKeys))
	assert.NotNil(t, transaction.tx)
	testCloseTransaction(t, transaction)
}

// test commands:
// watch {a}1 {a}2
// multi
// set {a}1 10
// set {a}2 100
// get {a}1
// get {a}2
// exec
func TestExec(t *testing.T) {
	transaction := NewTransaction()
	watchedKeys := []string{"{a}1", "{a}2"}
	command, _ := NewWatchCommand(append([]string{"watch"}, watchedKeys...))
	transaction.Process(command)

	command, _ = NewMultiCommand([]string{"multi"})
	transaction.Process(command)

	command, _ = NewSetCommand([]string{"set", "{a}1", "10"})
	result := transaction.Process(command)
	assert.Equal(t, RESPData{DataType: SimpleStringRespType, Value: "QUEUED"}, result)

	command, _ = NewSetCommand([]string{"set", "{a}2", "100"})
	result = transaction.Process(command)
	assert.Equal(t, RESPData{DataType: SimpleStringRespType, Value: "QUEUED"}, result)

	command, _ = NewGetCommand([]string{"get", "{a}1"})
	result = transaction.Process(command)
	assert.Equal(t, RESPData{DataType: SimpleStringRespType, Value: "QUEUED"}, result)

	command, _ = NewGetCommand([]string{"get", "{a}2"})
	result = transaction.Process(command)
	assert.Equal(t, RESPData{DataType: SimpleStringRespType, Value: "QUEUED"}, result)

	assert.Equal(t, TransactionStatusStarted, transaction.Status())

	command, _ = NewExecCommand([]string{"exec"})
	result = transaction.Process(command)
	expectedResult := RESPData{
		DataType: ArrayRespType,
		Value: []RESPData{
			{DataType: SimpleStringRespType, Value: "OK"},
			{DataType: SimpleStringRespType, Value: "OK"},
			{DataType: BulkStringRespType, Value: "10"},
			{DataType: BulkStringRespType, Value: "100"},
		},
	}
	assert.Equal(t, expectedResult, result)
	assert.True(t, transaction.IsClosed())
	testEmptyKeysInRedis("{a}1", "{a}2")
}

// test commands:
// tx1: watch {a}1 {a}2
// tx2: set {a}1 a
// tx1: multi
// tx1: set {a}2 b
// tx1: exec

func TestOptimisticLockLost(t *testing.T) {
	tx1 := NewTransaction()
	watchedKeys := []string{"{a}1", "{a}2"}
	command, _ := NewWatchCommand(append([]string{"watch"}, watchedKeys...))
	tx1.Process(command)

	tx2 := NewTransaction()
	command, _ = NewSetCommand([]string{"set", "{a}1", "a"})
	tx2.Process(command)

	command, _ = NewMultiCommand([]string{"multi"})
	tx1.Process(command)
	command, _ = NewSetCommand([]string{"set", "{a}2", "b"})
	result := tx1.Process(command)
	assert.Equal(t, RESPData{DataType: SimpleStringRespType, Value: "QUEUED"}, result)

	command, _ = NewExecCommand([]string{"exec"})
	result = tx1.Process(command)
	assert.Equal(t, RESPData{DataType: ErrorRespType, Value: redis.TxFailedErr}, result)
	assert.True(t, tx1.IsClosed())

	command, _ = NewGetCommand([]string{"get", "{a}1"})
	result = ExecuteCommand(command)
	assert.Equal(t, RESPData{DataType: BulkStringRespType, Value: "a"}, result)

	command, _ = NewGetCommand([]string{"get", "{a}2"})
	result = ExecuteCommand(command)
	assert.Equal(t, RESPData{DataType: NilRespType, Value: nil}, result)

	testCloseTransaction(t, tx1, tx2)
	testEmptyKeysInRedis("{a}1")
}

// test commands:
// multi
// set {a}1 x
// discard

func TestDiscard(t *testing.T) {
	transaction := NewTransaction()
	command, _ := NewMultiCommand([]string{"multi"})
	transaction.Process(command)

	command, _ = NewSetCommand([]string{"set", "{a}1", "x"})
	transaction.Process(command)

	command, _ = NewDiscardCommand([]string{"discard"})
	result := transaction.Process(command)
	assert.Equal(t, RESPData{DataType: SimpleStringRespType, Value: "OK"}, result)

	assert.True(t, transaction.IsClosed())
	assert.Equal(t, 0, len(transaction.watchedKeys))
	assert.Equal(t, 0, len(transaction.keys))
	assert.Equal(t, 0, len(transaction.commands))
	assert.Nil(t, transaction.tx)

	command, _ = NewGetCommand([]string{"get", "{a}1"})
	result = ExecuteCommand(command)
	assert.Equal(t, RESPData{DataType: NilRespType, Value: nil}, result)

}

// test commands:
// watch {a}1 {a}2
// multi
// set {a}1 x
// discard
func TestDiscardWithWatch(t *testing.T) {
	transaction := NewTransaction()
	watchedKeys := []string{"{a}1", "{a}2"}
	command, _ := NewWatchCommand(append([]string{"watch"}, watchedKeys...))
	transaction.Process(command)
	assert.Equal(t, 2, len(watchedKeys))

	command, _ = NewMultiCommand([]string{"multi"})
	transaction.Process(command)

	command, _ = NewSetCommand([]string{"set", "{a}1", "x"})
	transaction.Process(command)

	command, _ = NewDiscardCommand([]string{"discard"})
	result := transaction.Process(command)
	assert.Equal(t, RESPData{DataType: SimpleStringRespType, Value: "OK"}, result)

	assert.True(t, transaction.IsClosed())
	assert.Equal(t, 0, len(transaction.watchedKeys))
	assert.Equal(t, 0, len(transaction.keys))
	assert.Equal(t, 0, len(transaction.commands))
	assert.Nil(t, transaction.tx)

	command, _ = NewGetCommand([]string{"get", "{a}1"})
	result = ExecuteCommand(command)
	assert.Equal(t, RESPData{DataType: NilRespType, Value: nil}, result)
}

// test commands:
// discard

func TestDiscardWithoutMulti(t *testing.T) {
	command, _ := NewDiscardCommand([]string{"discard"})
	result := ExecuteCommand(command)
	assert.Equal(t, ErrorRespType, result.DataType)
}

// test commands:
// watch {a}1 {a}2
// discard
func TestDiscardWithoutMultiAfterWatch(t *testing.T) {
	transaction := NewTransaction()
	watchedKeys := []string{"{a}1", "{a}2"}
	command, _ := NewWatchCommand(append([]string{"watch"}, watchedKeys...))
	transaction.Process(command)

	command, _ = NewDiscardCommand([]string{"discard"})
	result := transaction.Process(command)

	assert.Equal(t, ErrorRespType, result.DataType)
	assert.Equal(t, 2, len(transaction.watchedKeys))
	assert.Equal(t, TransactionStatusInited, transaction.Status())
	assert.NotNil(t, transaction.tx)
	testCloseTransaction(t, transaction)
}

// test commands:
// watch {a}1 {a}2
// unwatch
func TestUnwatch(t *testing.T) {
	transaction := NewTransaction()
	watchedKeys := []string{"{a}1", "{a}2"}
	command, _ := NewWatchCommand(append([]string{"watch"}, watchedKeys...))
	transaction.Process(command)

	command, _ = NewUnwatchCommand([]string{"unwatch"})
	result := transaction.Process(command)
	assert.Equal(t, RESPData{DataType: SimpleStringRespType, Value: "OK"}, result)

	assert.True(t, transaction.IsClosed())
	assert.Equal(t, 0, len(transaction.watchedKeys))
	assert.Nil(t, transaction.tx)
}

// test commands:
// watch {a}1 {a}2
// unwatch
// watch {b}1 {b}2

func TestWatchAfterUnwatch(t *testing.T) {
	transaction := NewTransaction()
	watchedKeys := []string{"{a}1", "{a}2"}
	command, _ := NewWatchCommand(append([]string{"watch"}, watchedKeys...))
	transaction.Process(command)

	command, _ = NewUnwatchCommand([]string{"unwatch"})
	transaction.Process(command)

	watchedKeys2 := []string{"{b}1", "{b}2"}
	command, _ = NewWatchCommand(append([]string{"watch"}, watchedKeys2...))
	result := transaction.Process(command)

	assert.Equal(t, RESPData{DataType: SimpleStringRespType, Value: "OK"}, result)
	assert.Equal(t, TransactionStatusInited, transaction.Status())
	assert.Equal(t, 2, len(transaction.watchedKeys))
	assert.NotNil(t, transaction.tx)
	testCloseTransaction(t, transaction)
}
