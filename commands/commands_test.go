package commands

import (
	"bytepower_room/base"
	"context"
	"log"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestMain(m *testing.M) {
	configFile := "../cmd/config.yaml"
	if err := base.InitBasicDependencies(configFile); err != nil {
		panic(err)
	}
	code := m.Run()
	os.Exit(code)
}

func TestCommands(t *testing.T) {
	for _, testCase := range testCommandCases {
		newFn := supportedCommands[testCase.name]
		command, err := newFn(testCase.args)
		log.Printf("test case: command args: %v\n", testCase.args)
		if testCase.valid {
			assert.Nil(t, err)
			assert.Equal(t, testCase.name, command.Name())
			assert.Equal(t, testCase.writeKeys, command.WriteKeys())
			assert.Equal(t, testCase.readKeys, command.ReadKeys())
			assert.Equal(t, testCase.args, command.Args())
			assert.Equal(t, testCase.accessMode, GetCommnadKeysAccessMode(command))
			assert.IsType(t, testCase.cmdType, command.Cmd())
		} else {
			assert.NotNil(t, err)
			assert.Nil(t, command)
		}
	}
}
func TestExecuteCommands(t *testing.T) {
	for _, testCase := range testExecuteCommandCases {
		log.Printf("start test case: %s\n", testCase.description)
		testCase.prepareFn(testCase.prepareArgs)
		newFn := supportedCommands[testCase.name]
		command, err := newFn(testCase.args)
		assert.Nil(t, err)
		log.Printf("test case: %s, command execution: %s\n", testCase.description, command.String())
		result := ExecuteCommand(command)
		assert.True(t, testCase.compareFn(testCase.respData, result))
		testEmptyKeysInRedis(testCase.emptyKeys...)
	}
}

func testEmptyKeysInRedis(keys ...string) {
	for _, key := range keys {
		base.GetRedisCluster().Del(contextTODO, key)
	}
}

type testCommandCase struct {
	name       string
	args       []string
	writeKeys  []string
	readKeys   []string
	valid      bool
	accessMode base.HashTagAccessMode
	cmdType    redis.Cmder
}

var testCommandCases = []testCommandCase{
	{
		name:       "del",
		args:       []string{"del", "{a}123"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "del",
		args:       []string{"del", "{a}123", "{a}1234"},
		writeKeys:  []string{"{a}123", "{a}1234"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "del",
		args:  []string{"del"},
		valid: false,
	}, {
		name:       "exists",
		args:       []string{"exists", "{a}123", "{a}1234"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}123", "{a}1234"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "exists",
		args:  []string{"exists"},
		valid: false,
	}, {
		name:       "expire",
		args:       []string{"expire", "{a}123", "10"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "expire",
		args:  []string{"expire", "{a}123", "10", "1000"},
		valid: false,
	}, {
		name:  "expire",
		args:  []string{"expire"},
		valid: false,
	}, {
		name:  "expire",
		args:  []string{"expire", "{a}123", "nan"},
		valid: false,
	}, {
		name:       "expireat",
		args:       []string{"expireat", "{a}123", "10"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "expireat",
		args:  []string{"expireat", "{a}123", "10", "1000"},
		valid: false,
	}, {
		name:  "expireat",
		args:  []string{"expireat"},
		valid: false,
	}, {
		name:  "expireat",
		args:  []string{"expireat", "{a}123", "nan"},
		valid: false,
	}, {
		name:       "pexpire",
		args:       []string{"pexpire", "{a}123", "10"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "pexpire",
		args:  []string{"pexpire", "{a}123", "10", "1000"},
		valid: false,
	}, {
		name:  "pexpire",
		args:  []string{"pexpire"},
		valid: false,
	}, {
		name:  "pexpire",
		args:  []string{"pexpire", "{a}123", "nan"},
		valid: false,
	}, {
		name:       "pexpireat",
		args:       []string{"pexpireat", "{a}123", "10"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "pexpireat",
		args:  []string{"expireat", "{a}123", "10", "1000"},
		valid: false,
	}, {
		name:  "pexpireat",
		args:  []string{"pexpireat"},
		valid: false,
	}, {
		name:  "pexpireat",
		args:  []string{"pexpireat", "{a}123", "nan"},
		valid: false,
	}, {
		name:       "persist",
		args:       []string{"persist", "{a}123"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "persist",
		args:  []string{"persist"},
		valid: false,
	}, {
		name:  "persist",
		args:  []string{"persist", "{a}123", "{a}1234"},
		valid: false,
	}, {
		name:       "pttl",
		args:       []string{"pttl", "{a}123"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}123"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "pttl",
		args:  []string{"pttl", "{a}123", "{a}1234"},
		valid: false,
	}, {
		name:  "pttl",
		args:  []string{"pttl"},
		valid: false,
	}, {
		name:       "ttl",
		args:       []string{"ttl", "{a}123"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}123"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "ttl",
		args:  []string{"ttl", "{a}123", "{a}1234"},
		valid: false,
	}, {
		name:  "ttl",
		args:  []string{"ttl"},
		valid: false,
	}, {
		name:       "rename",
		args:       []string{"rename", "{a}123", "{a}1234"},
		writeKeys:  []string{"{a}123", "{a}1234"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StatusCmd{},
	}, {
		name:  "rename",
		args:  []string{"rename", "{a}123", "{a}1234", "{a}12345"},
		valid: false,
	}, {
		name:       "renamenx",
		args:       []string{"renamenx", "{a}123", "{a}1234"},
		writeKeys:  []string{"{a}123", "{a}1234"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "renamenx",
		args:  []string{"renamenx", "{a}123", "{a}1234", "{a}12345"},
		valid: false,
	}, {
		name:       "type",
		args:       []string{"type", "{a}123"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}123"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StatusCmd{},
	}, {
		name:  "type",
		args:  []string{"type"},
		valid: false,
	}, {
		name:  "type",
		args:  []string{"type", "{a}123", "{a}1234"},
		valid: false,
	}, {
		name:       "set",
		args:       []string{"set", "{a}123", "value"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StatusCmd{},
	}, {
		name:       "set",
		args:       []string{"set", "{a}123", "value", "ex", "10"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StatusCmd{},
	}, {
		name:       "set",
		args:       []string{"set", "{a}123", "value", "px", "10"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StatusCmd{},
	}, {
		name:       "set",
		args:       []string{"set", "{a}123", "value", "keepttl"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StatusCmd{},
	}, {
		name:       "set",
		args:       []string{"set", "{a}123", "value", "nx"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StatusCmd{},
	}, {
		name:       "set",
		args:       []string{"set", "{a}123", "value", "xx"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StatusCmd{},
	}, {
		name:       "set",
		args:       []string{"set", "{a}123", "value", "get"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StringCmd{},
	}, {
		name:       "set",
		args:       []string{"set", "{a}123", "value", "ex", "10", "get"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StringCmd{},
	}, {
		name:       "set",
		args:       []string{"set", "{a}123", "value", "ex", "10", "nx", "get"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StringCmd{},
	}, {
		name:  "set",
		args:  []string{"set", "{a}123", "value", "xx", "10", "nx", "get"},
		valid: false,
	}, {
		name:  "set",
		args:  []string{"set", "{a}123", "value", "ex", "nan"},
		valid: false,
	}, {
		name:  "set",
		args:  []string{"set", "{a}123", "value", "keepttl", "100"},
		valid: false,
	}, {
		name:       "get",
		args:       []string{"get", "{a}123"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}123"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringCmd{},
	}, {
		name:  "get",
		args:  []string{"get", "{a}123", "{a}1234"},
		valid: false,
	}, {
		name:  "get",
		args:  []string{"get"},
		valid: false,
	}, {
		name:       "decr",
		args:       []string{"decr", "{a}123"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "decr",
		args:  []string{"decr", "{a}123", "{a}1234"},
		valid: false,
	}, {
		name:  "decr",
		args:  []string{"decr"},
		valid: false,
	}, {
		name:       "decrby",
		args:       []string{"decrby", "{a}123", "10"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "decrby",
		args:  []string{"decrby", "{a}123", "non"},
		valid: false,
	}, {
		name:  "decrby",
		args:  []string{"decrby"},
		valid: false,
	}, {
		name:  "decrby",
		args:  []string{"decrby", "{a}123", "10", "extra"},
		valid: false,
	}, {
		name:       "incr",
		args:       []string{"incr", "{a}123"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "incr",
		args:  []string{"incr", "{a}123", "{a}1234"},
		valid: false,
	}, {
		name:  "incr",
		args:  []string{"incr"},
		valid: false,
	}, {
		name:       "incrby",
		args:       []string{"incrby", "{a}123", "10"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "incrby",
		args:  []string{"incrby", "{a}123", "non"},
		valid: false,
	}, {
		name:  "incrby",
		args:  []string{"incrby"},
		valid: false,
	}, {
		name:  "incrby",
		args:  []string{"incrby", "{a}123", "10", "extra"},
		valid: false,
	}, {
		name:       "incrbyfloat",
		args:       []string{"incrbyfloat", "{a}123", "1.82"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StringCmd{},
	}, {
		name:  "incrbyfloat",
		args:  []string{"incrbyfloat", "{a}123", "not a float"},
		valid: false,
	}, {
		name:       "mget",
		args:       []string{"mget", "{a}123", "{a}1234"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}123", "{a}1234"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.SliceCmd{},
	}, {
		name:  "mget",
		args:  []string{"mget"},
		valid: false,
	}, {
		name:       "setex",
		args:       []string{"setex", "{a}123", "10", "value"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StatusCmd{},
	}, {
		name:  "setex",
		args:  []string{"setex", "{a}123", "nan", "value"},
		valid: false,
	}, {
		name:       "setnx",
		args:       []string{"setnx", "{a}123", "value"},
		writeKeys:  []string{"{a}123"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "setnx",
		args:  []string{"setnx", "{a}123", "value", "extra"},
		valid: false,
	}, {
		name:       "strlen",
		args:       []string{"strlen", "{a}123"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}123"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "strlen",
		args:  []string{"strlen", "{a}123", "{a}1234"},
		valid: false,
	}, {
		name:       "lindex",
		args:       []string{"lindex", "{a}123", "100"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}123"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringCmd{},
	}, {
		name:       "lindex",
		args:       []string{"lindex", "{a}123", "-10"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}123"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringCmd{},
	}, {
		name:  "lindex",
		args:  []string{"lindex", "{a}123", "nan"},
		valid: false,
	}, {
		name:  "lindex",
		args:  []string{"lindex", "{a}123", "10", "extra"},
		valid: false,
	}, {
		name:  "lindex",
		args:  []string{"lindex"},
		valid: false,
	}, {
		name:       "llen",
		args:       []string{"llen", "{a}list"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}list"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "llen",
		args:  []string{"llen", "{a}list", "{a}list2"},
		valid: false,
	}, {
		name:       "lpop",
		args:       []string{"lpop", "{a}list"},
		writeKeys:  []string{"{a}list"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StringCmd{},
	}, {
		name:       "lpos",
		args:       []string{"lpos", "{a}list", "a"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}list"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "lpos",
		args:       []string{"lpos", "{a}list", "a", "count", "10"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}list"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.IntSliceCmd{},
	}, {
		name:       "lpush",
		args:       []string{"lpush", "{a}list", "a", "b"},
		writeKeys:  []string{"{a}list"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "lpush",
		args:       []string{"lpush", "{a}list", "a"},
		writeKeys:  []string{"{a}list"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "lpush",
		args:  []string{"lpush", "{a}list"},
		valid: false,
	}, {
		name:       "lpushx",
		args:       []string{"lpushx", "{a}list", "a", "b"},
		writeKeys:  []string{"{a}list"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "lpushx",
		args:       []string{"lpushx", "{a}list", "a"},
		writeKeys:  []string{"{a}list"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "lpushx",
		args:  []string{"lpushx", "{a}list"},
		valid: false,
	}, {
		name:       "lrange",
		args:       []string{"lrange", "{a}list", "0", "-1"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}list"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:  "lrange",
		args:  []string{"lrange", "{a}list", "0", "nan"},
		valid: false,
	}, {
		name:  "lrange",
		args:  []string{"lrange"},
		valid: false,
	}, {
		name:  "lrange",
		args:  []string{"lrange", "{a}list", "5", "10", "extra"},
		valid: false,
	}, {
		name:       "lrem",
		args:       []string{"lrem", "{a}list", "0", "a"},
		writeKeys:  []string{"{a}list"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "lrem",
		args:       []string{"lrem", "{a}list", "-2", "a"},
		writeKeys:  []string{"{a}list"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "lrem",
		args:  []string{"lrem", "{a}list", "2"},
		valid: false,
	}, {
		name:       "lset",
		args:       []string{"lset", "{a}list", "0", "value"},
		writeKeys:  []string{"{a}list"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StatusCmd{},
	}, {
		name:       "ltrim",
		args:       []string{"ltrim", "{a}list", "0", "2"},
		writeKeys:  []string{"{a}list"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StatusCmd{},
	}, {
		name:       "rpop",
		args:       []string{"rpop", "{a}list"},
		writeKeys:  []string{"{a}list"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StringCmd{},
	}, {
		name:       "rpoplpush",
		args:       []string{"rpoplpush", "{a}list1", "{a}list2"},
		writeKeys:  []string{"{a}list1", "{a}list2"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StringCmd{},
	}, {
		name:       "lmove",
		args:       []string{"lmove", "{a}list1", "{a}list2", "left", "right"},
		writeKeys:  []string{"{a}list1", "{a}list2"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StringCmd{},
	}, {
		name:       "rpush",
		args:       []string{"rpush", "{a}list", "a", "b"},
		writeKeys:  []string{"{a}list"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "rpush",
		args:       []string{"rpush", "{a}list", "a"},
		writeKeys:  []string{"{a}list"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "rpush",
		args:  []string{"rpush", "{a}list"},
		valid: false,
	}, {
		name:       "rpushx",
		args:       []string{"rpushx", "{a}list", "a", "b"},
		writeKeys:  []string{"{a}list"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "rpushx",
		args:       []string{"rpushx", "{a}list", "a"},
		writeKeys:  []string{"{a}list"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "rpushx",
		args:  []string{"rpushx", "{a}list"},
		valid: false,
	}, {
		name:       "sadd",
		args:       []string{"sadd", "{a}set", "a", "b", "c"},
		writeKeys:  []string{"{a}set"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "sadd",
		args:  []string{"sadd"},
		valid: false,
	}, {
		name:       "scard",
		args:       []string{"scard", "{a}set"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}set"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "scard",
		args:  []string{"scard", "{a}", "{a}1"},
		valid: false,
	}, {
		name:  "scard",
		args:  []string{"scard"},
		valid: false,
	}, {
		name:       "sdiff",
		args:       []string{"sdiff", "{a}set1", "{a}set2", "{a}set3"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}set1", "{a}set2", "{a}set3"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "sdiffstore",
		args:       []string{"sdiffstore", "{a}set1", "{a}set2", "{a}set3"},
		writeKeys:  []string{"{a}set1"},
		readKeys:   []string{"{a}set2", "{a}set3"},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "sinter",
		args:       []string{"sinter", "{a}set1", "{a}set2", "{a}set3"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}set1", "{a}set2", "{a}set3"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "sinterstore",
		args:       []string{"sinterstore", "{a}set1", "{a}set2", "{a}set3"},
		writeKeys:  []string{"{a}set1"},
		readKeys:   []string{"{a}set2", "{a}set3"},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "sismember",
		args:       []string{"sismember", "{a}set1", "a"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}set1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "smismember",
		args:       []string{"smismember", "{a}set1", "a", "b", "c"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}set1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.IntSliceCmd{},
	}, {
		name:       "smembers",
		args:       []string{"smembers", "{a}set1"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}set1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "smove",
		args:       []string{"smove", "{a}set1", "{a}set2", "value"},
		writeKeys:  []string{"{a}set1", "{a}set2"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "spop",
		args:       []string{"spop", "{a}set1"},
		writeKeys:  []string{"{a}set1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StringCmd{},
	}, {
		name:       "spop",
		args:       []string{"spop", "{a}set1", "10"},
		writeKeys:  []string{"{a}set1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "srandmember",
		args:       []string{"srandmember", "{a}set1"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}set1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringCmd{},
	}, {
		name:       "srandmember",
		args:       []string{"srandmember", "{a}set1", "10"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}set1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "srem",
		args:       []string{"srem", "{a}set1", "a", "b"},
		writeKeys:  []string{"{a}set1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "sunion",
		args:       []string{"sunion", "{a}set1", "{a}set2", "{a}set3"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}set1", "{a}set2", "{a}set3"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "sunionstore",
		args:       []string{"sunionstore", "{a}set1", "{a}set2", "{a}set3"},
		writeKeys:  []string{"{a}set1"},
		readKeys:   []string{"{a}set2", "{a}set3"},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "hdel",
		args:       []string{"hdel", "{a}hash1", "a", "b"},
		writeKeys:  []string{"{a}hash1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "hdel",
		args:  []string{"hdel", "{a}hash1"},
		valid: false,
	}, {
		name:       "hexists",
		args:       []string{"hexists", "{a}hash1", "a"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}hash1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "hget",
		args:       []string{"hget", "{a}hash1", "a"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}hash1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringCmd{},
	}, {
		name:       "hgetall",
		args:       []string{"hgetall", "{a}hash1"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}hash1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "hincrby",
		args:       []string{"hincrby", "{a}hash1", "a", "10"},
		writeKeys:  []string{"{a}hash1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "hincrby",
		args:  []string{"hincrby", "{a}hash1", "a", "non"},
		valid: false,
	}, {
		name:  "hincrby",
		args:  []string{"hincrby"},
		valid: false,
	}, {
		name:  "hincrby",
		args:  []string{"hincrby", "{a}123", "a"},
		valid: false,
	}, {
		name:       "hincrbyfloat",
		args:       []string{"hincrbyfloat", "{a}hash1", "a", "10.5"},
		writeKeys:  []string{"{a}hash1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StringCmd{},
	}, {
		name:  "hincrbyfloat",
		args:  []string{"hincrbyfloat", "{a}hash1", "a", "non"},
		valid: false,
	}, {
		name:  "hincrbyfloat",
		args:  []string{"hincrbyfloat"},
		valid: false,
	}, {
		name:  "hincrbyfloat",
		args:  []string{"hincrbyfloat", "{a}123", "a"},
		valid: false,
	}, {
		name:       "hkeys",
		args:       []string{"hkeys", "{a}hash1"},
		readKeys:   []string{"{a}hash1"},
		writeKeys:  []string{},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "hlen",
		args:       []string{"hlen", "{a}hash1"},
		readKeys:   []string{"{a}hash1"},
		writeKeys:  []string{},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "hmget",
		args:       []string{"hmget", "{a}hash1", "a", "b"},
		readKeys:   []string{"{a}hash1"},
		writeKeys:  []string{},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.SliceCmd{},
	}, {
		name:  "hmget",
		args:  []string{"hmget", "{a}hash1"},
		valid: false,
	}, {
		name:       "hmset",
		args:       []string{"hmset", "{a}hash1", "a", "b", "c", "d"},
		writeKeys:  []string{"{a}hash1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StatusCmd{},
	}, {
		name:  "hmset",
		args:  []string{"hmset", "{a}hash1", "a", "b", "c", "d", "e"},
		valid: false,
	}, {
		name:       "hset",
		args:       []string{"hset", "{a}hash1", "a", "b"},
		writeKeys:  []string{"{a}hash1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "hset",
		args:       []string{"hset", "{a}hash1", "a", "b", "x", "y"},
		writeKeys:  []string{"{a}hash1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "hset",
		args:  []string{"hset", "{a}hash1", "a", "b", "x", "y", "c"},
		valid: false,
	}, {
		name:       "hsetnx",
		args:       []string{"hsetnx", "{a}hash1", "a", "b"},
		writeKeys:  []string{"{a}hash1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:  "hsetnx",
		args:  []string{"hsetnx", "{a}hash1", "a", "b", "c", "d"},
		valid: false,
	}, {
		name:       "hstrlen",
		args:       []string{"hstrlen", "{a}hash1", "a"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}hash1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "hvals",
		args:       []string{"hvals", "{a}hash1"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}hash1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zadd",
		args:       []string{"zadd", "{a}zset1", "0.5", "a", "0.6", "b"},
		writeKeys:  []string{"{a}zset1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "zadd",
		args:       []string{"zadd", "{a}zset1", "ch", "0.5", "a", "0.6", "b"},
		writeKeys:  []string{"{a}zset1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "zadd",
		args:       []string{"zadd", "{a}zset1", "gt", "0.5", "a", "0.6", "b"},
		writeKeys:  []string{"{a}zset1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "zadd",
		args:       []string{"zadd", "{a}zset1", "lt", "0.5", "a", "0.6", "b"},
		writeKeys:  []string{"{a}zset1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "zadd",
		args:       []string{"zadd", "{a}zset1", "nx", "0.5", "a", "0.6", "b"},
		writeKeys:  []string{"{a}zset1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "zadd",
		args:       []string{"zadd", "{a}zset1", "xx", "0.5", "a", "0.6", "b"},
		writeKeys:  []string{"{a}zset1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "zadd",
		args:       []string{"zadd", "{a}zset1", "incr", "0.5", "a"},
		writeKeys:  []string{"{a}zset1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StringCmd{},
	}, {
		name:       "zcard",
		args:       []string{"zcard", "{a}zset1"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "zcount",
		args:       []string{"zcount", "{a}zset1", "1.5", "2.8"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "zdiff",
		args:       []string{"zdiff", "3", "{a}zset1", "{a}zset2", "{a}zset3"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1", "{a}zset2", "{a}zset3"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zdiff",
		args:       []string{"zdiff", "3", "{a}zset1", "{a}zset2", "{a}zset3", "withscores"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1", "{a}zset2", "{a}zset3"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zdiffstore",
		args:       []string{"zdiffstore", "{a}zset0", "3", "{a}zset1", "{a}zset2", "{a}zset3"},
		writeKeys:  []string{"{a}zset0"},
		readKeys:   []string{"{a}zset1", "{a}zset2", "{a}zset3"},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zincrby",
		args:       []string{"zincrby", "{a}zset1", "10.5", "a"},
		writeKeys:  []string{"{a}zset1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StringCmd{},
	}, {
		name:       "zlexcount",
		args:       []string{"zlexcount", "{a}zset1", "a", "b"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "zpopmax",
		args:       []string{"zpopmax", "{a}zset1"},
		writeKeys:  []string{"{a}zset1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zpopmax",
		args:       []string{"zpopmax", "{a}zset1", "10"},
		writeKeys:  []string{"{a}zset1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zpopmin",
		args:       []string{"zpopmin", "{a}zset1"},
		writeKeys:  []string{"{a}zset1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zpopmin",
		args:       []string{"zpopmin", "{a}zset1", "10"},
		writeKeys:  []string{"{a}zset1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zrange",
		args:       []string{"zrange", "{a}zset1", "0", "10"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zrange",
		args:       []string{"zrange", "{a}zset1", "0", "10", "withscores"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
		// TODO: other options available in 6.2.
		// 	}, {
		// 		name:      "zrange",
		// 		args:      []string{"zrange", "{a}zset1", "byscore", "1.2", "10.8", "withscores"},
		// 		writeKeys: []string{},
		// 		readKeys:  []string{"{a}zset1"},
		// 		valid:     true,
		// 		cmdType:   &redis.StringSliceCmd{},
		// 	}, {
		// 		name:      "zrange",
		// 		args:      []string{"zrange", "{a}zset1", "bylex", "(a", "(b", "withscores"},
		// 		writeKeys: []string{},
		// 		readKeys:  []string{"{a}zset1"},
		// 		valid:     true,
		// 		cmdType:   &redis.StringSliceCmd{},
		// 	}, {
		// 		name:      "zrange",
		// 		args:      []string{"zrange", "{a}zset1", "rev", "(a", "(b", "withscores"},
		// 		writeKeys: []string{},
		// 		readKeys:  []string{"{a}zset1"},
		// 		valid:     true,
		// 		cmdType:   &redis.StringSliceCmd{},
		// 	}, {
		// 		name:      "zrange",
		// 		args:      []string{"zrange", "{a}zset1", "rev", "1", "10", "withscores"},
		// 		writeKeys: []string{},
		// 		readKeys:  []string{"{a}zset1"},
		// 		valid:     true,
		// 		cmdType:   &redis.StringSliceCmd{},
		// 	}, {
		// 		name:      "zrange",
		// 		args:      []string{"zrange", "{a}zset1", "rev", "1", "10", "limit", "2", "5", "withscores"},
		// 		writeKeys: []string{},
		// 		readKeys:  []string{"{a}zset1"},
		// 		valid:     true,
		// 		cmdType:   &redis.StringSliceCmd{},
	}, {
		name:       "zrangebylex",
		args:       []string{"zrangebylex", "{a}zset1", "(a", "(b"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zrangebylex",
		args:       []string{"zrangebylex", "{a}zset1", "(a", "(b", "limit", "0", "10"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zrevrangebylex",
		args:       []string{"zrevrangebylex", "{a}zset1", "(a", "(b"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zrevrangebylex",
		args:       []string{"zrevrangebylex", "{a}zset1", "(a", "(b", "limit", "0", "10"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zrangebyscore",
		args:       []string{"zrangebyscore", "{a}zset1", "(1", "5"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zrangebyscore",
		args:       []string{"zrangebyscore", "{a}zset1", "(1", "5", "withscores"},
		readKeys:   []string{"{a}zset1"},
		writeKeys:  []string{},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zrangebyscore",
		args:       []string{"zrangebyscore", "{a}zset1", "(1", "5", "limit", "2", "5"},
		readKeys:   []string{"{a}zset1"},
		writeKeys:  []string{},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zrank",
		args:       []string{"zrank", "{a}zset1", "a"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "zrem",
		args:       []string{"zrem", "{a}zset1", "a", "b"},
		writeKeys:  []string{"{a}zset1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "zremrangebylex",
		args:       []string{"zremrangebylex", "{a}zset1", "(a", "[b"},
		writeKeys:  []string{"{a}zset1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "zremrangebyrank",
		args:       []string{"zremrangebyrank", "{a}zset1", "0", "10"},
		writeKeys:  []string{"{a}zset1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "zremrangebyscore",
		args:       []string{"zremrangebyscore", "{a}zset1", "-inf", "(2"},
		writeKeys:  []string{"{a}zset1"},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeWrite,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "zrevrange",
		args:       []string{"zrevrange", "{a}zset1", "-2", "-1"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zrevrange",
		args:       []string{"zrevrange", "{a}zset1", "-2", "-1", "withscores"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zrevrangebyscore",
		args:       []string{"zrevrangebyscore", "{a}zset1", "(2", "(1"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zrevrangebyscore",
		args:       []string{"zrevrangebyscore", "{a}zset1", "(2", "(1", "withscores"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringSliceCmd{},
	}, {
		name:       "zrevrank",
		args:       []string{"zrevrank", "{a}zset1", "a"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.IntCmd{},
	}, {
		name:       "zscore",
		args:       []string{"zscore", "{a}zset1", "a"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StringCmd{},
	}, {
		name:       "zmscore",
		args:       []string{"zmscore", "{a}zset1", "a", "b", "c"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}zset1"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.SliceCmd{},
	}, {
		name:       "watch",
		args:       []string{"watch", "{a}", "{a}1", "{a}2"},
		writeKeys:  []string{},
		readKeys:   []string{"{a}", "{a}1", "{a}2"},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StatusCmd{},
	}, {
		name:       "unwatch",
		args:       []string{"unwatch"},
		writeKeys:  []string{},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StatusCmd{},
	}, {
		name:       "discard",
		args:       []string{"discard"},
		writeKeys:  []string{},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StatusCmd{},
	}, {
		name:       "exec",
		args:       []string{"exec"},
		writeKeys:  []string{},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.Cmd{},
	}, {
		name:       "multi",
		args:       []string{"multi"},
		writeKeys:  []string{},
		readKeys:   []string{},
		accessMode: base.HashTagAccessModeRead,
		valid:      true,
		cmdType:    &redis.StatusCmd{},
	},
}

type testExecuteCommandCase struct {
	name        string
	description string
	prepareFn   func(interface{})
	prepareArgs interface{}
	args        []string
	respData    RESPData
	compareFn   func(RESPData, RESPData) bool
	emptyKeys   []string
}

var testExecuteCommandCases = []testExecuteCommandCase{
	{
		name:        "del",
		description: "del two keys",
		prepareFn:   testNewStringKeys,
		prepareArgs: []string{"{a}123", "{a}1234"},
		args:        []string{"del", "{a}123", "{a}1234"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(2)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123", "{a}1234"},
	}, {
		name:        "exists",
		description: "exists two existed keys",
		prepareFn:   testNewStringKeys,
		prepareArgs: []string{"{a}123", "{a}1234"},
		args:        []string{"exists", "{a}123", "{a}1234"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(2)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123", "{a}1234"},
	}, {
		name:        "exists",
		description: "exists two keys, 1 existed, 1 not",
		prepareFn:   testNewStringKeys,
		prepareArgs: []string{"{a}123"},
		args:        []string{"exists", "{a}123", "{a}1234"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(1)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "exists",
		description: "exists two non-existed keys",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"exists", "{a}123", "{a}1234"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{},
	}, {
		name:        "expire",
		description: "expire key",
		prepareFn:   testNewStringKeys,
		prepareArgs: []string{"{a}123"},
		args:        []string{"expire", "{a}123", "10"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(1)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "expire",
		description: "expire not existed key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"expire", "{a}123", "10"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{},
	}, {
		name:        "expireat",
		description: "expireat key",
		prepareFn:   testNewStringKeys,
		prepareArgs: []string{"{a}123"},
		args:        []string{"expireat", "{a}123", "1234567"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(1)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "expireat",
		description: "expireat not existed key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"expireat", "{a}123", "10"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{},
	}, {
		name:        "persist",
		description: "persist not existed key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"persist", "{a}123"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{},
	}, {
		name:        "persist",
		description: "persist a persisted key",
		prepareFn:   testNewStringKeys,
		prepareArgs: []string{"{a}123"},
		args:        []string{"persist", "{a}123"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "persist",
		description: "persist a key with expiration",
		prepareFn:   testNewStringKeyWithExpiration,
		prepareArgs: []interface{}{"{a}123", time.Duration(100) * time.Second},
		args:        []string{"persist", "{a}123"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(1)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "pexpire",
		description: "pexpire key",
		prepareFn:   testNewStringKeys,
		prepareArgs: []string{"{a}123"},
		args:        []string{"pexpire", "{a}123", "10000000"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(1)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "pexpire",
		description: "pexpire not existed key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"pexpire", "{a}123", "100000"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{},
	}, {
		name:        "pexpireat",
		description: "pexpireat key",
		prepareFn:   testNewStringKeys,
		prepareArgs: []string{"{a}123"},
		args:        []string{"pexpireat", "{a}123", "10000000"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(1)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "pexpireat",
		description: "pexpireat not existed key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"pexpireat", "{a}123", "100000"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{},
	}, {
		name:        "pttl",
		description: "pttl an existed key without expiration date",
		prepareFn:   testNewStringKeys,
		prepareArgs: []string{"{a}123"},
		args:        []string{"pttl", "{a}123"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(-1)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{},
	}, {
		name:        "pttl",
		description: "pttl an existed key with expiration date",
		prepareFn:   testNewStringKeyWithExpiration,
		prepareArgs: []interface{}{"{a}123", 10 * time.Second},
		args:        []string{"pttl", "{a}123"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareIntGreaterThan,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "ttl",
		description: "ttl an existed key without expiration date",
		prepareFn:   testNewStringKeys,
		prepareArgs: []string{"{a}123"},
		args:        []string{"ttl", "{a}123"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(-1)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{},
	}, {
		name:        "ttl",
		description: "ttl an existed key with expiration date",
		prepareFn:   testNewStringKeyWithExpiration,
		prepareArgs: []interface{}{"{a}123", 10 * time.Second},
		args:        []string{"ttl", "{a}123"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareIntGreaterThan,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "rename",
		description: "rename a key",
		prepareFn:   testNewStringKeys,
		prepareArgs: []string{"{a}123"},
		args:        []string{"rename", "{a}123", "{a}1234"},
		respData:    RESPData{DataType: SimpleStringRespType, Value: "OK"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123", "{a}1234"},
	}, {
		name:        "rename",
		description: "rename an non-existed key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"rename", "{a}123", "{a}1234"},
		respData:    RESPData{DataType: ErrorRespType, Value: nil},
		compareFn:   testIsErrorType,
		emptyKeys:   []string{},
	}, {
		name:        "type",
		description: "type a string key",
		prepareFn:   testNewStringKeys,
		prepareArgs: []string{"{a}123"},
		args:        []string{"type", "{a}123"},
		respData:    RESPData{DataType: SimpleStringRespType, Value: "string"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "type",
		description: "type a list key",
		prepareFn:   testNewListKey,
		prepareArgs: []interface{}{"{a}123", "a", "b", "c"},
		args:        []string{"type", "{a}123"},
		respData:    RESPData{DataType: SimpleStringRespType, Value: "list"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "type",
		description: "type an non-existed key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"type", "{a}123"},
		respData:    RESPData{DataType: SimpleStringRespType, Value: "none"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{},
	}, {
		name:        "set",
		description: "set a key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"set", "{a}123", "value"},
		respData:    RESPData{DataType: SimpleStringRespType, Value: "OK"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "set",
		description: "set a key with expiration",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"set", "{a}123", "value", "ex", "10"},
		respData:    RESPData{DataType: SimpleStringRespType, Value: "OK"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "set",
		description: "set nx a key when it is not existed",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"set", "{a}123", "value", "nx"},
		respData:    RESPData{DataType: SimpleStringRespType, Value: "OK"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "set",
		description: "set nx a key when it is existed",
		prepareFn:   testNewStringKeys,
		prepareArgs: []string{"{a}123"},
		args:        []string{"set", "{a}123", "value", "nx"},
		respData:    RESPData{DataType: NilRespType, Value: nil},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "set",
		description: "set xx a key when it is not existed",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"set", "{a}123", "value", "xx"},
		respData:    RESPData{DataType: NilRespType, Value: nil},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "set",
		description: "set xx a key when it is existed",
		prepareFn:   testNewStringKeys,
		prepareArgs: []string{"{a}123"},
		args:        []string{"set", "{a}123", "value", "xx"},
		respData:    RESPData{DataType: SimpleStringRespType, Value: "OK"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "get",
		description: "get an existed key",
		prepareFn:   testNewStringKeys,
		prepareArgs: []string{"{a}123"},
		args:        []string{"get", "{a}123"},
		respData:    RESPData{DataType: BulkStringRespType, Value: "{a}123"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "get",
		description: "get a non existed key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"get", "{a}123"},
		respData:    RESPData{DataType: NilRespType, Value: nil},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{},
	}, {
		name:        "decr",
		description: "decr a key",
		prepareFn:   testNewStringKeyValue,
		prepareArgs: []string{"{a}123", "10"},
		args:        []string{"decr", "{a}123"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(9)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "decr",
		description: "decr a key with no int value",
		prepareFn:   testNewStringKeyValue,
		prepareArgs: []string{"{a}123", "abc"},
		args:        []string{"decr", "{a}123"},
		respData:    RESPData{DataType: ErrorRespType, Value: nil},
		compareFn:   testIsErrorType,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "decr",
		description: "decr a non existed key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"decr", "{a}123"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(-1)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "decrby",
		description: "decrby a key",
		prepareFn:   testNewStringKeyValue,
		prepareArgs: []string{"{a}123", "10"},
		args:        []string{"decrby", "{a}123", "8"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(2)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "decrby",
		description: "decrby a key with no int value",
		prepareFn:   testNewStringKeyValue,
		prepareArgs: []string{"{a}123", "abc"},
		args:        []string{"decrby", "{a}123", "8"},
		respData:    RESPData{DataType: ErrorRespType, Value: nil},
		compareFn:   testIsErrorType,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "decrby",
		description: "decrby a non existed key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"decrby", "{a}123", "7"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(-7)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "incr",
		description: "incr a key",
		prepareFn:   testNewStringKeyValue,
		prepareArgs: []string{"{a}123", "10"},
		args:        []string{"incr", "{a}123"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(11)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "incr",
		description: "incr a key with no int value",
		prepareFn:   testNewStringKeyValue,
		prepareArgs: []string{"{a}123", "abc"},
		args:        []string{"incr", "{a}123"},
		respData:    RESPData{DataType: ErrorRespType, Value: nil},
		compareFn:   testIsErrorType,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "incr",
		description: "incr a non existed key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"incr", "{a}123"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(1)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "incrby",
		description: "incrby a key",
		prepareFn:   testNewStringKeyValue,
		prepareArgs: []string{"{a}123", "10"},
		args:        []string{"incrby", "{a}123", "8"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(18)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "incrby",
		description: "incrby a key with no int value",
		prepareFn:   testNewStringKeyValue,
		prepareArgs: []string{"{a}123", "abc"},
		args:        []string{"incrby", "{a}123", "8"},
		respData:    RESPData{DataType: ErrorRespType, Value: nil},
		compareFn:   testIsErrorType,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "incrby",
		description: "incrby a non existed key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"incrby", "{a}123", "7"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(7)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "incrbyfloat",
		description: "incrbyfloat a key",
		prepareFn:   testNewStringKeyValue,
		prepareArgs: []string{"{a}123", "1.2"},
		args:        []string{"incrbyfloat", "{a}123", "1.5"},
		respData:    RESPData{DataType: BulkStringRespType, Value: "2.7"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "mget",
		description: "mget keys",
		prepareFn:   testNewStringKeys,
		prepareArgs: []string{"{a}123", "{a}1234"},
		args:        []string{"mget", "{a}123", "{a}1234"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{
				{
					DataType: BulkStringRespType,
					Value:    "{a}123",
				}, {
					DataType: BulkStringRespType,
					Value:    "{a}1234",
				},
			},
		},
		compareFn: testCompareEqual,
		emptyKeys: []string{"{a}123", "{a}1234"},
	}, {
		name:        "mget",
		description: "mget keys with not existed keys",
		prepareFn:   testNewStringKeys,
		prepareArgs: []string{"{a}123"},
		args:        []string{"mget", "{a}123", "{a}1234"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{
				{
					DataType: BulkStringRespType,
					Value:    "{a}123",
				}, {
					DataType: NilRespType,
					Value:    nil,
				},
			},
		},
		compareFn: testCompareEqual,
		emptyKeys: []string{"{a}123"},
	}, {
		name:        "setex",
		description: "setex a key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"setex", "{a}123", "100", "value"},
		respData:    RESPData{DataType: SimpleStringRespType, Value: "OK"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "setnx",
		description: "setnx a key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"setnx", "{a}123", "value"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(1)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "setnx",
		description: "setnx an existed key",
		prepareFn:   testNewStringKeys,
		prepareArgs: []string{"{a}123"},
		args:        []string{"setnx", "{a}123", "value"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "strlen",
		description: "strlen a key",
		prepareFn:   testNewStringKeyValue,
		prepareArgs: []string{"{a}123", "avalue"},
		args:        []string{"strlen", "{a}123"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(6)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}123"},
	}, {
		name:        "strlen",
		description: "strlen a non-existed key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"strlen", "{a}123"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{},
	}, {
		name:        "lindex",
		description: "lindex a list key",
		prepareFn:   testNewListKey,
		prepareArgs: []interface{}{"{a}list1", "x", "y", "z"},
		args:        []string{"lindex", "{a}list1", "2"},
		respData:    RESPData{DataType: BulkStringRespType, Value: "z"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}list1"},
	}, {
		name:        "lindex",
		description: "lindex a list key with negative index",
		prepareFn:   testNewListKey,
		prepareArgs: []interface{}{"{a}list1", "x", "y", "z"},
		args:        []string{"lindex", "{a}list1", "-2"},
		respData:    RESPData{DataType: BulkStringRespType, Value: "y"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}list1"},
	}, {
		name:        "llen",
		description: "llen a list key",
		prepareFn:   testNewListKey,
		prepareArgs: []interface{}{"{a}list1", "x", "y", "z"},
		args:        []string{"llen", "{a}list1"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(3)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}list1"},
	}, {
		name:        "llen",
		description: "llen a non existed key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"llen", "{a}list1"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{},
	}, {
		name:        "lpop",
		description: "lpop a list key",
		prepareFn:   testNewListKey,
		prepareArgs: []interface{}{"{a}list1", "x", "y", "z"},
		args:        []string{"lpop", "{a}list1"},
		respData:    RESPData{DataType: BulkStringRespType, Value: "x"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}list1"},
	}, {
		name:        "lpush",
		description: "lpush a list key",
		prepareFn:   testNewListKey,
		prepareArgs: []interface{}{"{a}list1", "x", "y", "z"},
		args:        []string{"lpush", "{a}list1", "a", "b"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(5)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}list1"},
	}, {
		name:        "lpush",
		description: "lpush a non existed list key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"lpush", "{a}list1", "a", "b"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(2)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}list1"},
	}, {
		name:        "lpushx",
		description: "lpushx a list key",
		prepareFn:   testNewListKey,
		prepareArgs: []interface{}{"{a}list1", "x", "y", "z"},
		args:        []string{"lpushx", "{a}list1", "a", "b"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(5)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}list1"},
	}, {
		name:        "lpushx",
		description: "lpushx a non existed list key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"lpushx", "{a}list1", "a", "b"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{""},
	}, {
		name:        "lrange",
		description: "lrange list key from 0 to -1",
		prepareFn:   testNewListKey,
		prepareArgs: []interface{}{"{a}list1", "a", "b", "c", "d"},
		args:        []string{"lrange", "{a}list1", "0", "-1"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{
				{
					DataType: BulkStringRespType,
					Value:    "a",
				}, {
					DataType: BulkStringRespType,
					Value:    "b",
				}, {
					DataType: BulkStringRespType,
					Value:    "c",
				}, {
					DataType: BulkStringRespType,
					Value:    "d",
				}},
		},
		compareFn: testCompareEqual,
		emptyKeys: []string{"{a}list1"},
	}, {
		name:        "lrange",
		description: "lrange list key from 1 to 2",
		prepareFn:   testNewListKey,
		prepareArgs: []interface{}{"{a}list1", "a", "b", "c", "d"},
		args:        []string{"lrange", "{a}list1", "1", "2"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{
				{
					DataType: BulkStringRespType,
					Value:    "b",
				}, {
					DataType: BulkStringRespType,
					Value:    "c",
				}},
		},
		compareFn: testCompareEqual,
		emptyKeys: []string{"{a}list1"},
	}, {
		name:        "lrem",
		description: "lrem a list key",
		prepareFn:   testNewListKey,
		prepareArgs: []interface{}{"{a}list1", "a", "b", "a", "a", "c"},
		args:        []string{"lrem", "{a}list1", "0", "a"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(3)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}list1"},
	}, {
		name:        "lrem",
		description: "lrem a list key with negative count",
		prepareFn:   testNewListKey,
		prepareArgs: []interface{}{"{a}list1", "a", "b", "a", "a", "c"},
		args:        []string{"lrem", "{a}list1", "-2", "a"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(2)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}list1"},
	}, {
		name:        "rpop",
		description: "rpop a list key",
		prepareFn:   testNewListKey,
		prepareArgs: []interface{}{"{a}list1", "x", "y", "z"},
		args:        []string{"rpop", "{a}list1"},
		respData:    RESPData{DataType: BulkStringRespType, Value: "z"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}list1"},
	}, {
		name:        "rpush",
		description: "rpush a list key",
		prepareFn:   testNewListKey,
		prepareArgs: []interface{}{"{a}list1", "x", "y", "z"},
		args:        []string{"rpush", "{a}list1", "a", "b"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(5)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}list1"},
	}, {
		name:        "rpush",
		description: "rpush a non existed list key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"rpush", "{a}list1", "a", "b"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(2)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}list1"},
	}, {
		name:        "rpushx",
		description: "rpushx a list key",
		prepareFn:   testNewListKey,
		prepareArgs: []interface{}{"{a}list1", "x", "y", "z"},
		args:        []string{"rpushx", "{a}list1", "a", "b"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(5)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}list1"},
	}, {
		name:        "rpushx",
		description: "rpushx a non existed list key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"rpushx", "{a}list1", "a", "b"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{},
	}, {
		name:        "sadd",
		description: "sadd a set key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"sadd", "{a}set1", "a", "b", "a"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(2)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}set1"},
	}, {
		name:        "scard",
		description: "scard a set key",
		prepareFn:   testNewSetKey,
		prepareArgs: []interface{}{"{a}set1", "a", "b", "c", "d"},
		args:        []string{"scard", "{a}set1"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(4)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}set1"},
	}, {
		name:        "sismember",
		description: "sismember a set key",
		prepareFn:   testNewSetKey,
		prepareArgs: []interface{}{"{a}set1", "x", "y", "z"},
		args:        []string{"sismember", "{a}set1", "x"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(1)},
		compareFn:   testCompareEqual,
	}, {
		name:        "sismember",
		description: "sismember a set key with a non existed element",
		prepareFn:   testNewSetKey,
		prepareArgs: []interface{}{"{a}set1", "x", "y", "z"},
		args:        []string{"sismember", "{a}set1", "a"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}set1"},
	}, {
		// TODO:  available since redis 6.2.0
		// 	name:        "smismember",
		// 	description: "smismember a set key",
		// 	prepareFn:   testNewSetKey,
		// 	prepareArgs: []interface{}{"{a}set1", "x", "y", "z"},
		// 	args:        []string{"smismember", "{a}set1", "a", "x", "b", "z"},
		// 	respData: RESPData{
		// 		DataType: ArrayRespType,
		// 		Value: []RESPData{{
		// 			DataType: IntegerRespType,
		// 			Value:    int64(0),
		// 		}, {
		// 			DataType: IntegerRespType,
		// 			Value:    int64(1),
		// 		}, {
		// 			DataType: IntegerRespType,
		// 			Value:    int64(0),
		// 		}, {
		// 			DataType: IntegerRespType,
		// 			Value:    int64(1),
		// 		}}},
		// 	compareFn: testCompareEqual,
		// 	emptyKeys: []string{"{a}set1"},
		// }, {
		name:        "smembers",
		description: "smembers set key",
		prepareFn:   testNewSetKey,
		prepareArgs: []interface{}{"{a}set1", "a", "b", "c", "d"},
		args:        []string{"smembers", "{a}set1"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{
				{
					DataType: BulkStringRespType,
					Value:    "a",
				}, {
					DataType: BulkStringRespType,
					Value:    "b",
				}, {
					DataType: BulkStringRespType,
					Value:    "c",
				}, {
					DataType: BulkStringRespType,
					Value:    "d",
				}},
		},
		compareFn: testCompareSameElement,
		emptyKeys: []string{"{a}set1"},
	}, {
		name:        "spop",
		description: "spop multiple elements in a set key",
		prepareFn:   testNewSetKey,
		prepareArgs: []interface{}{"{a}set1", "a", "b", "c", "d"},
		args:        []string{"spop", "{a}set1", "4"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{
				{
					DataType: BulkStringRespType,
					Value:    "a",
				}, {
					DataType: BulkStringRespType,
					Value:    "b",
				}, {
					DataType: BulkStringRespType,
					Value:    "c",
				}, {
					DataType: BulkStringRespType,
					Value:    "d",
				}},
		},
		compareFn: testCompareSameElement,
		emptyKeys: []string{"{a}set1"},
	}, {
		name:        "spop",
		description: "spop single element in a set key",
		prepareFn:   testNewSetKey,
		prepareArgs: []interface{}{"{a}set1", "a"},
		args:        []string{"spop", "{a}set1"},
		respData:    RESPData{DataType: BulkStringRespType, Value: "a"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}set1"},
	}, {
		name:        "spop",
		description: "spop single element in a non existed set key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"spop", "{a}set1"},
		respData:    RESPData{DataType: NilRespType, Value: nil},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{},
	}, {
		name:        "spop",
		description: "spop multiple elements in a non existed set key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"spop", "{a}set1", "4"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value:    []RESPData{},
		},
		compareFn: testCompareSameElement,
		emptyKeys: []string{},
	}, {
		name:        "srem",
		description: "srem a set key",
		prepareFn:   testNewSetKey,
		prepareArgs: []interface{}{"{a}set1", "a", "b", "c", "d"},
		args:        []string{"srem", "{a}set1", "a", "b", "x", "y"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(2)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}set1"},
	}, {
		name:        "srem",
		description: "srem a non existed set key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"srem", "{a}set1", "a", "b", "x", "y"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}set1"},
	}, {
		name:        "hdel",
		description: "hdel a hash key",
		prepareFn:   testNewHashKey,
		prepareArgs: []interface{}{"{a}hash1", "a", "b", "c", "d"},
		args:        []string{"hdel", "{a}hash1", "a", "x", "c", "y", "z"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(2)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}hash1"},
	}, {
		name:        "hexists",
		description: "hexists a hash key",
		prepareFn:   testNewHashKey,
		prepareArgs: []interface{}{"{a}hash1", "a", "b", "c", "d"},
		args:        []string{"hexists", "{a}hash1", "a"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(1)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}hash1"},
	}, {
		name:        "hexists",
		description: "hexists a hash key with a non existed element",
		prepareFn:   testNewHashKey,
		prepareArgs: []interface{}{"{a}hash1", "a", "b", "c", "d"},
		args:        []string{"hexists", "{a}hash1", "x"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}hash1"},
	}, {
		name:        "hget",
		description: "hget a hash key",
		prepareFn:   testNewHashKey,
		prepareArgs: []interface{}{"{a}hash1", "a", "b", "c", "d"},
		args:        []string{"hget", "{a}hash1", "a"},
		respData:    RESPData{DataType: BulkStringRespType, Value: "b"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}hash1"},
	}, {
		name:        "hget",
		description: "hget a hash key with a non existed element",
		prepareFn:   testNewHashKey,
		prepareArgs: []interface{}{"{a}hash1", "a", "b", "c", "d"},
		args:        []string{"hget", "{a}hash1", "x"},
		respData:    RESPData{DataType: NilRespType, Value: nil},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}hash1"},
	}, {
		name:        "hgetall",
		description: "hgetall hash key",
		prepareFn:   testNewHashKey,
		prepareArgs: []interface{}{"{a}hash1", "a", "b", "c", "d"},
		args:        []string{"hgetall", "{a}hash1"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{
				{
					DataType: BulkStringRespType,
					Value:    "a",
				}, {
					DataType: BulkStringRespType,
					Value:    "b",
				}, {
					DataType: BulkStringRespType,
					Value:    "c",
				}, {
					DataType: BulkStringRespType,
					Value:    "d",
				}},
		},
		compareFn: testCompareSameElementAndOrder,
		emptyKeys: []string{"{a}hash1"},
	}, {
		name:        "hincrby",
		description: "hincrby a hash key",
		prepareFn:   testNewHashKey,
		prepareArgs: []interface{}{"{a}hash1", "a", "1", "c", "d"},
		args:        []string{"hincrby", "{a}hash1", "a", "8"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(9)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}hash1"},
	}, {
		name:        "hincrby",
		description: "hincrby a key with no int value",
		prepareFn:   testNewHashKey,
		prepareArgs: []interface{}{"{a}hash1", "a", "1", "c", "d"},
		args:        []string{"hincrby", "{a}hash1", "c", "8"},
		respData:    RESPData{DataType: ErrorRespType, Value: nil},
		compareFn:   testIsErrorType,
		emptyKeys:   []string{"{a}hash1"},
	}, {
		name:        "hincrby",
		description: "hincrby a non existed key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []string{},
		args:        []string{"hincrby", "{a}hash1", "a", "7"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(7)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}hash1"},
	}, {
		name:        "hincrbyfloat",
		description: "hincrbyfloat a key",
		prepareFn:   testNewHashKey,
		prepareArgs: []interface{}{"{a}hash1", "a", "1.2", "c", "d"},
		args:        []string{"hincrbyfloat", "{a}hash1", "a", "1.5"},
		respData:    RESPData{DataType: BulkStringRespType, Value: "2.7"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}hash1"},
	}, {
		name:        "hkeys",
		description: "hkeys a hash key",
		prepareFn:   testNewHashKey,
		prepareArgs: []interface{}{"{a}hash1", "a", "b", "c", "d"},
		args:        []string{"hkeys", "{a}hash1"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{{
				DataType: BulkStringRespType,
				Value:    "a",
			}, {
				DataType: BulkStringRespType,
				Value:    "b",
			},
			},
		},
		compareFn: testCompareSameElement,
		emptyKeys: []string{"{a}hash1"},
	}, {
		name:        "hkeys",
		description: "hkeys a non existed hash key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"hkeys", "{a}hash1"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value:    []RESPData{},
		},
		compareFn: testCompareEqual,
		emptyKeys: []string{},
	}, {
		name:        "hlen",
		description: "hlen a hash key",
		prepareFn:   testNewHashKey,
		prepareArgs: []interface{}{"{a}hash1", "a", "b", "c", "d"},
		args:        []string{"hlen", "{a}hash1"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(2)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}hash1"},
	}, {
		name:        "hlen",
		description: "hlen a non existed hash key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"hlen", "{a}hash1"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{},
	}, {
		name:        "hmget",
		description: "hmget a hash key",
		prepareFn:   testNewHashKey,
		prepareArgs: []interface{}{"{a}hash1", "a", "b", "c", "d"},
		args:        []string{"hmget", "{a}hash1", "a", "c", "x", "y"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{{
				DataType: BulkStringRespType,
				Value:    "b",
			}, {
				DataType: BulkStringRespType,
				Value:    "d",
			}, {
				DataType: NilRespType,
				Value:    nil,
			}, {
				DataType: NilRespType,
				Value:    nil,
			}},
		},
		compareFn: testCompareEqual,
		emptyKeys: []string{"{a}hash1"},
	}, {
		name:        "hmget",
		description: "hmget a non existed hash key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"hmget", "{a}hash1", "a", "c", "x", "y"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{{
				DataType: NilRespType,
				Value:    nil,
			}, {
				DataType: NilRespType,
				Value:    nil,
			}, {
				DataType: NilRespType,
				Value:    nil,
			}, {
				DataType: NilRespType,
				Value:    nil,
			}},
		},
		compareFn: testCompareEqual,
		emptyKeys: []string{"{a}hash1"},
	}, {
		name:        "hmset",
		description: "hmset a hash key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"hmset", "{a}hash1", "a", "b", "c", "d"},
		respData:    RESPData{DataType: SimpleStringRespType, Value: "OK"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}hash1"},
	}, {
		name:        "hset",
		description: "hset a hash key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"hset", "{a}hash1", "a", "b", "c", "d", "x", "y"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(3)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}hash1"},
	}, {
		name:        "hsetnx",
		description: "hsetnx a hash key",
		prepareFn:   testNewHashKey,
		prepareArgs: []interface{}{"{a}hash1", "a", "b", "c", "d"},
		args:        []string{"hsetnx", "{a}hash1", "x", "y"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(1)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}hash1"},
	}, {
		name:        "hsetnx",
		description: "hsetnx a non existed hash key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"hsetnx", "{a}hash1", "x", "y"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(1)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}hash1"},
	}, {
		name:        "hsetnx",
		description: "hsetnx a hash key with existed field",
		prepareFn:   testNewHashKey,
		prepareArgs: []interface{}{"{a}hash1", "a", "b", "c", "d"},
		args:        []string{"hsetnx", "{a}hash1", "a", "c"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}hash1"},
	}, {
		name:        "hstrlen",
		description: "hstrlen a hash key",
		prepareFn:   testNewHashKey,
		prepareArgs: []interface{}{"{a}hash1", "a", "bcd", "c", "xyzabc"},
		args:        []string{"hstrlen", "{a}hash1", "c"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(6)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}hash1"},
	}, {
		name:        "hstrlen",
		description: "hstrlen a non existed hash key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"hstrlen", "{a}hash1", "c"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{},
	}, {
		name:        "hstrlen",
		description: "hstrlen a hash key with non existed field",
		prepareFn:   testNewHashKey,
		prepareArgs: []interface{}{"{a}hash1", "a", "bcd", "c", "xyzabc"},
		args:        []string{"hstrlen", "{a}hash1", "x"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}hash1"},
	}, {
		name:        "hvals",
		description: "hvals a hash key",
		prepareFn:   testNewHashKey,
		prepareArgs: []interface{}{"{a}hash1", "a", "bcd", "c", "xyzabc"},
		args:        []string{"hvals", "{a}hash1"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{{
				DataType: BulkStringRespType,
				Value:    "xyzabc",
			}, {
				DataType: BulkStringRespType,
				Value:    "bcd",
			}},
		},
		compareFn: testCompareSameElement,
		emptyKeys: []string{"{a}hash1"},
	}, {
		name:        "hvals",
		description: "hvals a non existed hash key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"hvals", "{a}hash1"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value:    []RESPData{},
		},
		compareFn: testCompareEqual,
		emptyKeys: []string{"{a}hash1"},
	}, {
		name:        "zadd",
		description: "zadd a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "10.5", "b", "0.4", "c", "3.6"},
		args:        []string{"zadd", "{a}zset1", "10.5", "a", "0.5", "b", "10.2", "x"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(1)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}zset1"},
	}, {
		// TODO: gt and lt available since redis 6.2.
		// 	name:        "zadd",
		// 	description: "zadd a zset key with lt and ch",
		// 	prepareFn:   testNewZSetKey,
		// 	prepareArgs: []interface{}{"{a}zset1", "a", "10.5", "b", "0.4", "c", "3.6"},
		// 	args:        []string{"zadd", "{a}zset1", "ch", "lt", "a", "0.5", "b", "0.5", "x", "10.2"},
		// 	respData:    RESPData{DataType: IntegerRespType, Value: int64(2)},
		// 	compareFn:   testCompareEqual,
		// 	emptyKeys:   []string{"{a}zset1"},
		// }, {
		// 	name:        "zadd",
		// 	description: "zadd a zset key with gt and ch",
		// 	prepareFn:   testNewZSetKey,
		// 	prepareArgs: []interface{}{"{a}zset1", "a", "10.5", "b", "0.4", "c", "3.6"},
		// 	args:        []string{"zadd", "{a}zset1", "ch", "gt", "a", "0.5", "b", "0.5", "x", "10.2"},
		// 	respData:    RESPData{DataType: IntegerRespType, Value: int64(2)},
		// 	compareFn:   testCompareEqual,
		// 	emptyKeys:   []string{"{a}zset1"},
		// }, {
		name:        "zadd",
		description: "zadd a zset key with xx and ch",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "10.5", "b", "0.4", "c", "3.6"},
		args:        []string{"zadd", "{a}zset1", "xx", "ch", "0.5", "a", "0.5", "b", "10.2", "x"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(2)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}zset1"},
	}, {
		name:        "zadd",
		description: "zadd a zset key with nx and ch",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "10.5", "b", "0.4", "c", "3.6"},
		args:        []string{"zadd", "{a}zset1", "nx", "ch", "0.5", "a", "0.5", "b", "10.2", "x"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(1)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}zset1"},
	}, {
		name:        "zadd",
		description: "zadd a zset key with incr",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.4", "c", "3.6"},
		args:        []string{"zadd", "{a}zset1", "incr", "0.25", "a"},
		respData:    RESPData{DataType: BulkStringRespType, Value: "0.75"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}zset1"},
	}, {
		name:        "zcard",
		description: "zcard a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.4", "c", "3.6"},
		args:        []string{"zcard", "{a}zset1"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(3)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}zset1"},
	}, {
		name:        "zcount",
		description: "zcount a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.4", "c", "3.6"},
		args:        []string{"zcount", "{a}zset1", "0.2", "1"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(2)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}zset1"},
	}, {
		name:        "zincrby",
		description: "zincrby a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.4", "c", "3.6"},
		args:        []string{"zincrby", "{a}zset1", "0.25", "a"},
		respData:    RESPData{DataType: BulkStringRespType, Value: "0.75"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}zset1"},
	}, {
		name:        "zpopmax",
		description: "zpopmax a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.25", "c", "1.25"},
		args:        []string{"zpopmax", "{a}zset1", "2"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{{
				DataType: BulkStringRespType,
				Value:    "c",
			}, {
				DataType: BulkStringRespType,
				Value:    "1.25",
			}, {
				DataType: BulkStringRespType,
				Value:    "a",
			}, {
				DataType: BulkStringRespType,
				Value:    "0.5",
			}},
		},
		compareFn: testCompareEqual,
		emptyKeys: []string{"{a}zset1"},
	}, {
		name:        "zpopmin",
		description: "zpopmin a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.25", "c", "1.25"},
		args:        []string{"zpopmin", "{a}zset1", "2"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{{
				DataType: BulkStringRespType,
				Value:    "b",
			}, {
				DataType: BulkStringRespType,
				Value:    "0.25",
			}, {
				DataType: BulkStringRespType,
				Value:    "a",
			}, {
				DataType: BulkStringRespType,
				Value:    "0.5",
			}},
		},
		compareFn: testCompareEqual,
		emptyKeys: []string{"{a}zset1"},
	}, {
		name:        "zrange",
		description: "zrange a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.25", "c", "1.25"},
		args:        []string{"zrange", "{a}zset1", "0", "-1"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{{
				DataType: BulkStringRespType,
				Value:    "b",
			}, {
				DataType: BulkStringRespType,
				Value:    "a",
			}, {
				DataType: BulkStringRespType,
				Value:    "c",
			}},
		},
		compareFn: testCompareEqual,
		emptyKeys: []string{"{a}zset1"},
	}, {
		name:        "zrange",
		description: "zrange a zset key with scores",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.25", "c", "1.25"},
		args:        []string{"zrange", "{a}zset1", "0", "-1", "withscores"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{{
				DataType: BulkStringRespType,
				Value:    "b",
			}, {
				DataType: BulkStringRespType,
				Value:    "0.25",
			}, {
				DataType: BulkStringRespType,
				Value:    "a",
			}, {
				DataType: BulkStringRespType,
				Value:    "0.5",
			}, {
				DataType: BulkStringRespType,
				Value:    "c",
			}, {
				DataType: BulkStringRespType,
				Value:    "1.25",
			}},
		},
		compareFn: testCompareEqual,
		emptyKeys: []string{"{a}zset1"},
	}, {
		name:        "zrangebylex",
		description: "zrangebylex a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.5", "c", "0.5"},
		args:        []string{"zrangebylex", "{a}zset1", "[a", "(c"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{{
				DataType: BulkStringRespType,
				Value:    "a",
			}, {
				DataType: BulkStringRespType,
				Value:    "b",
			}},
		},
		compareFn: testCompareEqual,
		emptyKeys: []string{"{a}zset1"},
	}, {
		name:        "zrevrangebylex",
		description: "zrevrangebylex a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.5", "c", "0.5"},
		args:        []string{"zrevrangebylex", "{a}zset1", "(c", "(a"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{{
				DataType: BulkStringRespType,
				Value:    "b",
			}},
		},
		compareFn: testCompareEqual,
		emptyKeys: []string{"{a}zset1"},
	}, {
		name:        "zrangebyscore",
		description: "zrangebyscore a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.25", "c", "1.25"},
		args:        []string{"zrangebyscore", "{a}zset1", "(0.25", "(1.25"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{{
				DataType: BulkStringRespType,
				Value:    "a",
			}},
		},
		compareFn: testCompareEqual,
		emptyKeys: []string{"{a}zset1"},
	}, {
		name:        "zrangebyscore",
		description: "zrangebyscore a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.25", "c", "1.25"},
		args:        []string{"zrangebyscore", "{a}zset1", "(0.25", "1.25", "withscores"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{{
				DataType: BulkStringRespType,
				Value:    "a",
			}, {
				DataType: BulkStringRespType,
				Value:    "0.5",
			}, {
				DataType: BulkStringRespType,
				Value:    "c",
			}, {
				DataType: BulkStringRespType,
				Value:    "1.25",
			}},
		},
		compareFn: testCompareEqual,
		emptyKeys: []string{"{a}zset1"},
	}, {
		name:        "zrank",
		description: "zrank a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.25", "c", "1.25"},
		args:        []string{"zrank", "{a}zset1", "a"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(1)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}zset1"},
	}, {
		name:        "zrank",
		description: "zrank a zset key with a non existed element",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.25", "c", "1.25"},
		args:        []string{"zrank", "{a}zset1", "x"},
		respData:    RESPData{DataType: NilRespType, Value: nil},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}zset1"},
	}, {
		name:        "zrank",
		description: "zrank a non existed zset key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"zrank", "{a}zset1", "x"},
		respData:    RESPData{DataType: NilRespType, Value: nil},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{},
	}, {
		name:        "zrem",
		description: "zrem a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.25", "c", "1.25"},
		args:        []string{"zrem", "{a}zset1", "x", "a", "b", "y", "z"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(2)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}zset1"},
	}, {
		name:        "zremrangebylex",
		description: "zremrangebylex a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.5", "c", "0.5"},
		args:        []string{"zremrangebylex", "{a}zset1", "(a", "[c"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(2)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}zset1"},
	}, {
		name:        "zremrangebyrank",
		description: "zremrangebyrank a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.25", "c", "1.25"},
		args:        []string{"zremrangebyrank", "{a}zset1", "1", "10"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(2)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}zset1"},
	}, {
		name:        "zremrangebyscore",
		description: "zremrangebyscore a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.25", "c", "1.25"},
		args:        []string{"zremrangebyscore", "{a}zset1", "-inf", "(1.25"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(2)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}zset1"},
	}, {
		name:        "zrevrange",
		description: "zrevrange a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.25", "c", "1.25"},
		args:        []string{"zrevrange", "{a}zset1", "0", "-1"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{{
				DataType: BulkStringRespType,
				Value:    "c",
			}, {
				DataType: BulkStringRespType,
				Value:    "a",
			}, {
				DataType: BulkStringRespType,
				Value:    "b",
			}},
		},
		compareFn: testCompareEqual,
		emptyKeys: []string{"{a}zset1"},
	}, {
		name:        "zrevrange",
		description: "zrevrange a zset key with scores",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.25", "c", "1.25"},
		args:        []string{"zrevrange", "{a}zset1", "0", "-1", "withscores"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{{
				DataType: BulkStringRespType,
				Value:    "c",
			}, {
				DataType: BulkStringRespType,
				Value:    "1.25",
			}, {
				DataType: BulkStringRespType,
				Value:    "a",
			}, {
				DataType: BulkStringRespType,
				Value:    "0.5",
			}, {
				DataType: BulkStringRespType,
				Value:    "b",
			}, {
				DataType: BulkStringRespType,
				Value:    "0.25",
			}},
		},
		compareFn: testCompareEqual,
		emptyKeys: []string{"{a}zset1"},
	}, {
		name:        "zrevrangebyscore",
		description: "zrevrangebyscore a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.25", "c", "1.25"},
		args:        []string{"zrevrangebyscore", "{a}zset1", "1.25", "(0.25", "withscores"},
		respData: RESPData{
			DataType: ArrayRespType,
			Value: []RESPData{{
				DataType: BulkStringRespType,
				Value:    "c",
			}, {
				DataType: BulkStringRespType,
				Value:    "1.25",
			}, {
				DataType: BulkStringRespType,
				Value:    "a",
			}, {
				DataType: BulkStringRespType,
				Value:    "0.5",
			}},
		},
		compareFn: testCompareEqual,
		emptyKeys: []string{"{a}zset1"},
	}, {
		name:        "zrevrank",
		description: "zrevrank a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.25", "c", "1.25"},
		args:        []string{"zrevrank", "{a}zset1", "c"},
		respData:    RESPData{DataType: IntegerRespType, Value: int64(0)},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}zset1"},
	}, {
		name:        "zrevrank",
		description: "zrevrank a zset key with a non existed element",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.25", "c", "1.25"},
		args:        []string{"zrevrank", "{a}zset1", "x"},
		respData:    RESPData{DataType: NilRespType, Value: nil},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}zset1"},
	}, {
		name:        "zrevrank",
		description: "zrevrank a non existed zset key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"zrevrank", "{a}zset1", "x"},
		respData:    RESPData{DataType: NilRespType, Value: nil},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{},
	}, {
		name:        "zscore",
		description: "zscore a zset key",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.25", "c", "1.25"},
		args:        []string{"zscore", "{a}zset1", "c"},
		respData:    RESPData{DataType: BulkStringRespType, Value: "1.25"},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}zset1"},
	}, {
		name:        "zscore",
		description: "zscore a zset key with non existed element",
		prepareFn:   testNewZSetKey,
		prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.25", "c", "1.25"},
		args:        []string{"zscore", "{a}zset1", "x"},
		respData:    RESPData{DataType: NilRespType, Value: nil},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{"{a}zset1"},
	}, {
		name:        "zscore",
		description: "zscore a non existed zset key",
		prepareFn:   testPrepareNOOP,
		prepareArgs: []interface{}{},
		args:        []string{"zscore", "{a}zset1", "x"},
		respData:    RESPData{DataType: NilRespType, Value: nil},
		compareFn:   testCompareEqual,
		emptyKeys:   []string{},
		// TODO: zmscore available since 6.2
		// }, {
		// 	name:        "zmscore",
		// 	description: "zmscore a zset key",
		// 	prepareFn:   testNewZSetKey,
		// 	prepareArgs: []interface{}{"{a}zset1", "a", "0.5", "b", "0.25", "c", "1.25"},
		// 	args:        []string{"zmscore", "{a}zset1", "c", "a", "x", "y", "b"},
		// 	respData: RESPData{
		// 		DataType: ArrayRespType,
		// 		Value: []RESPData{{
		// 			DataType: BulkStringRespType,
		// 			Value:    "1.25",
		// 		}, {
		// 			DataType: BulkStringRespType,
		// 			Value:    "0.5",
		// 		}, {
		// 			DataType: NilRespType,
		// 			Value:    nil,
		// 		}, {
		// 			DataType: NilRespType,
		// 			Value:    nil,
		// 		}, {
		// 			DataType: BulkStringRespType,
		// 			Value:    "0.25",
		// 		}},
		// 	},
		// 	compareFn: testCompareEqual,
		// 	emptyKeys: []string{"{a}zset1"},
		// }, {
		// 	name:        "zmscore",
		// 	description: "zmscore a non existed zset key",
		// 	prepareFn:   testPrepareNOOP,
		// 	prepareArgs: []interface{}{},
		// 	args:        []string{"zmscore", "{a}zset1", "a", "b", "c"},
		// 	respData: RESPData{
		// 		DataType: ArrayRespType,
		// 		Value: []RESPData{{
		// 			DataType: NilRespType,
		// 			Value:    nil,
		// 		}, {
		// 			DataType: NilRespType,
		// 			Value:    nil,
		// 		}, {
		// 			DataType: NilRespType,
		// 			Value:    nil,
		// 		}},
		// 	},
		// 	compareFn: testCompareEqual,
		// 	emptyKeys: []string{},
	},
}

func testCompareEqual(data1, data2 RESPData) bool {
	if data1.DataType != data2.DataType {
		return false
	}
	switch data1.DataType {
	case ArrayRespType:
		a1, ok1 := data1.Value.([]RESPData)
		a2, ok2 := data2.Value.([]RESPData)
		if !ok1 || !ok2 {
			return false
		}
		if len(a1) != len(a2) {
			return false
		}
		for index, item := range a1 {
			if !testCompareEqual(item, a2[index]) {
				return false
			}
		}
	default:
		return data1.Value == data2.Value
	}
	return true
}

func testCompareSameElement(data1, data2 RESPData) bool {
	if data1.DataType != ArrayRespType || data2.DataType != ArrayRespType {
		return false
	}
	a1, ok1 := data1.Value.([]RESPData)
	a2, ok2 := data2.Value.([]RESPData)
	if !ok1 || !ok2 {
		return false
	}
	if len(a1) != len(a2) {
		return false
	}
	for _, item := range a1 {
		if !testRESPArrayContains(a2, item) {
			return false
		}
	}
	return true
}

func testCompareSameElementAndOrder(data1, data2 RESPData) bool {
	if !testCompareSameElement(data1, data2) {
		return false
	}
	a1 := data1.Value.([]RESPData)
	a2 := data2.Value.([]RESPData)
	for index, item := range a1 {
		index2 := testRESPDataFindIndex(a2, item)
		if index2 == -1 {
			return false
		}
		if (index % 2) != (index2 % 2) {
			return false
		}
	}
	return true
}

func testCompareIntGreaterThan(data1, data2 RESPData) bool {
	if data1.DataType != IntegerRespType || data2.DataType != IntegerRespType {
		return false
	}
	return data1.Value.(int64) < data2.Value.(int64)
}

func testIsErrorType(data1, data2 RESPData) bool {
	return data1.DataType == ErrorRespType && data2.DataType == ErrorRespType
}

func testRESPArrayContains(array []RESPData, item RESPData) bool {
	for _, element := range array {
		if element.DataType == item.DataType && element.Value == element.Value {
			return true
		}
	}
	return false
}

func testRESPDataFindIndex(array []RESPData, item RESPData) int {
	for index, element := range array {
		if element.DataType == item.DataType && element.Value == item.Value {
			return index
		}
	}
	return -1
}

func testNewStringKeys(keys interface{}) {
	client := base.GetRedisCluster()
	for _, key := range keys.([]string) {
		client.Set(context.TODO(), key, key, 0)
	}
}

func testNewStringKeyWithExpiration(input interface{}) {
	slice := input.([]interface{})
	key := slice[0].(string)
	duration := slice[1].(time.Duration)
	client := base.GetRedisCluster()
	client.Set(context.TODO(), key, key, duration)
}

func testNewListKey(input interface{}) {
	slice := input.([]interface{})
	key := slice[0].(string)
	values := slice[1:]
	client := base.GetRedisCluster()
	client.RPush(context.TODO(), key, values...)
}

func testNewSetKey(input interface{}) {
	slice := input.([]interface{})
	key := slice[0].(string)
	values := slice[1:]
	client := base.GetRedisCluster()
	client.SAdd(context.TODO(), key, values...)
}

func testNewHashKey(input interface{}) {
	slice := input.([]interface{})
	key := slice[0].(string)
	values := slice[1:]
	client := base.GetRedisCluster()
	client.HSet(context.TODO(), key, values...)
}

func testPrepareNOOP(input interface{}) {
	return
}

func testNewStringKeyValue(input interface{}) {
	slice := input.([]string)
	key := slice[0]
	value := slice[1]
	client := base.GetRedisCluster()
	client.Set(context.TODO(), key, value, 0)
}

func testNewZSetKey(input interface{}) {
	slice := input.([]interface{})
	key := slice[0].(string)
	values := slice[1:]
	zSlice := make([]*redis.Z, len(values)/2)
	for i := 0; i < len(values)-1; i += 2 {
		score, _ := strconv.ParseFloat(values[i+1].(string), 64)
		z := &redis.Z{
			Member: values[i],
			Score:  score,
		}
		zSlice[i/2] = z
	}

	client := base.GetRedisCluster()
	client.ZAdd(context.TODO(), key, zSlice...)
}

func TestExtractHashTagFromKey(t *testing.T) {
	cases := []struct {
		key     string
		hashTag string
	}{
		{"a", ""},
		{"", ""},
		{"a}{", ""},
		{"{}a", ""},
		{"{a}", "a"},
		{"{ab}", "ab"},
		{"{a}b", "a"},
		{"{ab}c", "ab"},
		{"{ab}c{d}", "ab"},
		{"x{ab}c{d}", "ab"},
		{"a{b}", "b"},
		{"a{bc}", "bc"},
		{"a{bc}d", "bc"},
		{"}{ab}cab", "ab"},
		{"{}{abc}xy", ""},
		{"{{abc}}xy", "{abc"},
	}
	for _, c := range cases {
		hashTag := ExtractHashTagFromKey(c.key)
		assert.Equal(t, c.hashTag, hashTag)
	}
}

func TestCommandKeysHashTag(t *testing.T) {
	for _, testCase := range testCommandHashTagCases {
		newFn := supportedCommands[testCase.name]
		log.Printf("command keys hash tag test case: command args: %v\n", testCase.args)
		command, err := newFn(testCase.args)
		assert.Nil(t, err)
		hashTag, err := CheckAndGetCommandKeysHashTag(command)
		if testCase.valid {
			assert.Equal(t, testCase.hashTag, hashTag)
			assert.Nil(t, err)
		} else {
			assert.Equal(t, "", hashTag)
			assert.Equal(t, testCase.err, err)
		}
	}

}

type testCommandHashTagCase struct {
	name    string
	args    []string
	valid   bool
	hashTag string
	err     error
}

var testCommandHashTagCases = []testCommandHashTagCase{
	{
		name:    "del",
		args:    []string{"del", "x{abc}", "{abc}y", "a{abc}d", "{abc}"},
		valid:   true,
		hashTag: "abc",
	}, {
		name:  "exists",
		args:  []string{"exists", "x{a}", "x{b}", "y{a}"},
		valid: false,
		err:   errCommnandKeysMultipleHashTags,
	}, {
		name:  "get",
		args:  []string{"get", "abc"},
		valid: false,
		err:   errCommandKeyNoHashTag,
	}, {
		name:    "sadd",
		args:    []string{"sadd", "x{abc}", "a", "b", "c"},
		valid:   true,
		hashTag: "abc",
	}, {
		name:    "exec",
		args:    []string{"exec"},
		valid:   true,
		hashTag: "",
	},
}
