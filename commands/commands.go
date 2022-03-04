package commands

import (
	"bytepower_room/base"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/go-redis/redis/v8"
)

var contextTODO = context.TODO()

type NewCommandFunc func([]string) (Commander, error)

var supportedCommands = map[string]NewCommandFunc{
	// keys commands
	"del":       NewDelCommand,
	"exists":    NewExistsCommand,
	"expire":    NewExpireCommand,
	"expireat":  NewExpireAtCommand,
	"persist":   NewPersistCommand,
	"pexpire":   NewPExpireCommand,
	"pexpireat": NewExpireAtCommand,
	"pttl":      NewPTTLCommand,
	"rename":    NewRenameCommand,
	"renamenx":  NewRenameNXCommand,
	"ttl":       NewTTLCommand,
	"type":      NewTypeCommand,

	// string commands
	"set":         NewSetCommand,
	"get":         NewGetCommand,
	"append":      NewAppendCommand,
	"decr":        NewDecrCommand,
	"decrby":      NewDecrByCommand,
	"getrange":    NewGetRangeCommand,
	"getset":      NewGetSetCommand,
	"incr":        NewIncrCommand,
	"incrby":      NewIncrByCommand,
	"incrbyfloat": NewIncrByFloatCommand,
	"mget":        NewMGetCommand,
	"mset":        NewMSetCommand,
	"msetnx":      NewMSetNXCommand,
	"psetex":      NewPSetEXCommand,
	"setex":       NewSetEXCommand,
	"setnx":       NewSetNXCommand,
	"setrange":    NewSetRangeCommand,
	"strlen":      NewStrlenCommand,

	// list commands
	"lindex":    NewLIndexCommand,
	"linsert":   NewLInsertCommand,
	"llen":      NewLLenCommand,
	"lpop":      NewLPopCommand,
	"lpos":      NewLPosCommand,
	"lpush":     NewLPushCommand,
	"lpushx":    NewLPushXCommand,
	"lrange":    NewLRangeCommand,
	"lrem":      NewLRemCommand,
	"lset":      NewLSetCommand,
	"ltrim":     NewLTrimCommand,
	"rpop":      NewRPopCommand,
	"rpoplpush": NewRPopLPushCommand,
	"lmove":     NewLMoveCommand,
	"rpush":     NewRPushCommand,
	"rpushx":    NewRPushXCommand,

	// set commands
	"sadd":        NewSAddCommand,
	"scard":       NewSCardCommand,
	"sdiff":       NewSDiffCommand,
	"sdiffstore":  NewSDiffStoreCommand,
	"sinter":      NewSInterCommand,
	"sinterstore": NewSInterStoreCommand,
	"sismember":   NewSIsMemberCommand,
	"smismember":  NewSMIsMemberCommand,
	"smembers":    NewSMembersCommand,
	"smove":       NewSMoveCommand,
	"spop":        NewSPopCommand,
	"srandmember": NewSRandMemberCommand,
	"srem":        NewSRemCommand,
	"sunion":      NewSUnionCommand,
	"sunionstore": NewSUnionStoreCommand,

	// hash commands
	"hdel":         NewHDelCommand,
	"hexists":      NewHExistsCommand,
	"hget":         NewHGetCommand,
	"hgetall":      NewHGetAllCommand,
	"hincrby":      NewHIncrByCommand,
	"hincrbyfloat": NewHIncrByFloatCommand,
	"hkeys":        NewHKeysCommand,
	"hlen":         NewHLenCommand,
	"hmget":        NewHMGetCommand,
	"hmset":        NewHMSetCommand,
	"hset":         NewHSetCommand,
	"hsetnx":       NewHSetNXCommand,
	"hstrlen":      NewHStrlenCommand,
	"hvals":        NewHValsCommand,

	//zset commands
	"zadd":             NewZAddCommand,
	"zcard":            NewZCardCommand,
	"zcount":           NewZCountCommand,
	"zdiff":            NewZDiffCommand,
	"zdiffstore":       NewZDiffStoreCommand,
	"zincrby":          NewZIncrByCommand,
	"zlexcount":        NewZLexCountCommand,
	"zpopmax":          NewZPopMaxCommand,
	"zpopmin":          NewZPopMinCommand,
	"zrange":           NewZRangeCommand,
	"zrangebylex":      NewZRangeByLexCommand,
	"zrevrangebylex":   NewZRevRangeByLexCommand,
	"zrangebyscore":    NewZRangeByScoreCommand,
	"zrank":            NewZRankCommand,
	"zrem":             NewZRemCommand,
	"zremrangebylex":   NewZRemRangeByLexCommand,
	"zremrangebyrank":  NewZRemRangeByRankCommand,
	"zremrangebyscore": NewZRemRangeByScoreCommand,
	"zrevrange":        NewZRevRangeCommand,
	"zrevrangebyscore": NewZRevRangeByScoreCommand,
	"zrevrank":         NewZRevRankCommand,
	"zscore":           NewZScoreCommand,
	"zmscore":          NewZMScoreCommand,

	// server commands
	"command": NewCommandCommand,
	"echo":    NewEchoCommand,
	"ping":    NewPingCommand,

	// transaction commands
	"watch":   NewWatchCommand,
	"multi":   NewMultiCommand,
	"exec":    NewExecCommand,
	"discard": NewDiscardCommand,
	"unwatch": NewUnwatchCommand,
}

type RESPType string

const (
	SimpleStringRespType RESPType = "simple_string"
	BulkStringRespType   RESPType = "bulk_string"
	ErrorRespType        RESPType = "error"
	IntegerRespType      RESPType = "integer"
	ArrayRespType        RESPType = "array"
	NilRespType          RESPType = "nil"
	NilArrayRespType     RESPType = "nil_array"
)

type RESPData struct {
	DataType RESPType
	Value    interface{}
}

func (data RESPData) String() string {
	var result string
	switch data.DataType {
	case SimpleStringRespType:
		result = fmt.Sprintf("s:%s", data.Value)
	case BulkStringRespType:
		result = fmt.Sprintf("bs:%s", data.Value)
	case ErrorRespType:
		result = fmt.Sprintf("err:%v", data.Value)
	case IntegerRespType:
		result = fmt.Sprintf("i:%d", data.Value)
	case NilRespType:
		result = "nil:nil"
	case ArrayRespType:
		array := data.Value.([]RESPData)
		result = fmt.Sprintf("a:%d{ ", len(array))
		for _, item := range array {
			result = result + item.String() + " "
		}
		result = result + " }"
	case NilArrayRespType:
		result = "na:na"
	}
	return result
}

func ConvertErrorToRESPData(err error) RESPData {
	if errors.Is(err, redis.Nil) {
		return RESPData{DataType: NilRespType, Value: nil}
	}
	if errors.Is(err, redis.TxFailedErr) {
		return RESPData{DataType: NilArrayRespType, Value: nil}
	}
	return RESPData{DataType: ErrorRespType, Value: err}
}

type Commander interface {
	Name() string
	ReadKeys() []string
	WriteKeys() []string
	Cmd() redis.Cmder
	Args() []string
	String() string
}

type commonCommand struct {
	name string
	args []string
}

func (command *commonCommand) Name() string {
	return command.name
}

func (command *commonCommand) String() string {
	return strings.Join(command.args, " ")
}

func (command *commonCommand) Args() []string {
	return command.args
}

func (command *commonCommand) ReadKeys() []string {
	return []string{}
}

func (command *commonCommand) WriteKeys() []string {
	return []string{}
}

func (command *commonCommand) init(args []string) {
	command.name = strings.ToLower(args[0])
	command.args = args
}

func (command *commonCommand) argsToInterfaceSlice() []interface{} {
	args := make([]interface{}, len(command.args))
	for index, arg := range command.args {
		args[index] = arg
	}
	return args
}

func GetCommnadKeysAccessMode(command Commander) base.HashTagAccessMode {
	if len(command.WriteKeys()) > 0 {
		return base.HashTagAccessModeWrite
	}
	return base.HashTagAccessModeRead
}

func CheckAndGetCommandKeysHashTag(command Commander) (string, error) {
	hashTag := ""
	for _, key := range append(command.ReadKeys(), command.WriteKeys()...) {
		tag := ExtractHashTagFromKey(key)
		if tag == "" {
			return "", errCommandKeyNoHashTag
		}
		if hashTag != "" && tag != hashTag {
			return "", errCommnandKeysMultipleHashTags
		}
		hashTag = tag
	}
	return hashTag, nil
}

func ParseCommand(args []string) (Commander, error) {
	if len(args) == 0 {
		return nil, errEmptyCommand
	}
	commandName := strings.ToLower(args[0])

	fn, ok := supportedCommands[commandName]
	if !ok {
		return nil, newUnknownCommand(commandName, args[1:])
	}
	return fn(args)
}

func ExecuteCommand(redisCluster *redis.ClusterClient, command Commander) RESPData {
	cmd := command.Cmd()
	if err := redisCluster.Process(contextTODO, cmd); err != nil {
		return ConvertErrorToRESPData(err)
	}

	return convertCmdResultToRESPData(cmd)
}

type CommandBatch struct {
	cmds map[int]Commander
}

func NewCommandBatch() CommandBatch {
	return CommandBatch{cmds: make(map[int]Commander)}
}

func (c CommandBatch) getSortedIndexes() []int {
	indexes := make([]int, 0, len(c.cmds))
	for index := range c.cmds {
		indexes = append(indexes, index)
	}
	sort.Ints(indexes)
	return indexes
}

func (c CommandBatch) AddCommand(index int, cmd Commander) {
	c.cmds[index] = cmd
}

func (c CommandBatch) Execute(ctx context.Context, redisCluster *redis.ClusterClient) map[int]RESPData {
	indexes := c.getSortedIndexes()
	result := make(map[int]RESPData, len(c.cmds))
	pipeline := redisCluster.Pipeline()
	for _, index := range indexes {
		pipeline.Process(ctx, c.cmds[index].Cmd())
	}
	cmds, _ := pipeline.Exec(ctx)
	for i, index := range indexes {
		result[index] = convertCmdResultToRESPData(cmds[i])
	}
	return result
}

func ExtractHashTagFromKey(key string) string {
	leftBraceIndex := strings.Index(key, "{")
	if leftBraceIndex == -1 {
		return ""
	}
	rightBraceIndex := strings.Index(key[leftBraceIndex:], "}")
	if rightBraceIndex > 1 {
		return key[leftBraceIndex+1 : leftBraceIndex+rightBraceIndex]
	}
	return ""
}

func convertCmdResultToRESPData(cmd redis.Cmder) RESPData {
	var result RESPData
	switch command := cmd.(type) {
	case *redis.StatusCmd:
		r, err := command.Result()
		if err != nil {
			result = ConvertErrorToRESPData(err)
		} else {
			result = RESPData{DataType: SimpleStringRespType, Value: r}
		}
	case *redis.IntCmd:
		r, err := command.Result()
		if err != nil {
			result = ConvertErrorToRESPData(err)
		} else {
			result = RESPData{DataType: IntegerRespType, Value: r}
		}
	case *redis.StringCmd:
		r, err := command.Result()
		if err != nil {
			result = ConvertErrorToRESPData(err)
		} else {
			result = RESPData{DataType: BulkStringRespType, Value: r}
		}
	case *redis.IntSliceCmd:
		r, err := command.Result()
		if err != nil {
			result = ConvertErrorToRESPData(err)
		} else {
			result = RESPData{DataType: ArrayRespType}
			value := make([]RESPData, 0)
			for _, item := range r {
				value = append(value, RESPData{DataType: IntegerRespType, Value: item})
			}
			result.Value = value
		}
	case *redis.StringSliceCmd:
		r, err := command.Result()
		if err != nil {
			result = ConvertErrorToRESPData(err)
		} else {
			result = RESPData{DataType: ArrayRespType}
			value := make([]RESPData, 0)
			for _, item := range r {
				value = append(value, RESPData{DataType: BulkStringRespType, Value: item})
			}
			result.Value = value
		}
	case *redis.SliceCmd:
		r, err := command.Result()
		if err != nil {
			result = ConvertErrorToRESPData(err)
		} else {
			result = convertSliceToRESPData(r)
		}
	case *redis.CommandsInfoCmd:
		r, err := command.Result()
		if err != nil {
			result = ConvertErrorToRESPData(err)
		} else {
			result = RESPData{DataType: ArrayRespType}
			value := make([]RESPData, 0)
			for name, cmd := range r {
				if _, ok := supportedCommands[strings.ToLower(name)]; ok {
					value = append(value, convertCommandInfoToRESPData(cmd))
				}
			}
			result.Value = value
		}
	default:
		result = ConvertErrorToRESPData(errors.New("ERR invalid response data format"))
	}
	return result
}

func convertSliceToRESPData(slice []interface{}) RESPData {
	data := RESPData{DataType: ArrayRespType}
	value := make([]RESPData, 0)
	for _, item := range slice {
		switch v := item.(type) {
		case nil:
			value = append(value, RESPData{DataType: NilRespType, Value: nil})
		case string:
			value = append(value, RESPData{DataType: BulkStringRespType, Value: v})
		case int, int8, int16, int32, int64:
			value = append(value, RESPData{DataType: IntegerRespType, Value: reflect.ValueOf(v).Int()})
		case uint, uint8, uint16, uint32, uint64:
			value = append(value, RESPData{DataType: IntegerRespType, Value: reflect.ValueOf(v).Int()})
		case error:
			value = append(value, RESPData{DataType: ErrorRespType, Value: v})
		case []interface{}:
			value = append(value, convertSliceToRESPData(v))
		default:
			value = append(value, ConvertErrorToRESPData(errors.New("ERR: invalid response")))
		}
	}
	data.Value = value
	return data
}
