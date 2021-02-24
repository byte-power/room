package commands

import (
	"strconv"

	"github.com/go-redis/redis/v8"
)

type HDelCommand struct {
	key    string
	fields []string
	commonCommand
}

func NewHDelCommand(args []string) (Commander, error) {
	command := &HDelCommand{}
	command.init(args)
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.fields = args[2:]
	return command, nil
}

func (command *HDelCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *HDelCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type HExists struct {
	key   string
	field string
	commonCommand
}

func NewHExistsCommand(args []string) (Commander, error) {
	command := &HExists{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.field = args[2]
	return command, nil
}

func (command *HExists) ReadKeys() []string {
	return []string{command.key}
}

func (command *HExists) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.field)
}

type HGetCommand struct {
	key   string
	field string
	commonCommand
}

func NewHGetCommand(args []string) (Commander, error) {
	command := &HGetCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.field = args[2]
	return command, nil
}

func (command *HGetCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *HGetCommand) Cmd() redis.Cmder {
	return redis.NewStringCmd(contextTODO, command.name, command.key, command.field)
}

type HGetAllCommand struct {
	key string
	commonCommand
}

func NewHGetAllCommand(args []string) (Commander, error) {
	command := &HGetAllCommand{}
	command.init(args)
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	return command, nil
}

func (command *HGetAllCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *HGetAllCommand) Cmd() redis.Cmder {
	return redis.NewStringSliceCmd(contextTODO, command.name, command.key)
}

type HIncrByCommand struct {
	key       string
	field     string
	increment int64
	commonCommand
}

func NewHIncrByCommand(args []string) (Commander, error) {
	command := &HIncrByCommand{}
	command.init(args)
	if len(args) != 4 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	increment, err := strconv.ParseInt(args[3], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	command.key = args[1]
	command.field = args[2]
	command.increment = increment
	return command, nil
}

func (command *HIncrByCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *HIncrByCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.field, command.increment)
}

type HIncrByFloatCommand struct {
	key       string
	field     string
	increment float64
	commonCommand
}

func NewHIncrByFloatCommand(args []string) (Commander, error) {
	command := &HIncrByFloatCommand{}
	command.init(args)
	if len(args) != 4 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	increment, err := strconv.ParseFloat(args[3], 64)
	if err != nil {
		return nil, errInvalidFloat
	}
	command.key = args[1]
	command.field = args[2]
	command.increment = increment
	return command, nil
}

func (command *HIncrByFloatCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *HIncrByFloatCommand) Cmd() redis.Cmder {
	return redis.NewStringCmd(contextTODO, command.name, command.key, command.field, command.increment)
}

type HKeysCommand struct {
	key string
	commonCommand
}

func NewHKeysCommand(args []string) (Commander, error) {
	command := &HKeysCommand{}
	command.init(args)
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	return command, nil
}

func (command *HKeysCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *HKeysCommand) Cmd() redis.Cmder {
	return redis.NewStringSliceCmd(contextTODO, command.name, command.key)
}

type HLenCommand struct {
	key string
	commonCommand
}

func NewHLenCommand(args []string) (Commander, error) {
	command := &HLenCommand{}
	command.init(args)
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	return command, nil
}

func (command *HLenCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *HLenCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key)
}

type HMGetCommand struct {
	key    string
	fields []string
	commonCommand
}

func NewHMGetCommand(args []string) (Commander, error) {
	command := &HMGetCommand{}
	command.init(args)
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.fields = args[2:]
	return command, nil
}

func (command *HMGetCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *HMGetCommand) Cmd() redis.Cmder {
	return redis.NewSliceCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type HMSetCommand struct {
	key        string
	fieldPairs map[string]string
	commonCommand
}

func NewHMSetCommand(args []string) (Commander, error) {
	command := &HMSetCommand{}
	command.init(args)
	if (len(args) < 4) || (len(args)%2 != 0) {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.fieldPairs = make(map[string]string, (len(args)-2)/2)
	for i := 2; i < len(args)-1; i = i + 2 {
		field := args[i]
		value := args[i+1]
		command.fieldPairs[field] = value
	}
	return command, nil
}

func (command *HMSetCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *HMSetCommand) Cmd() redis.Cmder {
	return redis.NewStatusCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type HSetCommand struct {
	key        string
	fieldPairs map[string]string
	commonCommand
}

func NewHSetCommand(args []string) (Commander, error) {
	command := &HSetCommand{}
	command.init(args)
	if (len(args) < 4) || (len(args)%2 != 0) {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.fieldPairs = make(map[string]string, (len(args)-2)/2)
	for i := 2; i < len(args)-1; i = i + 2 {
		field := args[i]
		value := args[i+1]
		command.fieldPairs[field] = value
	}
	return command, nil
}

func (command *HSetCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *HSetCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type HSetNXCommand struct {
	key   string
	field string
	value string
	commonCommand
}

func NewHSetNXCommand(args []string) (Commander, error) {
	command := &HSetNXCommand{}
	command.init(args)
	if len(args) != 4 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.field = args[2]
	command.value = args[3]
	return command, nil
}

func (command *HSetNXCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *HSetNXCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.field, command.value)
}

type HStrlenCommand struct {
	key   string
	field string
	commonCommand
}

func NewHStrlenCommand(args []string) (Commander, error) {
	command := &HStrlenCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.field = args[2]
	return command, nil
}

func (command *HStrlenCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *HStrlenCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.field)
}

type HValsCommand struct {
	key string
	commonCommand
}

func NewHValsCommand(args []string) (Commander, error) {
	command := &HValsCommand{}
	command.init(args)
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	return command, nil
}

func (command *HValsCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *HValsCommand) Cmd() redis.Cmder {
	return redis.NewStringSliceCmd(contextTODO, command.name, command.key)
}
