package commands

import (
	"strconv"
	"strings"

	"github.com/go-redis/redis/v8"
)

type keyExistMode string

const (
	keyExistModeNX keyExistMode = "nx"
	keyExistModeXX keyExistMode = "xx"
)

type SetCommand struct {
	key        string
	value      string
	expire     int64
	expireUnit string
	existMode  keyExistMode
	returnOld  bool
	commonCommand
}

func NewSetCommand(args []string) (Commander, error) {
	command := &SetCommand{}
	command.init(args)
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.value = args[2]
	if len(args) > 3 {
		if err := command.parseOtherOptions(args[3:]); err != nil {
			return nil, err
		}
	}
	return command, nil
}

func (command *SetCommand) parseOtherOptions(options []string) error {
	for len(options) != 0 {
		item := strings.ToLower(options[0])
		switch item {
		case "ex", "px":
			if len(options) < 2 {
				return errSyntaxError
			}
			command.expireUnit = item
			i, err := strconv.Atoi(options[1])
			if err != nil {
				return errInvalidInteger
			}
			command.expire = int64(i)
			options = options[2:]
		case "keepttl":
			command.expire = -1
			options = options[1:]
		case "nx":
			command.existMode = keyExistModeNX
			options = options[1:]
		case "xx":
			command.existMode = keyExistModeXX
			options = options[1:]
		case "get":
			command.returnOld = true
			options = options[1:]
		default:
			return errSyntaxError
		}
	}
	return nil
}

func (command *SetCommand) Cmd() redis.Cmder {
	if command.returnOld {
		return redis.NewStringCmd(contextTODO, command.argsToInterfaceSlice()...)
	}
	return redis.NewStatusCmd(contextTODO, command.argsToInterfaceSlice()...)
}

func (command *SetCommand) ReadKeys() []string {
	return []string{}
}

func (command *SetCommand) WriteKeys() []string {
	return []string{command.key}
}

type GetCommand struct {
	key string
	commonCommand
}

func NewGetCommand(args []string) (Commander, error) {
	command := &GetCommand{}
	command.init(args)
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	return command, nil
}

func (command *GetCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *GetCommand) WriteKeys() []string {
	return []string{}
}

func (command *GetCommand) Cmd() redis.Cmder {
	return redis.NewStringCmd(contextTODO, command.name, command.key)
}

type AppendCommand struct {
	key   string
	value string
	commonCommand
}

func NewAppendCommand(args []string) (Commander, error) {
	command := &AppendCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.value = args[2]
	return command, nil
}

func (command *AppendCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *AppendCommand) ReadKeys() []string {
	return []string{}
}

func (command *AppendCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.value)
}

type DecrCommand struct {
	key string
	commonCommand
}

func NewDecrCommand(args []string) (Commander, error) {
	command := &DecrCommand{}
	command.init(args)
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	return command, nil
}

func (command *DecrCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *DecrCommand) ReadKeys() []string {
	return []string{}
}

func (command *DecrCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key)
}

type IncrCommand struct {
	key string
	commonCommand
}

func NewIncrCommand(args []string) (Commander, error) {
	command := &IncrCommand{}
	command.init(args)
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	return command, nil
}

func (command *IncrCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *IncrCommand) ReadKeys() []string {
	return []string{}
}

func (command *IncrCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key)
}

type DecrByCommand struct {
	key       string
	decrement int64
	commonCommand
}

func NewDecrByCommand(args []string) (Commander, error) {
	command := &DecrByCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	decrement, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	command.key = args[1]
	command.decrement = decrement
	return command, nil
}

func (command *DecrByCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *DecrByCommand) ReadKeys() []string {
	return []string{}
}

func (command *DecrByCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.decrement)
}

type IncrByCommand struct {
	key       string
	increment int64
	commonCommand
}

func NewIncrByCommand(args []string) (Commander, error) {
	command := &IncrByCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	increment, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	command.key = args[1]
	command.increment = increment
	return command, nil
}

func (command *IncrByCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *IncrByCommand) ReadKeys() []string {
	return []string{}
}

func (command *IncrByCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.increment)
}

type GetRangeCommand struct {
	key   string
	start int64
	end   int64
	commonCommand
}

func NewGetRangeCommand(args []string) (Commander, error) {
	command := &GetRangeCommand{}
	command.init(args)
	if len(args) != 4 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	start, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	end, err := strconv.ParseInt(args[3], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	command.key = args[1]
	command.start = start
	command.end = end
	return command, nil
}

func (command *GetRangeCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *GetRangeCommand) Cmd() redis.Cmder {
	return redis.NewStringCmd(contextTODO, command.name, command.key, command.start, command.end)
}

type GetSetCommand struct {
	key   string
	value string
	commonCommand
}

func NewGetSetCommand(args []string) (Commander, error) {
	command := &GetSetCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.value = args[2]
	return command, nil
}

func (command *GetSetCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *GetSetCommand) Cmd() redis.Cmder {
	return redis.NewStringCmd(contextTODO, command.name, command.key, command.value)
}

type IncrByFloatCommand struct {
	key       string
	increment float64
	commonCommand
}

func NewIncrByFloatCommand(args []string) (Commander, error) {
	command := &IncrByFloatCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	increment, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		return nil, errInvalidFloat
	}
	command.key = args[1]
	command.increment = increment
	return command, nil
}

func (command *IncrByFloatCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *IncrByFloatCommand) Cmd() redis.Cmder {
	return redis.NewStringCmd(contextTODO, command.name, command.key, command.increment)

}

type MGetCommand struct {
	keys []string
	commonCommand
}

func NewMGetCommand(args []string) (Commander, error) {
	command := &MGetCommand{}
	command.init(args)
	if len(args) < 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.keys = args[1:]
	return command, nil
}

func (command *MGetCommand) ReadKeys() []string {
	return command.keys
}

func (command *MGetCommand) Cmd() redis.Cmder {
	return redis.NewSliceCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type MSetCommand struct {
	keys   []string
	values []string
	commonCommand
}

func NewMSetCommand(args []string) (Commander, error) {
	command := &MSetCommand{}
	command.init(args)
	if (len(args) < 3) || (len(args)%2 != 1) {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	for i := 1; i < len(args)-1; i = +2 {
		command.keys = append(command.keys, args[i])
		command.values = append(command.values, args[i+1])
	}
	return command, nil
}

func (command *MSetCommand) WriteKeys() []string {
	return command.keys
}

func (command *MSetCommand) Cmd() redis.Cmder {
	return redis.NewStatusCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type MSetNXCommand struct {
	keys   []string
	values []string
	commonCommand
}

func NewMSetNXCommand(args []string) (Commander, error) {
	command := &MSetNXCommand{}
	command.init(args)
	if (len(args) < 3) || (len(args)%2 != 1) {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	for i := 1; i < len(args)-1; i = +2 {
		command.keys = append(command.keys, args[i])
		command.values = append(command.values, args[i+1])
	}
	return command, nil
}

func (command *MSetNXCommand) WriteKeys() []string {
	return command.keys
}

func (command *MSetNXCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type PSetEXCommand struct {
	key          string
	value        string
	milliseconds int64
	commonCommand
}

func NewPSetEXCommand(args []string) (Commander, error) {
	command := &PSetEXCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	d, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	command.key = args[1]
	command.value = args[3]
	command.milliseconds = d
	return command, nil
}

func (command *PSetEXCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *PSetEXCommand) Cmd() redis.Cmder {
	return redis.NewStatusCmd(contextTODO, command.name, command.key, command.milliseconds, command.value)
}

type SetEXCommand struct {
	key     string
	value   string
	seconds int64
	CommandCommand
}

func NewSetEXCommand(args []string) (Commander, error) {
	command := &SetEXCommand{}
	command.init(args)
	if len(args) != 4 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	d, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	command.key = args[1]
	command.value = args[3]
	command.seconds = d
	return command, nil
}

func (command *SetEXCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *SetEXCommand) Cmd() redis.Cmder {
	return redis.NewStatusCmd(contextTODO, command.name, command.key, command.seconds, command.value)
}

type SetNXCommand struct {
	key   string
	value string
	commonCommand
}

func NewSetNXCommand(args []string) (Commander, error) {
	command := &SetNXCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.value = args[2]
	return command, nil
}

func (command *SetNXCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *SetNXCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.value)
}

type SetRangeCommand struct {
	key    string
	value  string
	offset int64
	commonCommand
}

func NewSetRangeCommand(args []string) (Commander, error) {
	command := &SetRangeCommand{}
	command.init(args)
	if len(args) != 4 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	offset, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	if offset < 0 {
		return nil, errInvalidOffset
	}
	command.key = args[1]
	command.value = args[3]
	command.offset = offset
	return command, nil
}

func (command *SetRangeCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *SetRangeCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.offset, command.value)
}

type StrlenCommand struct {
	key string
	commonCommand
}

func NewStrlenCommand(args []string) (Commander, error) {
	command := &StrlenCommand{}
	command.init(args)
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	return command, nil
}

func (command *StrlenCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *StrlenCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key)
}
