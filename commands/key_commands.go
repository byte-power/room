package commands

import (
	"strconv"

	"github.com/go-redis/redis/v8"
)

type DelCommand struct {
	keys []string
	commonCommand
}

func NewDelCommand(args []string) (Commander, error) {
	command := &DelCommand{}
	command.init(args)
	if len(args) < 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.keys = args[1:]
	return command, nil
}

func (command *DelCommand) WriteKeys() []string {
	return command.keys
}

func (command *DelCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type ExistsCommand struct {
	keys []string
	commonCommand
}

func NewExistsCommand(args []string) (Commander, error) {
	command := &ExistsCommand{}
	command.init(args)
	if len(args) < 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.keys = args[1:]
	return command, nil
}

func (command *ExistsCommand) ReadKeys() []string {
	return command.keys
}

func (command *ExistsCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type ExpireCommand struct {
	key     string
	seconds int64
	commonCommand
}

func NewExpireCommand(args []string) (Commander, error) {
	command := &ExpireCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	seconds, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	command.seconds = seconds
	return command, nil
}

func (command *ExpireCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *ExpireCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type ExpireAtCommand struct {
	key       string
	timestamp int64
	commonCommand
}

func NewExpireAtCommand(args []string) (Commander, error) {
	command := &ExpireAtCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	timestamp, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	command.timestamp = timestamp
	return command, nil
}

func (command *ExpireAtCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *ExpireAtCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.timestamp)
}

type PersistCommand struct {
	key string
	commonCommand
}

func NewPersistCommand(args []string) (Commander, error) {
	command := &PersistCommand{}
	command.init(args)
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	return command, nil
}

func (command *PersistCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *PersistCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key)
}

type PExpireCommand struct {
	key          string
	milliseconds int64
	commonCommand
}

func NewPExpireCommand(args []string) (Commander, error) {
	command := &PExpireCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	milliseconds, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	command.milliseconds = milliseconds
	return command, nil
}

func (command *PExpireCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *PExpireCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.milliseconds)
}

type PExpireAtCommand struct {
	key         string
	msTimestamp int64
	commonCommand
}

func NewPExpireAtCommand(args []string) (Commander, error) {
	command := &PExpireAtCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	msTimestamp, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	command.msTimestamp = msTimestamp
	return command, nil
}

func (command *PExpireAtCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *PExpireAtCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.msTimestamp)
}

type PTTLCommand struct {
	key string
	commonCommand
}

func NewPTTLCommand(args []string) (Commander, error) {
	command := &PTTLCommand{}
	command.init(args)
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	return command, nil
}

func (command *PTTLCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *PTTLCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key)
}

type RenameCommand struct {
	key    string
	newKey string
	commonCommand
}

func NewRenameCommand(args []string) (Commander, error) {
	command := &RenameCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.newKey = args[2]
	return command, nil
}

func (command *RenameCommand) WriteKeys() []string {
	return []string{command.key, command.newKey}
}

func (command *RenameCommand) Cmd() redis.Cmder {
	return redis.NewStatusCmd(contextTODO, command.name, command.key, command.newKey)
}

type RenameNXCommand struct {
	key    string
	newKey string
	commonCommand
}

func NewRenameNXCommand(args []string) (Commander, error) {
	command := &RenameNXCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.newKey = args[2]
	return command, nil
}

func (command *RenameNXCommand) WriteKeys() []string {
	return []string{command.key, command.newKey}
}

func (command *RenameNXCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.newKey)
}

type TTLCommand struct {
	key string
	commonCommand
}

func NewTTLCommand(args []string) (Commander, error) {
	command := &TTLCommand{}
	command.init(args)
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	return command, nil
}

func (command *TTLCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *TTLCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key)
}

type TypeCommand struct {
	key string
	commonCommand
}

func NewTypeCommand(args []string) (Commander, error) {
	command := &TypeCommand{}
	command.init(args)
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	return command, nil
}

func (command *TypeCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *TypeCommand) Cmd() redis.Cmder {
	return redis.NewStatusCmd(contextTODO, command.name, command.key)
}
