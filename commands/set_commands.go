package commands

import (
	"strconv"

	"github.com/go-redis/redis/v8"
)

type SAddCommand struct {
	key     string
	members []string
	commonCommand
}

func NewSAddCommand(args []string) (Commander, error) {
	command := &SAddCommand{}
	command.init(args)
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.members = args[2:]
	return command, nil
}

func (command *SAddCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *SAddCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type SCardCommand struct {
	key string
	commonCommand
}

func NewSCardCommand(args []string) (Commander, error) {
	command := &SCardCommand{}
	command.init(args)
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	return command, nil
}

func (command *SCardCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *SCardCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key)
}

type SDiffCommand struct {
	keys []string
	commonCommand
}

func NewSDiffCommand(args []string) (Commander, error) {
	command := &SDiffCommand{}
	command.init(args)
	if len(args) < 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.keys = args[1:]
	return command, nil
}

func (command *SDiffCommand) ReadKeys() []string {
	return command.keys
}

func (command *SDiffCommand) Cmd() redis.Cmder {
	return redis.NewStringSliceCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type SDiffStoreCommand struct {
	destKey    string
	sourceKeys []string
	commonCommand
}

func NewSDiffStoreCommand(args []string) (Commander, error) {
	command := &SDiffStoreCommand{}
	command.init(args)
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.destKey = args[1]
	command.sourceKeys = args[2:]
	return command, nil
}

func (command *SDiffStoreCommand) WriteKeys() []string {
	return []string{command.destKey}
}

func (command *SDiffStoreCommand) ReadKeys() []string {
	return command.sourceKeys
}

func (command *SDiffStoreCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type SInterCommand struct {
	keys []string
	commonCommand
}

func NewSInterCommand(args []string) (Commander, error) {
	command := &SInterCommand{}
	command.init(args)
	if len(args) == 1 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.keys = args[1:]
	return command, nil
}

func (command *SInterCommand) ReadKeys() []string {
	return command.keys
}

func (command *SInterCommand) Cmd() redis.Cmder {
	return redis.NewStringSliceCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type SInterStoreCommand struct {
	destKey    string
	sourceKeys []string
	commonCommand
}

func NewSInterStoreCommand(args []string) (Commander, error) {
	command := &SInterStoreCommand{}
	command.init(args)
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.destKey = args[1]
	command.sourceKeys = args[2:]
	return command, nil
}

func (command *SInterStoreCommand) WriteKeys() []string {
	return []string{command.destKey}
}

func (command *SInterStoreCommand) ReadKeys() []string {
	return command.sourceKeys
}

func (command *SInterStoreCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type SIsMemberCommand struct {
	key    string
	member string
	commonCommand
}

func NewSIsMemberCommand(args []string) (Commander, error) {
	command := &SIsMemberCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.member = args[2]
	return command, nil
}

func (command *SIsMemberCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *SIsMemberCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.member)
}

type SMIsMemberCommand struct {
	key     string
	members []string
	commonCommand
}

func NewSMIsMemberCommand(args []string) (Commander, error) {
	command := &SMIsMemberCommand{}
	command.init(args)
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.members = args[2:]
	return command, nil
}

func (command *SMIsMemberCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *SMIsMemberCommand) Cmd() redis.Cmder {
	return redis.NewIntSliceCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type SMembersCommand struct {
	key string
	commonCommand
}

func NewSMembersCommand(args []string) (Commander, error) {
	command := &SMembersCommand{}
	command.init(args)
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	return command, nil
}

func (command *SMembersCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *SMembersCommand) Cmd() redis.Cmder {
	return redis.NewStringSliceCmd(contextTODO, command.name, command.key)
}

type SMoveCommand struct {
	sourceKey string
	destKey   string
	member    string
	commonCommand
}

func NewSMoveCommand(args []string) (Commander, error) {
	command := &SMoveCommand{}
	command.init(args)
	if len(args) != 4 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.sourceKey = args[1]
	command.destKey = args[2]
	command.member = args[3]
	return command, nil
}

func (command *SMoveCommand) WriteKeys() []string {
	return []string{command.sourceKey, command.destKey}
}

func (command *SMoveCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.sourceKey, command.destKey, command.member)
}

type SPopCommand struct {
	key   string
	count *int64
	commonCommand
}

func NewSPopCommand(args []string) (Commander, error) {
	command := &SPopCommand{}
	command.init(args)
	if (len(args) != 2) && (len(args) != 3) {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	if len(args) == 3 {
		count, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return nil, errInvalidInteger
		}
		if count < 0 {
			return nil, errInvalidIndex
		}
		command.count = &count
	}
	return command, nil
}

func (command *SPopCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *SPopCommand) Cmd() redis.Cmder {
	if command.count != nil {
		return redis.NewStringSliceCmd(contextTODO, command.argsToInterfaceSlice()...)
	}
	return redis.NewStringCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type SRandMemberCommand struct {
	key   string
	count *int64
	commonCommand
}

func NewSRandMemberCommand(args []string) (Commander, error) {
	command := &SRandMemberCommand{}
	command.init(args)
	if (len(args) != 2) && (len(args) != 3) {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	if len(args) == 3 {
		count, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return nil, errInvalidInteger
		}
		command.count = &count
	}
	return command, nil
}

func (command *SRandMemberCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *SRandMemberCommand) Cmd() redis.Cmder {
	if command.count != nil {
		return redis.NewStringSliceCmd(contextTODO, command.argsToInterfaceSlice()...)
	}
	return redis.NewStringCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type SRemCommand struct {
	key     string
	members []string
	commonCommand
}

func NewSRemCommand(args []string) (Commander, error) {
	command := &SRemCommand{}
	command.init(args)
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.members = args[2:]
	return command, nil
}

func (command *SRemCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *SRemCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type SUnionCommand struct {
	keys []string
	commonCommand
}

func NewSUnionCommand(args []string) (Commander, error) {
	command := &SUnionCommand{}
	command.init(args)
	if len(args) < 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.keys = args[1:]
	return command, nil
}

func (command *SUnionCommand) ReadKeys() []string {
	return command.keys
}

func (command *SUnionCommand) Cmd() redis.Cmder {
	return redis.NewStringSliceCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type SUnionStoreCommand struct {
	destKey    string
	sourceKeys []string
	commonCommand
}

func NewSUnionStoreCommand(args []string) (Commander, error) {
	command := &SUnionStoreCommand{}
	command.init(args)
	if len(args) <= 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.destKey = args[1]
	command.sourceKeys = args[2:]
	return command, nil
}

func (command *SUnionStoreCommand) ReadKeys() []string {
	return command.sourceKeys
}

func (command *SUnionStoreCommand) WriteKeys() []string {
	return []string{command.destKey}
}

func (command *SUnionStoreCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.argsToInterfaceSlice()...)
}
