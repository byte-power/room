package commands

import (
	"strconv"
	"strings"

	"github.com/go-redis/redis/v8"
)

type LIndexCommand struct {
	key   string
	index int64
	commonCommand
}

func NewLIndexCommand(args []string) (Commander, error) {
	command := &LIndexCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	index, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	command.key = args[1]
	command.index = index
	return command, nil
}

func (command *LIndexCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *LIndexCommand) Cmd() redis.Cmder {
	return redis.NewStringCmd(contextTODO, command.name, command.key, command.index)
}

type LInsertCommand struct {
	key      string
	position string
	pivot    string
	element  string
	commonCommand
}

func NewLInsertCommand(args []string) (Commander, error) {
	command := &LInsertCommand{}
	command.init(args)
	if len(args) != 5 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	position := strings.ToLower(args[2])
	if (position != "before") && (position != "after") {
		return nil, errSyntaxError
	}
	command.key = args[1]
	command.position = position
	command.pivot = args[3]
	command.element = args[4]
	return command, nil
}

func (command *LInsertCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *LInsertCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.position, command.pivot, command.element)
}

type LLenCommand struct {
	key string
	commonCommand
}

func NewLLenCommand(args []string) (Commander, error) {
	command := &LLenCommand{}
	command.init(args)
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	return command, nil
}

func (command *LLenCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *LLenCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key)
}

type LPopCommand struct {
	key string
	commonCommand
}

func NewLPopCommand(args []string) (Commander, error) {
	command := &LPopCommand{}
	command.init(args)
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	return command, nil
}

func (command *LPopCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *LPopCommand) Cmd() redis.Cmder {
	return redis.NewStringCmd(contextTODO, command.name, command.key)
}

type LPosCommand struct {
	key     string
	element string
	rank    *int64
	count   *int64
	maxLen  *int64
	commonCommand
}

func NewLPosCommand(args []string) (Commander, error) {
	command := &LPosCommand{}
	command.init(args)
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.element = args[2]
	if err := command.parseOtherOptions(args[3:]); err != nil {
		return nil, err
	}
	return command, nil
}

func (command *LPosCommand) parseOtherOptions(options []string) error {
	if len(options)%2 != 0 {
		return errSyntaxError
	}
	for len(options) > 0 {
		switch strings.ToLower(options[0]) {
		case "rank":
			rank, err := strconv.ParseInt(options[1], 10, 64)
			if err != nil {
				return errInvalidInteger
			}
			command.rank = &rank
			options = options[2:]
		case "count":
			count, err := strconv.ParseInt(options[1], 10, 64)
			if err != nil {
				return errInvalidInteger
			}
			command.count = &count
			options = options[2:]
		case "maxlen":
			maxLen, err := strconv.ParseInt(options[1], 10, 64)
			if err != nil {
				return errInvalidInteger
			}
			command.maxLen = &maxLen
			options = options[2:]
		default:
			return errSyntaxError
		}
	}
	return nil
}

func (command *LPosCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *LPosCommand) Cmd() redis.Cmder {
	if command.count != nil {
		return redis.NewIntSliceCmd(contextTODO, command.argsToInterfaceSlice()...)
	}
	return redis.NewIntCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type LPushCommand struct {
	key      string
	elements []string
	commonCommand
}

func NewLPushCommand(args []string) (Commander, error) {
	command := &LPushCommand{}
	command.init(args)
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.elements = args[2:]
	return command, nil
}

func (command *LPushCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *LPushCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type LPushXCommand struct {
	key      string
	elements []string
	commonCommand
}

func NewLPushXCommand(args []string) (Commander, error) {
	command := &LPushXCommand{}
	command.init(args)
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.elements = args[2:]
	return command, nil
}

func (command *LPushXCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *LPushXCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type LRangeCommand struct {
	key   string
	start int64
	stop  int64
	commonCommand
}

func NewLRangeCommand(args []string) (Commander, error) {
	command := &LRangeCommand{}
	command.init(args)
	if len(args) != 4 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	start, startErr := strconv.ParseInt(args[2], 10, 64)
	stop, stopErr := strconv.ParseInt(args[3], 10, 64)
	if (startErr != nil) || (stopErr != nil) {
		return nil, errInvalidInteger
	}
	command.key = args[1]
	command.start = start
	command.stop = stop
	return command, nil
}

func (command *LRangeCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *LRangeCommand) Cmd() redis.Cmder {
	return redis.NewStringSliceCmd(contextTODO, command.name, command.key, command.start, command.stop)
}

type LRemCommand struct {
	key     string
	count   int64
	element string
	commonCommand
}

func NewLRemCommand(args []string) (Commander, error) {
	command := &LRemCommand{}
	command.init(args)
	if len(args) != 4 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	count, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	command.key = args[1]
	command.count = count
	command.element = args[3]
	return command, nil
}

func (command *LRemCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *LRemCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.count, command.element)
}

type LSetCommand struct {
	key     string
	index   int64
	element string
	commonCommand
}

func NewLSetCommand(args []string) (Commander, error) {
	command := &LSetCommand{}
	command.init(args)
	if len(args) != 4 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	index, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	command.key = args[1]
	command.index = index
	command.element = args[3]
	return command, nil
}

func (command *LSetCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *LSetCommand) Cmd() redis.Cmder {
	return redis.NewStatusCmd(contextTODO, command.name, command.key, command.index, command.element)
}

type LTrimCommand struct {
	key   string
	start int64
	stop  int64
	commonCommand
}

func NewLTrimCommand(args []string) (Commander, error) {
	command := &LTrimCommand{}
	command.init(args)
	if len(args) != 4 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	start, startErr := strconv.ParseInt(args[2], 10, 64)
	stop, stopErr := strconv.ParseInt(args[3], 10, 64)
	if (startErr != nil) || (stopErr != nil) {
		return nil, errInvalidInteger
	}
	command.key = args[1]
	command.start = start
	command.stop = stop
	return command, nil
}

func (command *LTrimCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *LTrimCommand) Cmd() redis.Cmder {
	return redis.NewStatusCmd(contextTODO, command.name, command.key, command.start, command.stop)
}

type RPopCommand struct {
	key string
	commonCommand
}

func NewRPopCommand(args []string) (Commander, error) {
	command := &RPopCommand{}
	command.init(args)
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	return command, nil
}

func (command *RPopCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *RPopCommand) Cmd() redis.Cmder {
	return redis.NewStringCmd(contextTODO, command.name, command.key)
}

type RPopLPushCommand struct {
	sourceKey string
	destKey   string
	commonCommand
}

func NewRPopLPushCommand(args []string) (Commander, error) {
	command := &RPopLPushCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.sourceKey = args[1]
	command.destKey = args[2]
	return command, nil
}

func (command *RPopLPushCommand) WriteKeys() []string {
	return []string{command.sourceKey, command.destKey}
}

func (command *RPopLPushCommand) Cmd() redis.Cmder {
	return redis.NewStringCmd(contextTODO, command.name, command.sourceKey, command.destKey)
}

type LMoveCommand struct {
	sourceKey string
	destKey   string
	whereFrom string
	whereTo   string
	commonCommand
}

func NewLMoveCommand(args []string) (Commander, error) {
	command := &LMoveCommand{}
	command.init(args)
	if len(args) != 5 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	whereFrom := strings.ToLower(args[3])
	whereTo := strings.ToLower(args[4])
	if ((whereFrom != "left") && (whereFrom != "right")) || ((whereTo != "left") && (whereTo != "right")) {
		return nil, errSyntaxError
	}
	command.sourceKey = args[1]
	command.destKey = args[2]
	command.whereFrom = whereFrom
	command.whereTo = whereTo
	return command, nil
}

func (command *LMoveCommand) WriteKeys() []string {
	return []string{command.sourceKey, command.destKey}
}

func (command *LMoveCommand) Cmd() redis.Cmder {
	return redis.NewStringCmd(contextTODO, command.name, command.sourceKey, command.destKey, command.whereFrom, command.whereTo)
}

type RPushCommand struct {
	key      string
	elements []string
	commonCommand
}

func NewRPushCommand(args []string) (Commander, error) {
	command := &RPushCommand{}
	command.init(args)
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.elements = args[2:]
	return command, nil
}

func (command *RPushCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *RPushCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type RPushXCommand struct {
	key      string
	elements []string
	commonCommand
}

func NewRPushXCommand(args []string) (Commander, error) {
	command := &RPushXCommand{}
	command.init(args)
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.elements = args[2:]
	return command, nil
}

func (command *RPushXCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *RPushXCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.argsToInterfaceSlice()...)
}
