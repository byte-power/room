package commands

import (
	"bytepower_room/utility"
	"strconv"
	"strings"

	"github.com/go-redis/redis/v8"
)

type ZAddCommand struct {
	key                string
	existMode          keyExistMode
	scoreCompare       *string
	returnChangedCount bool
	incr               bool
	scoreMembers       []string
	commonCommand
}

func NewZAddCommand(args []string) (Commander, error) {
	command := &ZAddCommand{}
	command.init(args)
	if len(args) < 4 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	options := args[2:]
	scoreStartIndex := command.parseOtherOptions(options)
	if scoreStartIndex >= len(options) {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	scoreMemberCount := len(options) - scoreStartIndex
	if scoreMemberCount%2 != 0 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	if command.incr && (scoreMemberCount != 2) {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	for index := scoreStartIndex; index < len(options)-1; index += 2 {
		score := options[index]
		if _, err := strconv.ParseFloat(score, 64); err != nil {
			return nil, errInvalidFloat
		}
		member := options[index+1]
		command.scoreMembers = append(command.scoreMembers, score, member)
	}

	return command, nil
}

func (command *ZAddCommand) parseOtherOptions(options []string) int {
	for index, option := range options {
		item := strings.ToLower(option)
		switch item {
		case "nx":
			command.existMode = keyExistModeNX
		case "xx":
			command.existMode = keyExistModeXX
		case "gt":
			command.scoreCompare = &option
		case "lt":
			command.scoreCompare = &option
		case "ch":
			command.returnChangedCount = true
		case "incr":
			command.incr = true
		default:
			return index
		}
	}
	return len(options)
}

func (command *ZAddCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *ZAddCommand) Cmd() redis.Cmder {
	args := utility.StringSliceToInterfaceSlice(command.args)
	if command.incr {
		return redis.NewStringCmd(contextTODO, args...)
	}
	return redis.NewIntCmd(contextTODO, args...)
}

type ZCardCommand struct {
	key string
	commonCommand
}

func NewZCardCommand(args []string) (Commander, error) {
	command := &ZCardCommand{}
	command.init(args)
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	return command, nil
}

func (command *ZCardCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *ZCardCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key)
}

type ZCountCommand struct {
	key string
	min string
	max string
	commonCommand
}

func NewZCountCommand(args []string) (Commander, error) {
	command := &ZCountCommand{}
	command.init(args)
	if len(args) != 4 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	if _, err := strconv.ParseFloat(args[2], 64); err != nil {
		return nil, errInvalidFloat
	}
	command.min = args[2]
	if _, err := strconv.ParseFloat(args[3], 64); err != nil {
		return nil, errInvalidFloat
	}
	command.max = args[3]
	return command, nil
}

func (command *ZCountCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *ZCountCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.min, command.max)
}

func isWithScores(s string) bool {
	return strings.ToLower(s) == "withscores"
}

func isLimit(s string) bool {
	return strings.ToLower(s) == "limit"
}

type ZDiffCommand struct {
	numKeys    int64
	keys       []string
	withScores bool
	commonCommand
}

func NewZDiffCommand(args []string) (Commander, error) {
	command := &ZDiffCommand{}
	command.init(args)
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	numKeys, err := strconv.ParseInt(args[1], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	command.numKeys = numKeys
	keys := args[2:]
	if int64(len(keys)) == numKeys {
		command.keys = keys
	} else if (int64(len(keys)) == numKeys+1) && isWithScores(keys[len(keys)-1]) {
		command.keys = keys[:len(keys)-1]
		command.withScores = true
	} else {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	return command, nil
}

func (command *ZDiffCommand) ReadKeys() []string {
	return command.keys
}

func (command *ZDiffCommand) Cmd() redis.Cmder {
	return redis.NewStringSliceCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type ZDiffStoreCommand struct {
	destKey    string
	numKeys    int64
	keys       []string
	withScores bool
	commonCommand
}

func NewZDiffStoreCommand(args []string) (Commander, error) {
	command := &ZDiffStoreCommand{}
	command.init(args)
	if len(args) < 4 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.destKey = args[1]
	numKeys, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	command.numKeys = numKeys
	keys := args[3:]
	if int64(len(keys)) == numKeys {
		command.keys = keys
	} else if (int64(len(keys)) == numKeys+1) && isWithScores(keys[len(keys)-1]) {
		command.keys = keys[:len(keys)-1]
		command.withScores = true
	} else {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	return command, nil
}

func (command *ZDiffStoreCommand) ReadKeys() []string {
	return command.keys
}

func (command *ZDiffStoreCommand) WriteKeys() []string {
	return []string{command.destKey}
}

func (command *ZDiffStoreCommand) Cmd() redis.Cmder {
	return redis.NewStringSliceCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type ZIncrByCommand struct {
	key       string
	increment float64
	member    string
	commonCommand
}

func NewZIncrByCommand(args []string) (Commander, error) {
	command := &ZIncrByCommand{}
	command.init(args)
	if len(args) != 4 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.member = args[3]
	increment, err := strconv.ParseFloat(args[2], 64)
	if err != nil {
		return nil, errInvalidFloat
	}
	command.increment = increment
	return command, nil
}

func (command *ZIncrByCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *ZIncrByCommand) Cmd() redis.Cmder {
	return redis.NewStringCmd(contextTODO, command.name, command.key, command.increment, command.member)
}

type ZLexCountCommand struct {
	key string
	min string
	max string
	commonCommand
}

func NewZLexCountCommand(args []string) (Commander, error) {
	command := &ZLexCountCommand{}
	command.init(args)
	if len(args) != 4 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.min = args[2]
	command.max = args[3]
	return command, nil
}

func (command *ZLexCountCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *ZLexCountCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.min, command.max)
}

type ZPopMaxCommand struct {
	key   string
	count int64
	commonCommand
}

func NewZPopMaxCommand(args []string) (Commander, error) {
	command := &ZPopMaxCommand{}
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
		command.count = count
	} else {
		command.count = 1
	}
	return command, nil
}

func (command *ZPopMaxCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *ZPopMaxCommand) Cmd() redis.Cmder {
	return redis.NewStringSliceCmd(contextTODO, command.name, command.key, command.count)
}

type ZPopMinCommand struct {
	key   string
	count int64
	commonCommand
}

func NewZPopMinCommand(args []string) (Commander, error) {
	command := &ZPopMinCommand{}
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
		command.count = count
	} else {
		command.count = 1
	}
	return command, nil
}

func (command *ZPopMinCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *ZPopMinCommand) Cmd() redis.Cmder {
	return redis.NewStringSliceCmd(contextTODO, command.name, command.key, command.count)
}

type ZRangeCommand struct {
	key        string
	startIndex int64
	stopIndex  int64
	withScores bool
	commonCommand
}

func NewZRangeCommand(args []string) (Commander, error) {
	command := &ZRangeCommand{}
	command.init(args)
	if (len(args) != 4) && (len(args) != 5) {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	startIndex, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	stopIndex, err := strconv.ParseInt(args[3], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	command.startIndex = startIndex
	command.stopIndex = stopIndex
	if len(args) == 5 {
		if isWithScores(args[4]) {
			command.withScores = true
		} else {
			return nil, errSyntaxError
		}
	}
	return command, nil
}

func (command *ZRangeCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *ZRangeCommand) Cmd() redis.Cmder {
	args := []interface{}{command.name, command.key, command.startIndex, command.stopIndex}
	if command.withScores {
		args = append(args, "withscores")
	}
	return redis.NewStringSliceCmd(contextTODO, args...)
}

type zRangeLimit struct {
	offset int64
	count  int64
}
type ZRangeByLexCommand struct {
	key   string
	min   string
	max   string
	limit *zRangeLimit
	commonCommand
}

func NewZRangeByLexCommand(args []string) (Commander, error) {
	command := &ZRangeByLexCommand{}
	command.init(args)
	if (len(args) != 4) && (len(args) != 7) {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.min = args[2]
	command.max = args[3]
	if len(args) == 7 {
		if err := command.parseLimitOptions(args[4:]); err != nil {
			return nil, err
		}
	}
	return command, nil
}

func (command *ZRangeByLexCommand) parseLimitOptions(options []string) error {
	if !isLimit(options[0]) {
		return errSyntaxError
	}
	offset, err := strconv.ParseInt(options[1], 10, 64)
	if err != nil {
		return errInvalidInteger
	}
	count, err := strconv.ParseInt(options[2], 10, 64)
	if err != nil {
		return errInvalidInteger
	}
	limit := &zRangeLimit{offset: offset, count: count}
	command.limit = limit
	return nil
}

func (command *ZRangeByLexCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *ZRangeByLexCommand) Cmd() redis.Cmder {
	return redis.NewStringSliceCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type ZRevRangeByLexCommand struct {
	key   string
	max   string
	min   string
	limit *zRangeLimit
	commonCommand
}

func NewZRevRangeByLexCommand(args []string) (Commander, error) {
	command := &ZRevRangeByLexCommand{}
	command.init(args)
	if (len(args) != 4) && (len(args) != 7) {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.max = args[2]
	command.min = args[3]
	if len(args) == 7 {
		if err := command.parseLimitOptions(args[4:]); err != nil {
			return nil, err
		}
	}
	return command, nil
}

func (command *ZRevRangeByLexCommand) parseLimitOptions(options []string) error {
	if !isLimit(options[0]) {
		return errSyntaxError
	}
	offset, err := strconv.ParseInt(options[1], 10, 64)
	if err != nil {
		return errInvalidInteger
	}
	count, err := strconv.ParseInt(options[2], 10, 64)
	if err != nil {
		return errInvalidInteger
	}
	limit := &zRangeLimit{offset: offset, count: count}
	command.limit = limit
	return nil
}

func (command *ZRevRangeByLexCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *ZRevRangeByLexCommand) Cmd() redis.Cmder {
	return redis.NewStringSliceCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type ZRangeByScoreCommand struct {
	key        string
	min        string
	max        string
	withScores bool
	limit      *zRangeLimit
	commonCommand
}

func NewZRangeByScoreCommand(args []string) (Commander, error) {
	command := &ZRangeByScoreCommand{}
	command.init(args)
	validCountOfArgs := []int{4, 5, 7, 8}
	if !utility.IntSliceContains(validCountOfArgs, len(args)) {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.min = args[2]
	command.max = args[3]
	if err := command.parseOtherOptions(args[4:]); err != nil {
		return nil, err
	}
	return command, nil
}

func (command *ZRangeByScoreCommand) parseOtherOptions(options []string) error {
	for len(options) != 0 {
		option := options[0]
		if isWithScores(option) {
			command.withScores = true
			options = options[1:]
		} else if isLimit(option) {
			if len(options) < 3 {
				return newWrongNumberOfArgumentsError(command.name)
			}
			offset, err := strconv.ParseInt(options[1], 10, 64)
			if err != nil {
				return errInvalidInteger
			}
			count, err := strconv.ParseInt(options[2], 10, 64)
			if err != nil {
				return errInvalidInteger
			}
			limit := &zRangeLimit{offset: offset, count: count}
			command.limit = limit
			options = options[3:]
		} else {
			return errSyntaxError
		}
	}
	return nil
}

func (command *ZRangeByScoreCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *ZRangeByScoreCommand) Cmd() redis.Cmder {
	return redis.NewStringSliceCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type ZRankCommand struct {
	key    string
	member string
	commonCommand
}

func NewZRankCommand(args []string) (Commander, error) {
	command := &ZRankCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.member = args[2]
	return command, nil
}

func (command *ZRankCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *ZRankCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.member)
}

type ZRemCommand struct {
	key     string
	members []string
	commonCommand
}

func NewZRemCommand(args []string) (Commander, error) {
	command := &ZRemCommand{}
	command.init(args)
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.members = args[2:]
	return command, nil
}

func (command *ZRemCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *ZRemCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type ZRemRangeByLexCommand struct {
	key string
	min string
	max string
	commonCommand
}

func NewZRemRangeByLexCommand(args []string) (Commander, error) {
	command := &ZRemRangeByLexCommand{}
	command.init(args)
	if len(args) != 4 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.min = args[2]
	command.max = args[3]
	return command, nil
}

func (command *ZRemRangeByLexCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *ZRemRangeByLexCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.min, command.max)
}

type ZRemRangeByRankCommand struct {
	key   string
	start int64
	stop  int64
	commonCommand
}

func NewZRemRangeByRankCommand(args []string) (Commander, error) {
	command := &ZRemRangeByRankCommand{}
	command.init(args)
	if len(args) != 4 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	start, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	stop, err := strconv.ParseInt(args[3], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	command.start = start
	command.stop = stop
	return command, nil
}

func (command *ZRemRangeByRankCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *ZRemRangeByRankCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.start, command.stop)
}

type ZRemRangeByScoreCommand struct {
	key string
	min string
	max string
	commonCommand
}

func NewZRemRangeByScoreCommand(args []string) (Commander, error) {
	command := &ZRemRangeByScoreCommand{}
	command.init(args)
	if len(args) != 4 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.min = args[2]
	command.max = args[3]
	return command, nil
}

func (command *ZRemRangeByScoreCommand) WriteKeys() []string {
	return []string{command.key}
}

func (command *ZRemRangeByScoreCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.min, command.max)
}

type ZRevRangeCommand struct {
	key        string
	startIndex int64
	stopIndex  int64
	withScores bool
	commonCommand
}

func NewZRevRangeCommand(args []string) (Commander, error) {
	command := &ZRevRangeCommand{}
	command.init(args)
	if (len(args) != 4) && (len(args) != 5) {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	startIndex, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	stopIndex, err := strconv.ParseInt(args[3], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	command.startIndex = startIndex
	command.stopIndex = stopIndex
	if len(args) == 5 {
		if isWithScores(args[4]) {
			command.withScores = true
		} else {
			return nil, errSyntaxError
		}
	}
	return command, nil
}

func (command *ZRevRangeCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *ZRevRangeCommand) Cmd() redis.Cmder {
	return redis.NewStringSliceCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type ZRevRangeByScoreCommand struct {
	key        string
	max        string
	min        string
	withScores bool
	limit      *zRangeLimit
	commonCommand
}

func NewZRevRangeByScoreCommand(args []string) (Commander, error) {
	command := &ZRevRangeByScoreCommand{}
	command.init(args)
	validCountOfArgs := []int{4, 5, 7, 8}
	if !utility.IntSliceContains(validCountOfArgs, len(args)) {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.max = args[2]
	command.min = args[3]
	if err := command.parseOtherOptions(args[4:]); err != nil {
		return nil, err
	}
	return command, nil
}

func (command *ZRevRangeByScoreCommand) parseOtherOptions(options []string) error {
	for len(options) != 0 {
		option := options[0]
		if isWithScores(option) {
			command.withScores = true
			options = options[1:]
		} else if isLimit(option) {
			if len(options) < 3 {
				return newWrongNumberOfArgumentsError(command.name)
			}
			offset, err := strconv.ParseInt(options[1], 10, 64)
			if err != nil {
				return errInvalidInteger
			}
			count, err := strconv.ParseInt(options[2], 10, 64)
			if err != nil {
				return errInvalidInteger
			}
			limit := &zRangeLimit{offset: offset, count: count}
			command.limit = limit
			options = options[3:]
		} else {
			return errSyntaxError
		}
	}
	return nil
}

func (command *ZRevRangeByScoreCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *ZRevRangeByScoreCommand) Cmd() redis.Cmder {
	return redis.NewStringSliceCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type ZRevRankCommand struct {
	key    string
	member string
	commonCommand
}

func NewZRevRankCommand(args []string) (Commander, error) {
	command := &ZRevRankCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.member = args[2]
	return command, nil
}

func (command *ZRevRankCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *ZRevRankCommand) Cmd() redis.Cmder {
	return redis.NewIntCmd(contextTODO, command.name, command.key, command.member)
}

type ZScoreCommand struct {
	key    string
	member string
	commonCommand
}

func NewZScoreCommand(args []string) (Commander, error) {
	command := &ZScoreCommand{}
	command.init(args)
	if len(args) != 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.member = args[2]
	return command, nil
}

func (command *ZScoreCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *ZScoreCommand) Cmd() redis.Cmder {
	return redis.NewStringCmd(contextTODO, command.name, command.key, command.member)
}

type ZMScoreCommand struct {
	key     string
	members []string
	commonCommand
}

func NewZMScoreCommand(args []string) (Commander, error) {
	command := &ZMScoreCommand{}
	command.init(args)
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.key = args[1]
	command.members = args[2:]
	return command, nil
}

func (command *ZMScoreCommand) ReadKeys() []string {
	return []string{command.key}
}

func (command *ZMScoreCommand) Cmd() redis.Cmder {
	return redis.NewSliceCmd(contextTODO, command.argsToInterfaceSlice()...)
}
