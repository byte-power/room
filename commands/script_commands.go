package commands

import (
	"strconv"

	"github.com/go-redis/redis/v8"
)

type EvalCommand struct {
	script     string
	keys       []string
	scriptArgs []string
	commonCommand
}

func NewEvalCommand(args []string) (Commander, error) {
	command := &EvalCommand{}
	command.init(args)
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.script = args[1]
	keysCount, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	keysAndArgs := args[2:]
	if len(keysAndArgs) < int(keysCount) {
		return nil, errScriptNumberOfKeysGreaterThanArgs
	}
	command.keys = keysAndArgs[:keysCount]
	if len(keysAndArgs) > int(keysCount) {
		command.scriptArgs = keysAndArgs[keysCount:]
	}
	return command, nil
}

func (command *EvalCommand) WriteKeys() []string {
	return command.keys
}

func (command *EvalCommand) Cmd() redis.Cmder {
	return redis.NewCmd(contextTODO, command.argsToInterfaceSlice()...)
}

type EvalShaCommand struct {
	scriptSha  string
	keys       []string
	scriptArgs []string
	commonCommand
}

func NewEvalShaCommand(args []string) (Commander, error) {
	command := &EvalShaCommand{}
	command.init(args)
	if len(args) < 3 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.scriptSha = args[1]
	keysCount, err := strconv.ParseInt(args[2], 10, 64)
	if err != nil {
		return nil, errInvalidInteger
	}
	keysAndArgs := args[2:]
	if len(keysAndArgs) < int(keysCount) {
		return nil, errScriptNumberOfKeysGreaterThanArgs
	}
	command.keys = keysAndArgs[:keysCount]
	if len(keysAndArgs) > int(keysCount) {
		command.scriptArgs = keysAndArgs[keysCount:]
	}
	return command, nil
}

func (command *EvalShaCommand) WriteKeys() []string {
	return command.keys
}

func (command *EvalShaCommand) Cmd() redis.Cmder {
	return redis.NewCmd(contextTODO, command.argsToInterfaceSlice()...)
}
