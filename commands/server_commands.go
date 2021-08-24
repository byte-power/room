package commands

import (
	"bytepower_room/utility"

	"github.com/go-redis/redis/v8"
)

type CommandCommand struct {
	commonCommand
}

func NewCommandCommand(args []string) (Commander, error) {
	command := &CommandCommand{}
	command.init(args)
	if len(args) != 1 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	return command, nil
}

func (command *CommandCommand) ReadKeys() []string {
	return []string{}
}

func (command *CommandCommand) WriteKeys() []string {
	return []string{}
}

func (command *CommandCommand) Cmd() redis.Cmder {
	return redis.NewCommandsInfoCmd(contextTODO, command.name)
}

// func (command *CommandCommand) Exec() RESPData {
// 	client := base.GetRedisCluster()
// 	commands, err := client.Command(contextTODO).Result()
// 	if err != nil {
// 		return RESPData{DataType: ErrorRespType, Value: err}
// 	}
// 	data := RESPData{DataType: ArrayRespType}
// 	value := make([]RESPData, 0)
// 	for name, cmd := range commands {
// 		if _, ok := supportedCommands[strings.ToLower(name)]; ok {
// 			value = append(value, convertCommandInfoToRESPData(cmd))
// 		}
// 	}
// 	data.Value = value
// 	return data
// }

func convertCommandInfoToRESPData(data *redis.CommandInfo) RESPData {
	respData := RESPData{DataType: ArrayRespType}
	value := make([]RESPData, 6)
	// Name
	value[0] = RESPData{DataType: SimpleStringRespType, Value: data.Name}
	// Arity
	value[1] = RESPData{DataType: IntegerRespType, Value: int64(data.Arity)}
	// Flags
	value[2] = RESPData{DataType: ArrayRespType}
	flags := make([]RESPData, len(data.Flags))
	for index, flag := range data.Flags {
		flags[index] = RESPData{DataType: SimpleStringRespType, Value: flag}
	}
	value[2].Value = flags
	// FirstKeyPos
	value[3] = RESPData{DataType: IntegerRespType, Value: int64(data.FirstKeyPos)}
	// LastKeyPos
	value[4] = RESPData{DataType: IntegerRespType, Value: int64(data.LastKeyPos)}
	// StepCount
	value[5] = RESPData{DataType: IntegerRespType, Value: int64(data.StepCount)}

	respData.Value = value
	return respData
}

type EchoCommand struct {
	message string
	commonCommand
}

func NewEchoCommand(args []string) (Commander, error) {
	command := &EchoCommand{}
	command.init(args)
	if len(args) != 2 {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	command.message = args[1]
	return command, nil
}

func (command *EchoCommand) Cmd() redis.Cmder {
	return redis.NewStringCmd(contextTODO, command.name, command.message)
}

type PingCommand struct {
	message *string
	commonCommand
}

func NewPingCommand(args []string) (Commander, error) {
	command := &PingCommand{}
	command.init(args)
	if !utility.IntSliceContains([]int{1, 2}, len(args)) {
		return nil, newWrongNumberOfArgumentsError(command.name)
	}
	if len(args) == 2 {
		command.message = &args[1]
	}
	return command, nil
}

func (command *PingCommand) Cmd() redis.Cmder {
	if command.message == nil {
		return redis.NewStatusCmd(contextTODO, command.name)
	}
	return redis.NewStringCmd(contextTODO, command.name, *command.message)
}
