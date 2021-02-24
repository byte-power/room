package commands

import (
	"errors"
	"fmt"
	"strings"
)

func newWrongNumberOfArgumentsError(command string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", command)
}

func newUnknownCommand(command string, args []string) error {
	argSlice := []string{}
	for _, arg := range args {
		argSlice = append(argSlice, fmt.Sprintf("`%s`", arg))
	}
	return fmt.Errorf(
		"ERR unknown command `%s`, with args beginning with: %s",
		command, strings.Join(argSlice, ","),
	)
}

var (
	errSyntaxError    = errors.New("ERR syntax error")
	errEmptyCommand   = errors.New("ERR empty command")
	errInvalidInteger = errors.New("ERR value is not an integer or out of range")
	errInvalidFloat   = errors.New("ERR value is not a valid float")
	errInvalidOffset  = errors.New("ERR offset is out of range")
	errInvalidIndex   = errors.New("ERR index out of range")
)
