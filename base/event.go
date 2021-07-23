package base

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrEventKeyEmpty = errors.New("key is empty")
)

type KeyAccessMode string

const (
	KeyAccessModeRead  KeyAccessMode = "read"
	KeyAccessModeWrite KeyAccessMode = "write"
)

type Event struct {
	Key        string        `json:"key"`
	AccessMode KeyAccessMode `json:"access_mode"`
	AccessTime time.Time     `json:"access_time"`
}

func NewEvent(key string, accessMode KeyAccessMode, accessTime time.Time) (Event, error) {
	if key == "" {
		return Event{}, ErrEventKeyEmpty
	}
	if accessMode == "" {
		return Event{}, ErrEventAccessModeEmpty
	}
	if accessTime.IsZero() {
		return Event{}, ErrEventAccessTimeEmpty
	}
	return Event{
		Key:        key,
		AccessMode: accessMode,
		AccessTime: accessTime,
	}, nil
}

func (event Event) String() string {
	return fmt.Sprintf(
		"Event[key=%s, access_mode=%s, access_time=%v]",
		event.Key, event.AccessMode, event.AccessTime)
}
