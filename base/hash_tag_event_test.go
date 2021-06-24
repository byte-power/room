package base

import (
	"bytepower_room/utility"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func testInitHashTagEventService() *HashTagEventService {
	loggerConfig := map[string]interface{}{"console": map[string]interface{}{"level": "debug"}}
	logger, _ := parseLogger(
		"room", "event_service",
		loggerConfig)
	metric, _ := InitMetric(MetricConfig{Host: "localhost"})
	hashTagEventConfig := HashTagEventServiceConfig{EventReport: HashTagEventReportConfig{URL: "localhost"}}
	service, _ := NewHashTagEventService(hashTagEventConfig, logger, metric)
	return service
}

func TestHashTagEventAggregateEvent(t *testing.T) {
	service := testInitHashTagEventService()

	hashTag := "abc"
	keys := []string{"a{abc}", "b{abc}"}
	currentTime := time.Now()
	event, _ := NewHashTagEvent(hashTag, keys, HashTagAccessModeRead, currentTime)
	service.aggregateEvent(event)
	assert.Equal(t, 1, len(service.events))
	assert.Equal(t, HashTagAccessModeRead, service.events[hashTag].AccessMode)
	assert.Equal(t, currentTime, service.events[hashTag].AccessTime)
	assert.ElementsMatch(t, keys, service.events[hashTag].Keys.ToSlice())

	currentTime = time.Now()
	keys2 := []string{"c{abc}", "d{abc}"}
	event, _ = NewHashTagEvent(hashTag, keys2, HashTagAccessModeWrite, currentTime)
	service.aggregateEvent(event)
	assert.Equal(t, 1, len(service.events))
	assert.Equal(t, HashTagAccessModeWrite, service.events[hashTag].AccessMode)
	assert.Equal(t, currentTime, service.events[hashTag].AccessTime)
	assert.ElementsMatch(t, append(keys, keys2...), service.events[hashTag].Keys.ToSlice())
}

func TestHashTagEventCollectEvent(t *testing.T) {
	service := testInitHashTagEventService()
	events := []HashTagEvent{
		{
			HashTag:    "a",
			Keys:       utility.NewStringSet("{a}b", "{a}c"),
			AccessMode: HashTagAccessModeRead,
			AccessTime: time.Now(),
		}, {
			HashTag:    "b",
			Keys:       utility.NewStringSet("{b}a", "{b}b"),
			AccessMode: HashTagAccessModeRead,
			AccessTime: time.Now(),
		}, {
			HashTag:    "c",
			Keys:       utility.NewStringSet("{c}a", "{c}b"),
			AccessMode: HashTagAccessModeRead,
			AccessTime: time.Now(),
		},
	}
	for _, event := range events {
		service.aggregateEvent(event)
	}
	assert.Equal(t, 3, len(service.events))

	collectedEvents := service.collectEvents()
	assert.Equal(t, 0, len(service.events))
	assert.Equal(t, 3, len(collectedEvents))
}
