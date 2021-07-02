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
	assert.Equal(t, currentTime, service.events[hashTag].AccessTime)
	assert.True(t, service.events[hashTag].WriteTime.IsZero())
	assert.Equal(t, 0, service.events[hashTag].Keys.Len())
	//assert.ElementsMatch(t, keys, service.events[hashTag].Keys.ToSlice())

	currentTime = time.Now()
	keys2 := []string{"c{abc}", "d{abc}"}
	event, _ = NewHashTagEvent(hashTag, keys2, HashTagAccessModeWrite, currentTime)
	service.aggregateEvent(event)
	assert.Equal(t, 1, len(service.events))
	assert.Equal(t, currentTime, service.events[hashTag].AccessTime)
	assert.Equal(t, currentTime, service.events[hashTag].WriteTime)
	assert.ElementsMatch(t, keys2, service.events[hashTag].Keys.ToSlice())
}

func TestHashTagEventCollectEvent(t *testing.T) {
	service := testInitHashTagEventService()
	events := []HashTagEvent{
		{
			HashTag:    "a",
			Keys:       utility.NewStringSet("{a}b", "{a}c"),
			AccessTime: time.Now(),
		}, {
			HashTag:    "b",
			Keys:       utility.NewStringSet("{b}a", "{b}b"),
			AccessTime: time.Now(),
			WriteTime:  time.Now(),
		}, {
			HashTag:    "c",
			Keys:       utility.NewStringSet("{c}a", "{c}b"),
			AccessTime: time.Now(),
			WriteTime:  time.Now(),
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

func TestHashTagEventMerge(t *testing.T) {
	currentTime := time.Now()

	count := 10
	times := make([]time.Time, count)
	for i := 0; i < count; i++ {
		times[i] = currentTime.Add(time.Duration(i) * time.Minute)
	}

	testCases := []struct {
		desc   string
		events []HashTagEvent
		valid  bool
		result HashTagEvent
	}{
		{
			"merge event with different hash tags",
			[]HashTagEvent{
				{"abc", utility.NewStringSet("{abc}a"), times[0], times[0]},
				{"bcd", utility.NewStringSet("{bcd}a"), times[0], times[0]},
			},
			false,
			HashTagEvent{},
		}, {
			"merge read and write events",
			[]HashTagEvent{
				{"abc", utility.NewStringSet("{abc}a", "{abc}c"), times[1], times[1]},
				{"abc", utility.NewStringSet("{abc}b"), times[2], times[0]},
			},
			true,
			HashTagEvent{"abc", utility.NewStringSet("{abc}a", "{abc}b", "{abc}c"), times[2], times[1]},
		}, {
			"merge read only events",
			[]HashTagEvent{
				{"abc", utility.NewStringSet("{abc}a", "{abc}b"), times[2], time.Time{}},
				{"abc", utility.NewStringSet("{abc}m", "{abc}n"), times[3], time.Time{}},
			},
			true,
			HashTagEvent{"abc", utility.NewStringSet("{abc}a", "{abc}b", "{abc}m", "{abc}n"), times[3], time.Time{}},
		},
	}
	for _, testCase := range testCases {
		event, err := MergeEvents(testCase.events[0], testCase.events[1:]...)
		if !testCase.valid {
			assert.NotNil(t, err)
		} else {
			assert.Nil(t, err)
			assert.Equal(t, testCase.result.HashTag, event.HashTag)
			assert.Equal(t, testCase.result.AccessTime, event.AccessTime)
			assert.Equal(t, testCase.result.WriteTime, event.WriteTime)
			assert.ElementsMatch(t, testCase.result.Keys.ToSlice(), event.Keys.ToSlice())
		}
	}
}
