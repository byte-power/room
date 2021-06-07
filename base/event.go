package base

import (
	"bytepower_room/base/log"
	"bytepower_room/utility"
	"strings"
	"sync/atomic"

	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrEventHashKeyEmpty    = errors.New("hash_tag is empty")
	ErrEventAccessModeEmpty = errors.New("access_mode is empty")
	ErrEventAccessTimeEmpty = errors.New("access_time is empty")

	errDrainEventTimeout = errors.New("drain event timeout")
)

var (
	metricReportEventsError = "error.report_events"
	metricSendEventPanic    = "error.send_event_panic"
	metricDrainEventError   = "error.drain_event"
)

type HashTagAccessMode string

const (
	HashTagAccessModeRead  HashTagAccessMode = "read"
	HashTagAccessModeWrite HashTagAccessMode = "write"
)

type Event struct {
	HashTag    string             `json:"hash_tag"`
	Keys       *utility.StringSet `json:"keys"`
	AccessMode HashTagAccessMode  `json:"access_mode"`
	AccessTime time.Time          `json:"access_time"`
}

func NewEvent(hashTag string, keys []string, accessMode HashTagAccessMode, accessTime time.Time) (Event, error) {
	if hashTag == "" {
		return Event{}, ErrEventHashKeyEmpty
	}
	if accessMode == "" {
		return Event{}, ErrEventAccessModeEmpty
	}
	if accessTime.IsZero() {
		return Event{}, ErrEventAccessTimeEmpty
	}
	return Event{
		HashTag:    hashTag,
		Keys:       utility.NewStringSet(keys...),
		AccessMode: accessMode,
		AccessTime: accessTime,
	}, nil
}

func (event Event) String() string {
	return fmt.Sprintf(
		"Event[hash_tag=%s, access_mode=%s, access_time=%v, keys=%s]",
		event.HashTag, event.AccessMode, event.AccessTime, strings.Join(event.Keys.ToSlice(), " "))
}

const (
	defaultEventServiceBufferLimit           = 16 * 1024 * 1024 // 16M
	defaultEventServiceAggregateInterval     = 1 * time.Minute
	defaultEventReportRequestTimeout         = 100 * time.Millisecond
	defaultEventReportRequestWorkerCount     = 2
	defaultEventReportRequestMaxEvent        = 10
	defaultEventReportRequestMaxWaitDuration = 5 * time.Second
	defaultDrainDuration                     = 5 * time.Second
)

type EventServiceConfig struct {
	EventReport EventReportConfig `yaml:"event_report"`

	RawAggInterval string `yaml:"agg_interval"`
	AggInterval    time.Duration

	RawDrainDuration string `yaml:"drain_duration"`
	DrainDuration    time.Duration

	BufferLimit int `yaml:"buffer_limit"`
}

type EventReportConfig struct {
	URL string `yaml:"url"`

	RawRequestTimeout string `yaml:"request_timeout"`
	RequestTimeout    time.Duration

	RequestMaxEvent int `yaml:"request_max_event"`

	RawRequestMaxWaitDuration string `yaml:"request_max_wait_duration"`
	RequestMaxWaitDuration    time.Duration

	RequestWorkerCount int `yaml:"request_worker_count"`
}

type EventService struct {
	config               *EventServiceConfig
	eventBuffer          chan Event
	mutex                sync.Mutex
	events               map[string]Event
	collectedEventBuffer chan Event
	logger               *log.Logger
	metric               *MetricClient
	wg                   sync.WaitGroup
	stopCh               chan bool
	stop                 int32
}

func NewEventService(config EventServiceConfig, logger *log.Logger, metric *MetricClient) (*EventService, error) {
	if config.RawAggInterval == "" {
		config.AggInterval = defaultEventServiceAggregateInterval
	} else {
		duration, err := time.ParseDuration(config.RawAggInterval)
		if err != nil {
			return nil, fmt.Errorf("event_service.agg_interval is invalid:%w", err)
		}
		config.AggInterval = duration
	}
	if config.RawDrainDuration == "" {
		config.DrainDuration = defaultDrainDuration
	} else {
		duration, err := time.ParseDuration(config.RawDrainDuration)
		if err != nil {
			return nil, fmt.Errorf("event_service.drain_duration is invalid:%w", err)
		}
		config.DrainDuration = duration
	}
	if config.BufferLimit <= 0 {
		config.BufferLimit = defaultEventServiceBufferLimit
	}
	if config.EventReport.URL == "" {
		return nil, errors.New("event_service.event_report.url is empty")
	}
	if config.EventReport.RawRequestTimeout == "" {
		config.EventReport.RequestTimeout = defaultEventReportRequestTimeout
	} else {
		duration, err := time.ParseDuration(config.EventReport.RawRequestTimeout)
		if err != nil {
			return nil, fmt.Errorf("event_service.event_report.request_timeout is invalid:%w", err)
		}
		config.EventReport.RequestTimeout = duration
	}
	if config.EventReport.RequestMaxEvent <= 0 {
		config.EventReport.RequestMaxEvent = defaultEventReportRequestMaxEvent
	}
	if config.EventReport.RawRequestMaxWaitDuration == "" {
		config.EventReport.RequestMaxWaitDuration = defaultEventReportRequestMaxWaitDuration
	} else {
		duration, err := time.ParseDuration(config.EventReport.RawRequestMaxWaitDuration)
		if err != nil {
			return nil, fmt.Errorf("event_service.event_report.request_max_wait_duration is invalid:%w", err)
		}
		config.EventReport.RequestMaxWaitDuration = duration
	}
	if config.EventReport.RequestWorkerCount <= 0 {
		config.EventReport.RequestWorkerCount = defaultEventReportRequestWorkerCount
	}

	if logger == nil {
		return nil, errors.New("logger should not be nil")
	}
	if metric == nil {
		return nil, errors.New("metric should not be nil")
	}
	server := &EventService{
		config:               &config,
		eventBuffer:          make(chan Event, config.BufferLimit),
		mutex:                sync.Mutex{},
		events:               make(map[string]Event),
		collectedEventBuffer: make(chan Event, config.BufferLimit),
		logger:               logger,
		metric:               metric,
		wg:                   sync.WaitGroup{},
		stopCh:               make(chan bool),
		stop:                 0,
	}
	logger.Info(
		"new event service",
		log.String("config", fmt.Sprintf("%+v", config)))
	server.startWorkers()
	return server, nil
}

func (service *EventService) startWorkers() {
	go service.aggregateEvents()
	go service.collectAggregatedEvents()
	for i := 0; i < service.config.EventReport.RequestWorkerCount; i++ {
		go service.reportEvents()
	}
}

// returns when channel `service.stopCh` is closed.
func (service *EventService) aggregateEvents() {
	service.wg.Add(1)
	defer service.wg.Done()
loop:
	for {
		select {
		case event := <-service.eventBuffer:
			service.aggregateEvent(event)
		case <-service.stopCh:
			break loop
		}
	}
}

func (service *EventService) aggregateEvent(event Event) {
	service.mutex.Lock()
	defer service.mutex.Unlock()
	if savedEvent, ok := service.events[event.HashTag]; ok {
		if savedEvent.AccessMode == HashTagAccessModeWrite {
			event.AccessMode = savedEvent.AccessMode
		}
		if savedEvent.AccessTime.After(event.AccessTime) {
			event.AccessTime = savedEvent.AccessTime
		}
		savedEvent.Keys.AddItems(event.Keys.ToSlice()...)
		event.Keys = savedEvent.Keys
	}
	service.events[event.HashTag] = event
}

// returns when channel `service.stopCh` is closed
func (service *EventService) collectAggregatedEvents() {
	service.wg.Add(1)
	defer service.wg.Done()
	ticker := time.NewTicker(service.config.AggInterval)
loop:
	for {
		select {
		case <-ticker.C:
			events := service._collect()
			for _, event := range events {
				service.collectedEventBuffer <- event
			}
		case <-service.stopCh:
			break loop
		}
	}
	ticker.Stop()
}

func (service *EventService) _collect() []Event {
	events := make([]Event, 0)
	service.mutex.Lock()
	defer service.mutex.Unlock()
	for hashTag, event := range service.events {
		events = append(events, event)
		delete(service.events, hashTag)
	}
	return events
}

// returns when channel `service.stopCh` is closed
func (service *EventService) reportEvents() {
	service.wg.Add(1)
	defer service.wg.Done()
	ticker := time.NewTicker(service.config.EventReport.RequestTimeout)
	requestMaxEvent := service.config.EventReport.RequestMaxEvent
	stop := false
	for {
		events := make([]Event, 0, requestMaxEvent)
		eventCount := 0
		ticker.Reset(service.config.EventReport.RequestTimeout)
	loop:
		for {
			select {
			case event, ok := <-service.collectedEventBuffer:
				if !ok {
					break loop
				}
				eventCount += 1
				events = append(events, event)
				if eventCount >= requestMaxEvent {
					break loop
				}
			case <-ticker.C:
				break loop
			case <-service.stopCh:
				stop = true
				break loop
			}
		}
		err := service._reportEvents(events)
		if err != nil {
			service.recordReportEventsError(events, err)
		}
		if stop {
			break
		}
	}
	ticker.Stop()
}

func (service *EventService) _reportEvents(events []Event) error {
	if len(events) == 0 {
		return nil
	}
	for _, event := range events {
		fmt.Printf("send event %s\n", event.String())
	}
	return nil
}

func (service *EventService) recordReportEventsError(events []Event, err error) {
	eventsInStr := make([]string, 0, len(events))
	for _, event := range events {
		eventsInStr = append(eventsInStr, event.String())
	}

	service.logger.Error(
		metricReportEventsError,
		log.String("events", strings.Join(eventsInStr, " ")),
		log.Int("event_count", len(events)),
		log.Error(err),
	)
	service.metric.MetricIncrease(metricReportEventsError)
}

func (service *EventService) SendEvent(hashTag string, keys []string, accessMode HashTagAccessMode, accessTime time.Time) error {
	event, err := NewEvent(hashTag, keys, accessMode, accessTime)
	if err != nil {
		return err
	}
	return service.send(event)
}

func (service *EventService) send(event Event) error {
	defer func() {
		if r := recover(); r != nil {
			service.logger.Error(
				metricSendEventPanic,
				log.String("info", fmt.Sprintf("%+v", r)),
			)
			service.metric.MetricIncrease(metricSendEventPanic)
		}
	}()
	select {
	case service.eventBuffer <- event:
		return nil
	default:
		return fmt.Errorf(
			"event service buffer is full with limit %d, event %s is discarded",
			service.config.BufferLimit, event.String())
	}
}

func (service *EventService) Stop() {
	if atomic.CompareAndSwapInt32(&service.stop, 0, 1) {
		close(service.stopCh)
	}
	service.wg.Wait()
	if err := service.drainEvents(); err != nil {
		service.recordDrainError(err)
	}
}

func (service *EventService) drainEvents() error {
	timer := time.NewTimer(service.config.DrainDuration)
	if err := service.closeAndEmptifyChannelWithTimer(timer, service.collectedEventBuffer); err != nil {
		return err
	}
	if err := service.closeAndEmptifyChannelWithTimer(timer, service.eventBuffer); err != nil {
		return err
	}
	requestMaxEvent := service.config.EventReport.RequestMaxEvent
	events := make([]Event, 0, requestMaxEvent)
	//TODO: add timeout
	for _, event := range service.events {
		events = append(events, event)
		if len(events) == requestMaxEvent {
			if err := service._reportEvents(events); err != nil {
				return err
			}
			events = make([]Event, 0, requestMaxEvent)
		}
	}
	if err := service._reportEvents(events); err != nil {
		return err
	}
	return nil
}

func (service *EventService) closeAndEmptifyChannelWithTimer(timer *time.Timer, ch chan Event) error {
	close(ch)
loop:
	for {
		select {
		case event, ok := <-ch:
			if ok {
				service.aggregateEvent(event)
			} else {
				break loop
			}
		case <-timer.C:
			return errDrainEventTimeout
		}
	}
	return nil
}

func (service *EventService) recordDrainError(err error) {
	service.logger.Error(metricDrainEventError, log.Error(err))
	service.metric.MetricIncrease(metricDrainEventError)
}
