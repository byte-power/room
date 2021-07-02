package base

import (
	"bytepower_room/base/log"
	"bytepower_room/utility"
	"bytes"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"

	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	ErrEventHashKeyEmpty     = errors.New("event hash_tag is empty")
	ErrEventAccessModeEmpty  = errors.New("event access_mode is empty")
	ErrEventAccessTimeEmpty  = errors.New("event access_time is empty")
	ErrWriteEventWithoutKeys = errors.New("write event does not have keys")

	errDrainEventTimeout = errors.New("drain event timeout")
)

const HTTPContentTypeJSON = "application/json"

const HashTagEventServiceName = "hash_tag_event_service"

var (
	metricReportEventsError   = "error.report_events"
	metricSendEventPanic      = "error.send_event_panic"
	metricDrainEventError     = "error.drain_event"
	metricAggregateEventError = "error.agg_event"

	metricEventCountInEventBuffer          = "event_in_buffer.total"
	metricEventCountInCollectedEventBuffer = "event_in_collected_buffer.total"
	metricAggregatedEventCount             = "aggregated_event.total"
)

type HashTagAccessMode string

const (
	HashTagAccessModeRead  HashTagAccessMode = "read"
	HashTagAccessModeWrite HashTagAccessMode = "write"
)

type HashTagEvent struct {
	HashTag    string             `json:"hash_tag"`
	Keys       *utility.StringSet `json:"keys"`
	AccessTime time.Time          `json:"access_time"`
	WriteTime  time.Time          `json:"write_time"`
}

func NewHashTagEvent(hashTag string, keys []string, accessMode HashTagAccessMode, accessTime time.Time) (HashTagEvent, error) {
	event := HashTagEvent{
		HashTag:    hashTag,
		Keys:       utility.NewStringSet(keys...),
		AccessTime: accessTime,
	}
	if accessMode == HashTagAccessModeWrite {
		event.WriteTime = accessTime
	}
	if err := event.Check(); err != nil {
		return HashTagEvent{}, err
	}
	return event, nil
}

func (event HashTagEvent) Check() error {
	if event.HashTag == "" {
		return ErrEventHashKeyEmpty
	}
	if event.AccessTime.IsZero() {
		return ErrEventAccessTimeEmpty
	}
	if !event.WriteTime.IsZero() && event.Keys.Len() == 0 {
		return ErrWriteEventWithoutKeys
	}
	return nil
}

func (event HashTagEvent) String() string {
	var result string
	bs, err := json.Marshal(event)
	if err != nil {
		result = fmt.Sprintf(
			"Event[hash_tag=%s, access_time=%v, write_time=%v, keys=%s]",
			event.HashTag, event.AccessTime, event.WriteTime, strings.Join(event.Keys.ToSlice(), " "))
	} else {
		result = string(bs)
	}
	return result
}

func (event HashTagEvent) Copy() HashTagEvent {
	return HashTagEvent{
		HashTag:    event.HashTag,
		Keys:       event.Keys.Copy(),
		AccessTime: event.AccessTime,
		WriteTime:  event.WriteTime,
	}
}

func MergeEvents(event HashTagEvent, events ...HashTagEvent) (HashTagEvent, error) {
	if err := event.Check(); err != nil {
		return HashTagEvent{}, err
	}
	newEvent := event.Copy()
	for _, event := range events {
		if err := event.Check(); err != nil {
			return HashTagEvent{}, err
		}
		if newEvent.HashTag != event.HashTag {
			return HashTagEvent{}, errors.New("events should have the same hash_tag")
		}
		newEvent.WriteTime = utility.GetLatestTime(newEvent.WriteTime, event.WriteTime)
		newEvent.AccessTime = utility.GetLatestTime(newEvent.AccessTime, event.AccessTime)
		newEvent.Keys.Merge(event.Keys)
	}
	return newEvent, nil
}

const (
	defaultEventServiceBufferLimit       = 16 * 1024 * 1024 // 16M
	defaultEventServiceAggregateInterval = 1 * time.Minute
	defaultEventServiceDrainDuration     = 5 * time.Second

	defaultEventReportRequestTimeout               = 100 * time.Millisecond
	defaultEventReportRequestWorkerCount           = 5
	defaultEventReportRequestMaxEvent              = 10
	defaultEventReportRequestMaxWaitDuration       = 5 * time.Second
	defaultEventReportRequestConnKeepAliveInterval = 30 * time.Second
	defaultEventReportRequestIdleConnTimeout       = 90 * time.Second
	defaultEventReportRequestMaxConn               = 100
	defaultMonitorInterval                         = 10 * time.Second
)

type HashTagEventServiceConfig struct {
	EventReport HashTagEventReportConfig `yaml:"event_report"`

	RawAggInterval string `yaml:"agg_interval"`
	AggInterval    time.Duration

	RawDrainDuration string `yaml:"drain_duration"`
	DrainDuration    time.Duration

	BufferLimit int `yaml:"buffer_limit"`

	RawMonitorInterval string `yaml:"monitor_interval"`
	MonitorInterval    time.Duration
}

type HashTagEventReportConfig struct {
	URL string `yaml:"url"`

	RawRequestTimeout string `yaml:"request_timeout"`
	RequestTimeout    time.Duration

	RequestMaxEvent int `yaml:"request_max_event"`

	RawRequestMaxWaitDuration string `yaml:"request_max_wait_duration"`
	RequestMaxWaitDuration    time.Duration

	RequestWorkerCount int `yaml:"request_worker_count"`

	RawRequestConnKeepAliveInterval string `yaml:"request_conn_keep_alive_interval"`
	RequestConnKeepAliveInterval    time.Duration

	RawRequestIdleConnTimeout string `yaml:"request_idle_conn_timeout"`
	RequestIdleConnTimeout    time.Duration

	RequestMaxConn int `yaml:"request_max_conn"`
}

type HashTagEventService struct {
	name                             string
	config                           *HashTagEventServiceConfig
	eventBuffer                      chan HashTagEvent
	eventCountInEventBuffer          int64
	mutex                            sync.Mutex
	events                           map[string]HashTagEvent
	collectedEventBuffer             chan HashTagEvent
	eventCountInCollectedEventBuffer int64
	logger                           *log.Logger
	metric                           *MetricClient
	wg                               sync.WaitGroup
	stopCh                           chan bool
	stop                             int32
	client                           *http.Client
}

func NewHashTagEventService(config HashTagEventServiceConfig, logger *log.Logger, metric *MetricClient) (*HashTagEventService, error) {
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
		config.DrainDuration = defaultEventServiceDrainDuration
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
	if config.RawMonitorInterval == "" {
		config.MonitorInterval = defaultMonitorInterval
	} else {
		duration, err := time.ParseDuration(config.RawMonitorInterval)
		if err != nil {
			return nil, fmt.Errorf("event_service.monitor_interval is invalid:%w", err)
		}
		config.MonitorInterval = duration
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
	if config.EventReport.RawRequestConnKeepAliveInterval == "" {
		config.EventReport.RequestConnKeepAliveInterval = defaultEventReportRequestConnKeepAliveInterval
	} else {
		duration, err := time.ParseDuration(config.EventReport.RawRequestConnKeepAliveInterval)
		if err != nil {
			return nil, fmt.Errorf("event_service.event_report.request_conn_keep_alive_interval is invalid:%w", err)
		}
		config.EventReport.RequestConnKeepAliveInterval = duration
	}
	if config.EventReport.RawRequestIdleConnTimeout == "" {
		config.EventReport.RequestIdleConnTimeout = defaultEventReportRequestIdleConnTimeout
	} else {
		duration, err := time.ParseDuration(config.EventReport.RawRequestIdleConnTimeout)
		if err != nil {
			return nil, fmt.Errorf("event_service.event_report.request_idle_conn_timeout is invalid:%w", err)
		}
		config.EventReport.RequestIdleConnTimeout = duration
	}
	if config.EventReport.RequestMaxConn <= 0 {
		config.EventReport.RequestMaxConn = defaultEventReportRequestMaxConn
	}

	if logger == nil {
		return nil, errors.New("logger should not be nil")
	}
	if metric == nil {
		return nil, errors.New("metric should not be nil")
	}
	client := &http.Client{
		Timeout: config.EventReport.RequestTimeout,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				KeepAlive: config.EventReport.RequestConnKeepAliveInterval,
			}).DialContext,
			ForceAttemptHTTP2: true,
			MaxConnsPerHost:   config.EventReport.RequestMaxConn,
			IdleConnTimeout:   config.EventReport.RequestIdleConnTimeout,
		},
	}
	server := &HashTagEventService{
		name:                 HashTagEventServiceName,
		config:               &config,
		eventBuffer:          make(chan HashTagEvent, config.BufferLimit),
		mutex:                sync.Mutex{},
		events:               make(map[string]HashTagEvent),
		collectedEventBuffer: make(chan HashTagEvent, config.BufferLimit),
		logger:               logger,
		metric:               metric,
		wg:                   sync.WaitGroup{},
		stopCh:               make(chan bool),
		stop:                 0,
		client:               client,
	}
	logger.Info(
		"new hash_tag_event service",
		log.String("config", fmt.Sprintf("%+v", config)))
	return server, nil
}

func (service *HashTagEventService) Run() {
	service.wg.Add(1)
	go service.aggregateEvents()
	service.wg.Add(1)
	go service.collectAggregatedEvents()
	for i := 0; i < service.config.EventReport.RequestWorkerCount; i++ {
		service.wg.Add(1)
		go service.reportEvents()
	}
	service.wg.Add(1)
	go service.mointor(service.config.MonitorInterval)
}

// returns when channel `service.stopCh` is closed.
func (service *HashTagEventService) aggregateEvents() {
	defer func() {
		service.logger.Info("stop aggregate events in hash_tag_event service")
		service.wg.Done()
	}()

loop:
	for {
		select {
		case event := <-service.eventBuffer:
			atomic.AddInt64(&service.eventCountInEventBuffer, -1)
			if err := service.aggregateEvent(event); err != nil {
				service.recordAggregateEventError(event, err)
			}
		case <-service.stopCh:
			break loop
		}
	}
}

func (service *HashTagEventService) recordAggregateEventError(event HashTagEvent, err error) {
	service.logger.Error(
		metricAggregateEventError,
		log.String("event", event.String()),
		log.Error(err),
	)
	service.metric.MetricIncrease(metricAggregateEventError)
}

func (service *HashTagEventService) aggregateEvent(event HashTagEvent) error {
	if event.WriteTime.IsZero() {
		event.Keys = utility.NewStringSet([]string{}...)
	}
	service.mutex.Lock()
	defer service.mutex.Unlock()
	var newEvent HashTagEvent
	var err error
	if savedEvent, ok := service.events[event.HashTag]; ok {
		newEvent, err = MergeEvents(savedEvent, event)
		if err != nil {
			return err
		}
	} else {
		newEvent = event
	}
	service.events[event.HashTag] = newEvent
	return nil
}

// returns when channel `service.stopCh` is closed
func (service *HashTagEventService) collectAggregatedEvents() {
	defer func() {
		service.logger.Info("stop collect aggregated events in hash_tag_event service")
		service.wg.Done()
	}()
	ticker := time.NewTicker(service.config.AggInterval)
	defer ticker.Stop()
loop:
	for {
		select {
		case <-ticker.C:
			events := service.collectEvents()
			for _, event := range events {
				service.collectedEventBuffer <- event
				atomic.AddInt64(&service.eventCountInCollectedEventBuffer, 1)
			}
		case <-service.stopCh:
			break loop
		}
	}
}

func (service *HashTagEventService) collectEvents() []HashTagEvent {
	events := make([]HashTagEvent, 0)
	service.mutex.Lock()
	defer service.mutex.Unlock()
	for hashTag, event := range service.events {
		events = append(events, event)
		delete(service.events, hashTag)
	}
	return events
}

// returns when channel `service.stopCh` is closed
func (service *HashTagEventService) reportEvents() {
	defer func() {
		service.wg.Done()
		service.logger.Info("stop report events in hash_tag_event service")
	}()
	ticker := time.NewTicker(service.config.EventReport.RequestMaxWaitDuration)
	defer ticker.Stop()
	requestMaxEvent := service.config.EventReport.RequestMaxEvent
	stop := false
	for {
		events := make([]HashTagEvent, 0, requestMaxEvent)
		eventCount := 0
		ticker.Reset(service.config.EventReport.RequestMaxWaitDuration)
	loop:
		for {
			select {
			case event, ok := <-service.collectedEventBuffer:
				if !ok {
					stop = true
					break loop
				}
				atomic.AddInt64(&service.eventCountInCollectedEventBuffer, -1)
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
}

func (service *HashTagEventService) _reportEvents(events []HashTagEvent) error {
	if len(events) == 0 {
		return nil
	}
	data := map[string][]HashTagEvent{"events": events}
	bs, err := json.Marshal(data)
	if err != nil {
		return err
	}
	requestBody := bytes.NewReader(bs)
	resp, err := service.client.Post(service.config.EventReport.URL, HTTPContentTypeJSON, requestBody)
	if err != nil {
		return err
	}
	defer func() {
		if resp.Body != nil {
			io.ReadAll(resp.Body)
			resp.Body.Close()
		}
	}()
	if resp.StatusCode != http.StatusOK {
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("response error, http_code=%d, read_body_err=%w", resp.StatusCode, err)
		}
		return fmt.Errorf("response error, http_code=%d, body=%s", resp.StatusCode, utility.AnyToString(respBody))
	}
	return nil
}

func (service *HashTagEventService) recordReportEventsError(events []HashTagEvent, err error) {
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

func (service *HashTagEventService) SendEvent(hashTag string, keys []string, accessMode HashTagAccessMode, accessTime time.Time) error {
	event, err := NewHashTagEvent(hashTag, keys, accessMode, accessTime)
	if err != nil {
		return err
	}
	return service.send(event)
}

func (service *HashTagEventService) send(event HashTagEvent) error {
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
		atomic.AddInt64(&service.eventCountInEventBuffer, 1)
		return nil
	default:
		return fmt.Errorf(
			"event service buffer is full with limit %d, event %s is discarded",
			service.config.BufferLimit, event.String())
	}
}

func (service *HashTagEventService) Stop() {
	if atomic.CompareAndSwapInt32(&service.stop, 0, 1) {
		close(service.stopCh)
	}
	service.wg.Wait()
	if err := service.drainEvents(); err != nil {
		service.recordDrainError(err)
	}
}

func (service *HashTagEventService) drainEvents() error {
	timer := time.NewTimer(service.config.DrainDuration)
	if err := service.closeAndEmptifyChannelWithTimer(timer, service.collectedEventBuffer, &service.eventCountInCollectedEventBuffer); err != nil {
		return err
	}
	if err := service.closeAndEmptifyChannelWithTimer(timer, service.eventBuffer, &service.eventCountInEventBuffer); err != nil {
		return err
	}
	requestMaxEvent := service.config.EventReport.RequestMaxEvent
	allEvents := service.collectEvents()
	events := make([]HashTagEvent, 0, requestMaxEvent)
	for _, event := range allEvents {
		events = append(events, event)
		if len(events) == requestMaxEvent {
			if err := service._reportEvents(events); err != nil {
				return err
			}
			events = make([]HashTagEvent, 0, requestMaxEvent)
		}
	}
	if err := service._reportEvents(events); err != nil {
		return err
	}
	return nil
}

func (service *HashTagEventService) closeAndEmptifyChannelWithTimer(timer *time.Timer, ch chan HashTagEvent, counter *int64) error {
	close(ch)
loop:
	for {
		select {
		case event, ok := <-ch:
			if ok {
				atomic.AddInt64(counter, -1)
				if err := service.aggregateEvent(event); err != nil {
					service.recordAggregateEventError(event, err)
				}
			} else {
				break loop
			}
		case <-timer.C:
			return errDrainEventTimeout
		}
	}
	return nil
}

func (service *HashTagEventService) recordDrainError(err error) {
	service.logger.Error(metricDrainEventError, log.Error(err))
	service.metric.MetricIncrease(metricDrainEventError)
}

func (service *HashTagEventService) mointor(interval time.Duration) {
	defer func() {
		service.logger.Info("stop monitor in hash_tag_event service")
		service.wg.Done()
	}()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
loop:
	for {
		select {
		case <-ticker.C:
			service.recordStat(
				metricEventCountInEventBuffer,
				atomic.LoadInt64(&service.eventCountInEventBuffer),
			)
			service.recordStat(
				metricEventCountInCollectedEventBuffer,
				atomic.LoadInt64(&service.eventCountInCollectedEventBuffer),
			)
			service.recordStat(
				metricAggregatedEventCount,
				service.GetAggregatedEventCount(),
			)
		case <-service.stopCh:
			break loop
		}
	}
}

func (service *HashTagEventService) GetAggregatedEventCount() int64 {
	service.mutex.Lock()
	defer service.mutex.Unlock()
	return int64(len(service.events))
}

func (service *HashTagEventService) recordStat(metricName string, count int64) {
	metricName = fmt.Sprintf("%s.%s", service.name, metricName)
	service.logger.Info(metricName, log.Int64("count", count))
	service.metric.MetricGauge(metricName, count)
}
