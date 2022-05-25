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
)

const HTTPContentTypeJSON = "application/json"

const HashTagEventServiceName = "hash_tag_event_service"

var (
	metricReportEventsError   = fmt.Sprintf("%s.error.report_events", HashTagEventServiceName)
	metricSendEventPanic      = fmt.Sprintf("%s.error.send_event_panic", HashTagEventServiceName)
	metricAggregateEventError = fmt.Sprintf("%s.error.agg_event", HashTagEventServiceName)

	metricReportEventsSuccess = fmt.Sprintf("%s.report_events", HashTagEventServiceName)

	metricEventCountInEventBuffer          = fmt.Sprintf("%s.event_in_buffer.total", HashTagEventServiceName)
	metricEventCountInCollectedEventBuffer = fmt.Sprintf("%s.event_in_collected_buffer.total", HashTagEventServiceName)
	metricAggregatedEventCount             = fmt.Sprintf("%s.aggregated_event.total", HashTagEventServiceName)
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

type HashTagEventServiceConfig struct {
	EventReport HashTagEventServiceEventReportConfig `yaml:"event_report"`

	RawAggInterval string `yaml:"agg_interval"`
	AggInterval    time.Duration

	BufferLimit int `yaml:"buffer_limit"`

	RawMonitorInterval string `yaml:"monitor_interval"`
	MonitorInterval    time.Duration
}

func (config HashTagEventServiceConfig) check() error {
	if err := config.EventReport.check(); err != nil {
		return fmt.Errorf("event_report.%w", err)
	}
	if config.RawAggInterval == "" {
		return errors.New("agg_interval should not be empty")
	}
	if config.BufferLimit <= 0 {
		return fmt.Errorf("buffer_limit=%d, it should be greater than 0", config.BufferLimit)
	}
	if config.RawMonitorInterval == "" {
		return errors.New("monitor_interval should not be empty")
	}
	return nil

}

type HashTagEventServiceEventReportConfig struct {
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

	RequestMaxRetry          int `yaml:"request_max_retry"`
	RequestMinRetryBackoffMS int `yaml:"request_min_retry_backoff_ms"`
	RequestMaxRetryBackoffMS int `yaml:"request_max_retry_backoff_ms"`
}

func (config HashTagEventServiceEventReportConfig) check() error {
	if config.URL == "" {
		return errors.New("url should not be empty")
	}
	if config.RawRequestTimeout == "" {
		return errors.New("request_timeout should not be empty")
	}
	if config.RequestMaxEvent <= 0 {
		return fmt.Errorf("request_max_event=%d, it should be greater than 0", config.RequestMaxEvent)
	}
	if config.RawRequestMaxWaitDuration == "" {
		return errors.New("request_max_wait_duration should not be empty")
	}
	if config.RequestWorkerCount <= 0 {
		return fmt.Errorf("request_worker_count=%d, it should be greater than 0", config.RequestWorkerCount)
	}
	if config.RawRequestConnKeepAliveInterval == "" {
		return errors.New("request_conn_keep_alive_interval should not be empty")
	}
	if config.RawRequestIdleConnTimeout == "" {
		return errors.New("request_idle_conn_timeout should not be empty")
	}
	if config.RequestMaxConn <= 0 {
		return fmt.Errorf("request_max_conn=%d, it should be greater than 0", config.RequestMaxConn)
	}
	if config.RequestMaxRetry <= 0 {
		return fmt.Errorf("request_max_retry=%d, it should be greater than 0", config.RequestMaxRetry)
	}

	if v := config.RequestMinRetryBackoffMS; v <= 0 {
		return fmt.Errorf("request_min_retry_backoff_ms=%d, it should be > 0", v)
	}
	if v := config.RequestMaxRetryBackoffMS; v <= 0 {
		return fmt.Errorf("request_max_retry_backoff_ms=%d, it should be > 0", v)
	}
	if config.RequestMinRetryBackoffMS > config.RequestMaxRetryBackoffMS {
		return fmt.Errorf(
			"request_min_retry_backoff_ms=%d, request_max_retry_backoff_ms=%d, request_min_retry_backoff_ms shoule be less than or equal to request_max_retry_backoff_ms",
			config.RequestMinRetryBackoffMS, config.RequestMaxRetryBackoffMS)
	}
	return nil
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

func NewHashTagEventService(config *HashTagEventServiceConfig, logger *log.Logger, metric *MetricClient) (*HashTagEventService, error) {
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
		name:                             HashTagEventServiceName,
		config:                           config,
		eventBuffer:                      make(chan HashTagEvent, config.BufferLimit),
		eventCountInEventBuffer:          0,
		mutex:                            sync.Mutex{},
		events:                           make(map[string]HashTagEvent),
		collectedEventBuffer:             make(chan HashTagEvent, config.BufferLimit),
		eventCountInCollectedEventBuffer: 0,
		logger:                           logger,
		metric:                           metric,
		wg:                               sync.WaitGroup{},
		stopCh:                           make(chan bool),
		stop:                             0,
		client:                           client,
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
		service.logger.Info(fmt.Sprintf("%s: stop aggregate events", service.name))
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
	ticker := time.NewTicker(service.config.AggInterval)
	defer func() {
		service.logger.Info(fmt.Sprintf("%s: stop collect aggregated events", service.name))
		ticker.Stop()
		service.wg.Done()
	}()
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
	ticker := time.NewTicker(service.config.EventReport.RequestMaxWaitDuration)
	defer func() {
		service.logger.Info(fmt.Sprintf("%s: stop report events in hash_tag_event service", service.name))
		ticker.Stop()
		service.wg.Done()
	}()
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
		} else {
			service.metric.MetricCount(metricReportEventsSuccess, len(events))
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

	for attempt := 0; attempt < service.config.EventReport.RequestMaxRetry; attempt++ {
		if attempt > 0 {
			waitBeforeRetryDuration := utility.RetryBackoff(
				uint(attempt),
				time.Duration(service.config.EventReport.RequestMinRetryBackoffMS),
				time.Duration(service.config.EventReport.RequestMaxRetryBackoffMS))

			time.Sleep(waitBeforeRetryDuration)
		}
		err = reportRequest(service.client, service.config.EventReport.URL, bs)
		if err != nil {
			continue
		}
	}
	return err
}

func reportRequest(client *http.Client, url string, body []byte) error {
	requestBody := bytes.NewReader(body)
	resp, err := client.Post(url, HTTPContentTypeJSON, requestBody)
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
			"%s: buffer is full with limit %d, event %s is discarded",
			service.name, service.config.BufferLimit, event.String())
	}
}

func (service *HashTagEventService) Stop() {
	if atomic.CompareAndSwapInt32(&service.stop, 0, 1) {
		close(service.stopCh)
		service.wg.Wait()
		service.drainEvents()
	}
}

func (service *HashTagEventService) drainEvents() {
	service.closeAndEmptifyChannel(service.collectedEventBuffer, &service.eventCountInCollectedEventBuffer)
	service.closeAndEmptifyChannel(service.eventBuffer, &service.eventCountInEventBuffer)

	requestMaxEvent := service.config.EventReport.RequestMaxEvent
	allEvents := service.collectEvents()
	events := make([]HashTagEvent, 0, requestMaxEvent)
	service.logger.Info(fmt.Sprintf("%s: draining %d events", service.name, len(allEvents)))
	for _, event := range allEvents {
		events = append(events, event)
		if len(events) == requestMaxEvent {
			if err := service._reportEvents(events); err != nil {
				service.recordReportEventsError(events, err)
			} else {
				service.metric.MetricCount(metricReportEventsSuccess, len(events))
			}
			events = make([]HashTagEvent, 0, requestMaxEvent)
		}
	}
	if err := service._reportEvents(events); err != nil {
		service.recordReportEventsError(events, err)
	} else {
		service.metric.MetricCount(metricReportEventsSuccess, len(events))
	}
}

func (service *HashTagEventService) closeAndEmptifyChannel(ch chan HashTagEvent, counter *int64) {
	close(ch)
	for event := range ch {
		atomic.AddInt64(counter, -1)
		if err := service.aggregateEvent(event); err != nil {
			service.recordAggregateEventError(event, err)
		}
	}
}

func (service *HashTagEventService) mointor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer func() {
		service.logger.Info(fmt.Sprintf("%s: stop monitor", service.name))
		ticker.Stop()
		service.wg.Done()
	}()
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
	service.logger.Info(metricName, log.Int64("count", count))
	service.metric.MetricGauge(metricName, count)
}
