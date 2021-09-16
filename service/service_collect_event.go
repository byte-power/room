package service

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"bytepower_room/utility"
	"context"
	"io/ioutil"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"

	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	HTTPHeaderContentType = "Content-Type"
	HTTPContentTypeJSON   = "application/json"
	eventFilePrefix       = "collect_event_"
)

const (
	metricEventCountInEventBuffer          = "event_in_buffer.total"
	metricEventCountInCollectedEventBuffer = "event_in_collected_buffer.total"
	metricAggregatedEventCount             = "aggregated_event.total"
	metricRequestBodyLength                = "request_body_length.total"
)

type CollectEventService struct {
	config *base.RoomCollectEventConfig

	eventBuffer             chan base.HashTagEvent
	eventCountInEventBuffer int64

	mutex  sync.Mutex
	events map[string]base.HashTagEvent

	collectedEventBuffer             chan base.HashTagEvent
	eventCountInCollectedEventBuffer int64

	logger *log.Logger
	metric *base.MetricClient
	db     *base.DBCluster

	wg     sync.WaitGroup
	stopCh chan bool
	stop   int32

	server                 *http.Server
	serverRequestCtx       context.Context
	serverRequestCtxCancel context.CancelFunc
}

func NewCollectEventService(config *base.RoomCollectEventConfig, logger *log.Logger, metric *base.MetricClient, db *base.DBCluster) (*CollectEventService, error) {
	if logger == nil {
		return nil, errors.New("logger should not be nil")
	}
	if metric == nil {
		return nil, errors.New("metric should not be nil")
	}
	if db == nil {
		return nil, errors.New("db should not be nil")
	}
	ctx, cancel := context.WithCancel(context.Background())
	service := &CollectEventService{
		config: config,

		eventBuffer:             make(chan base.HashTagEvent, config.BufferLimit),
		eventCountInEventBuffer: 0,

		mutex:  sync.Mutex{},
		events: make(map[string]base.HashTagEvent),

		collectedEventBuffer:             make(chan base.HashTagEvent, config.BufferLimit),
		eventCountInCollectedEventBuffer: 0,

		logger: logger,
		metric: metric,
		db:     db,

		wg:     sync.WaitGroup{},
		stopCh: make(chan bool),
		stop:   0,

		server:                 nil,
		serverRequestCtx:       ctx,
		serverRequestCtxCancel: cancel,
	}
	return service, nil
}

func (service *CollectEventService) Config() *base.RoomCollectEventConfig {
	return service.config
}

func (service *CollectEventService) Run() {
	go service.startServer()
	for i := 0; i < service.config.SaveEvent.WorkerCount; i++ {
		service.wg.Add(1)
		go service.saveEvents()
	}

	service.wg.Add(1)
	go service.aggregateEvents()

	service.wg.Add(1)
	go service.collectAggregatedEvents()

	service.wg.Add(1)
	go service.mointor(service.config.MonitorInterval)
}

func (service *CollectEventService) startServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/events", service.postEventsHandler)
	service.server = &http.Server{
		Addr:         service.config.Server.URL,
		Handler:      mux,
		ReadTimeout:  time.Duration(service.config.Server.ReadTimeoutMS) * time.Millisecond,
		WriteTimeout: time.Duration(service.config.Server.WriteTimeoutMS) * time.Millisecond,
		IdleTimeout:  time.Duration(service.config.Server.IdleTimeoutMS) * time.Millisecond,
		BaseContext:  func(_ net.Listener) context.Context { return service.serverRequestCtx },
	}
	if err := service.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		service.recordError("listen_serve", err, nil)
	}
}

func (service *CollectEventService) stopServer() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(service.config.ServerShutdownTimeoutSeconds)*time.Second)
	defer cancel()
	if err := service.server.Shutdown(ctx); err != nil {
		service.recordError("close_server", err, nil)
	} else {
		service.logger.Info("shutdown server success")
	}
	service.serverRequestCtxCancel()
	// wait 1 second for cancel process.
	time.Sleep(time.Second)
	service.logger.Info("cancel all server requests with context cancel function")
}

// returns when channel `service.stopCh` is closed.
func (service *CollectEventService) aggregateEvents() {
	defer func() {
		service.logger.Info("stop aggregate events")
		service.wg.Done()
	}()

loop:
	for {
		select {
		case event := <-service.eventBuffer:
			atomic.AddInt64(&service.eventCountInEventBuffer, -1)
			if err := service.aggregateEvent(event); err != nil {
				service.recordError("agg_event", err, map[string]string{"event": event.String()})
			}
		case <-service.stopCh:
			break loop
		}
	}
}

func (service *CollectEventService) aggregateEvent(event base.HashTagEvent) error {
	if event.WriteTime.IsZero() {
		event.Keys = utility.NewStringSet([]string{}...)
	}
	service.mutex.Lock()
	defer service.mutex.Unlock()
	var newEvent base.HashTagEvent
	var err error
	if savedEvent, ok := service.events[event.HashTag]; ok {
		newEvent, err = base.MergeEvents(savedEvent, event)
		if err != nil {
			return err
		}
	} else {
		newEvent = event
	}
	service.events[event.HashTag] = newEvent
	return nil
}

func (service *CollectEventService) collectAggregatedEvents() {
	ticker := time.NewTicker(service.config.AggInterval)
	defer func() {
		service.logger.Info("stop collect aggregated events")
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

func (service *CollectEventService) collectEvents() []base.HashTagEvent {
	events := make([]base.HashTagEvent, 0)
	service.mutex.Lock()
	defer service.mutex.Unlock()
	for hashTag, event := range service.events {
		events = append(events, event)
		delete(service.events, hashTag)
	}
	return events
}

func (service *CollectEventService) saveEvents() {
	defer func() {
		service.logger.Info("stop save events")
		service.wg.Done()
	}()
loop:
	for {
		select {
		case event, ok := <-service.eventBuffer:
			if !ok {
				break loop
			}
			atomic.AddInt64(&service.eventCountInEventBuffer, -1)
			if err := service.saveEvent(event); err != nil {
				service.recordError(
					"save_event", err,
					map[string]string{"event": event.String()},
				)
			}
		case <-service.stopCh:
			break loop
		}
	}
}

func (service *CollectEventService) saveEvent(event base.HashTagEvent) error {
	var err error
	if err = event.Check(); err != nil {
		return err
	}
	config := service.config.SaveEvent
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.TimeoutMS)*time.Millisecond)
	defer cancel()
	retryInterval := time.Duration(config.RetryIntervalMS) * time.Millisecond
	for i := 0; i < config.RetryTimes; i++ {
		err = upsertHashTagKeysRecordByEvent(ctx, service.db, event, time.Now())
		if err != nil {
			if isRetryErrorForUpdateInTx(err) {
				service.recordError(
					"save_event_retry",
					err,
					map[string]string{
						"event":       event.String(),
						"retry_times": strconv.Itoa(i),
					})
				time.Sleep(retryInterval)
				continue
			}
			return err
		}
		break
	}
	return err
}

func (service *CollectEventService) AddEvent(event base.HashTagEvent) error {
	defer func() {
		if r := recover(); r != nil {
			service.recordError("add_event_panic", fmt.Errorf("%+v", r), nil)
		}
	}()
	if err := event.Check(); err != nil {
		return err
	}
	select {
	case service.eventBuffer <- event:
		atomic.AddInt64(&service.eventCountInEventBuffer, 1)
		return nil
	default:
		return fmt.Errorf(
			"buffer is full with limit %d, event %s is discarded",
			service.config.BufferLimit, event.String())
	}
}

func (service *CollectEventService) AddEvents(events []base.HashTagEvent) error {
	for _, event := range events {
		if err := service.AddEvent(event); err != nil {
			return err
		}
	}
	return nil
}

func (service *CollectEventService) Stop() {
	service.stopServer()
	if atomic.CompareAndSwapInt32(&service.stop, 0, 1) {
		close(service.stopCh)
		service.wg.Wait()
		service.drainEvents()
	}
}

func (service *CollectEventService) drainEvents() {
	startTime := time.Now()
	service.closeAndEmptifyChannel(service.collectedEventBuffer, &service.eventCountInCollectedEventBuffer)
	service.closeAndEmptifyChannel(service.eventBuffer, &service.eventCountInEventBuffer)

	service.mutex.Lock()
	defer service.mutex.Unlock()
	service.logger.Info("draining events", log.Int("count", len(service.events)))
	for _, event := range service.events {
		err := service.saveEvent(event)
		if err != nil {
			service.recordError(
				"save_event", err,
				map[string]string{"event": event.String()},
			)
		}
	}
	service.logger.Info("events are drained", log.String("duration", time.Since(startTime).String()))
}

func (service *CollectEventService) closeAndEmptifyChannel(ch chan base.HashTagEvent, counter *int64) {
	close(ch)
	for event := range ch {
		atomic.AddInt64(counter, -1)
		if err := service.aggregateEvent(event); err != nil {
			service.recordError("agg_event", err, map[string]string{"event": event.String()})
		}
	}
}

func (service *CollectEventService) mointor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer func() {
		service.logger.Info("stop monitor")
		ticker.Stop()
		service.wg.Done()
	}()
loop:
	for {
		select {
		case <-ticker.C:
			service.recordGauge(metricEventCountInEventBuffer, atomic.LoadInt64(&service.eventCountInEventBuffer))
			service.recordGauge(metricEventCountInCollectedEventBuffer, atomic.LoadInt64(&service.eventCountInCollectedEventBuffer))
			service.recordGauge(metricAggregatedEventCount, service.GetAggregatedEventCount())
		case <-service.stopCh:
			break loop
		}
	}
}

func (service *CollectEventService) GetAggregatedEventCount() int64 {
	service.mutex.Lock()
	defer service.mutex.Unlock()
	return int64(len(service.events))
}

func (service *CollectEventService) recordGauge(metricName string, count int64) {
	service.logger.Info(metricName, log.Int64("count", count))
	service.recordGaugeMetric(metricName, count)
}

func (service *CollectEventService) recordGaugeMetric(metricName string, count int64) {
	service.metric.MetricGauge(metricName, count)
}

func (service *CollectEventService) recordError(reason string, err error, info map[string]string) {
	logPairs := make([]log.LogPair, 0)
	for key, value := range info {
		logPairs = append(logPairs, log.String(key, value))
	}
	if err != nil {
		logPairs = append(logPairs, log.Error(err))
	}
	service.logger.Error(reason, logPairs...)

	errorMetricName := "error"
	service.metric.MetricIncrease(errorMetricName)
	specificErrorMetricName := fmt.Sprintf("%s.%s", errorMetricName, reason)
	service.metric.MetricIncrease(specificErrorMetricName)
}

func (service *CollectEventService) recordWriteResponseError(err error, body []byte) {
	failedReasonWriteToClient := "write_to_client"
	service.recordError(failedReasonWriteToClient, err, map[string]string{"body": string(body)})
}

func (service *CollectEventService) recordSuccessWithDuration(metricName string, duration time.Duration) {
	service.metric.MetricIncrease(metricName)
	if duration > time.Duration(0) {
		durationMetricName := fmt.Sprintf("%s.duration", metricName)
		service.metric.MetricTimeDuration(durationMetricName, duration)
	}
}

func (service *CollectEventService) recordSuccessWithCount(metricName string, count int) {
	service.metric.MetricCount(metricName, count)
}

type CollectEventsRequestBody struct {
	Events []base.HashTagEvent `json:"events"`
}

func (service *CollectEventService) postEventsHandler(writer http.ResponseWriter, request *http.Request) {
	startTime := time.Now()
	if request.Method != http.MethodPost {
		err := fmt.Errorf("method %s is not allowed", request.Method)
		service.recordError("method_not_allowed", err, nil)
		if err = writeErrorResponse(writer, http.StatusMethodNotAllowed, err); err != nil {
			service.recordWriteResponseError(err, []byte{})
		}
		return
	}
	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		service.recordError("read_body", err, nil)
		if err = writeErrorResponse(writer, http.StatusInternalServerError, err); err != nil {
			service.recordWriteResponseError(err, []byte{})
		}
		return
	}
	service.recordGaugeMetric(metricRequestBodyLength, int64(len(body)))
	requestBodyStruct := CollectEventsRequestBody{}
	if err = json.Unmarshal(body, &requestBodyStruct); err != nil {
		service.recordError("unmarshal_body", err, map[string]string{"body": string(body)})
		if err = writeErrorResponse(writer, http.StatusBadRequest, err); err != nil {
			service.recordWriteResponseError(err, body)
		}
		return
	}
	events := requestBodyStruct.Events
	for _, event := range events {
		if err = event.Check(); err != nil {
			service.recordError("event_check", err, map[string]string{"event": event.String()})
			if err = writeErrorResponse(writer, http.StatusBadRequest, err); err != nil {
				service.recordWriteResponseError(err, body)
			}
			return
		}
	}

	err = service.AddEvents(events)
	if err != nil {
		service.recordError("add_event", err, map[string]string{"body": string(body)})
		if err = writeErrorResponse(writer, http.StatusInternalServerError, err); err != nil {
			service.recordWriteResponseError(err, body)
		}
		return
	}
	if err = writeSuccessResponse(writer, len(events)); err != nil {
		service.recordWriteResponseError(err, body)
	}
	service.recordSuccessWithDuration("add_event", time.Since(startTime))
	service.recordSuccessWithCount("add_event.events", len(events))
}

func writeErrorResponse(writer http.ResponseWriter, code int, err error) error {
	writer.Header().Set(HTTPHeaderContentType, HTTPContentTypeJSON)
	writer.WriteHeader(code)
	body := map[string]string{"error": err.Error()}
	bodyInBytes, err := json.Marshal(body)
	if err != nil {
		return err
	}
	_, err = writer.Write(bodyInBytes)
	return err
}

func writeSuccessResponse(writer http.ResponseWriter, count int) error {
	writer.Header().Set(HTTPHeaderContentType, HTTPContentTypeJSON)
	writer.WriteHeader(http.StatusOK)
	body := map[string]int{"count": count}
	bodyInBytes, err := json.Marshal(body)
	if err != nil {
		return err
	}
	_, err = writer.Write(bodyInBytes)
	return err
}

func SaveEvent(ctx context.Context, db *base.DBCluster, event base.HashTagEvent, saveTime time.Time) error {
	return upsertHashTagKeysRecordByEvent(ctx, db, event, saveTime)
}
