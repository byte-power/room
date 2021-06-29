package service

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"context"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	HTTPHeaderContentType = "Content-Type"
	HTTPContentTypeJSON   = "application/json"
)

type CollectEventService struct {
	name                    string
	config                  *base.CollectEventConfig
	eventBuffer             chan base.HashTagEvent
	eventCountInEventBuffer int64
	logger                  *log.Logger
	metric                  *base.MetricClient
	db                      *base.DBCluster
	wg                      sync.WaitGroup
	stopCh                  chan bool
	stop                    int32
	server                  *http.Server
}

func NewCollectEventService(config base.CollectEventConfig, logger *log.Logger, metric *base.MetricClient, db *base.DBCluster) (*CollectEventService, error) {
	if err := config.Init(); err != nil {
		return nil, err
	}
	if logger == nil {
		return nil, errors.New("logger should not be nil")
	}
	if metric == nil {
		return nil, errors.New("metric should not be nil")
	}
	if db == nil {
		return nil, errors.New("db should not be nil")
	}
	server := &CollectEventService{
		name:        "CollectEvent",
		config:      &config,
		eventBuffer: make(chan base.HashTagEvent, config.BufferLimit),
		logger:      logger,
		metric:      metric,
		db:          db,
		wg:          sync.WaitGroup{},
		stopCh:      make(chan bool),
		stop:        0,
	}
	logger.Info(
		"new ecollect event service",
		log.String("config", fmt.Sprintf("%+v", config)))
	return server, nil
}

func (service *CollectEventService) Run() {
	service.wg.Add(1)
	go service.startServer()
	for i := 0; i < service.config.AddEventToDB.WorkerCount; i++ {
		service.wg.Add(1)
		go service.saveEvents()
	}
	service.wg.Add(1)
	go service.mointor(service.config.MonitorInterval)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	sig := <-signalCh
	service.logger.Info(
		fmt.Sprintf("signal received, closing %s service...", service.name),
		log.String("signal", sig.String()))
	service.Stop()
	service.logger.Info(fmt.Sprintf("close %s service success", service.name))
}

func (service *CollectEventService) startServer() {
	defer service.wg.Done()
	mux := http.NewServeMux()
	mux.HandleFunc("/events", service.postEventsHandler)
	service.server = &http.Server{
		Addr:         service.config.Service.URL,
		Handler:      mux,
		ReadTimeout:  time.Duration(service.config.Service.ReadTimeoutMS) * time.Millisecond,
		WriteTimeout: time.Duration(service.config.Service.WriteTimeoutMS) * time.Millisecond,
		IdleTimeout:  time.Duration(service.config.Service.IdleTimeoutMS) * time.Millisecond,
	}
	if err := service.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		service.recordError("listen_serve", err, nil)
	}
}

func (service *CollectEventService) saveEvents() {
	defer service.wg.Done()
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
					"add_event_to_db", err,
					map[string]string{"event": event.String()},
				)
			}
		case <-service.stopCh:
			break loop
		}
	}
}

func (service *CollectEventService) saveEvent(event base.HashTagEvent) error {
	if err := event.Check(); err != nil {
		return err
	}
	config := service.config.AddEventToDB
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.TimeoutMS)*time.Millisecond)
	defer cancel()
	retryInterval := time.Duration(config.RetryIntervalMS) * time.Millisecond
	for i := 0; i < config.RetryTimes; i++ {
		err := upsertHashTagKeysRecordByEvent(ctx, service.db, event, time.Now())
		if err != nil {
			if errors.Is(err, base.DBTxError) {
				service.recordError("add_event_to_db_retry", err, map[string]string{"event": event.String()})
				time.Sleep(retryInterval)
				continue
			}
			return err
		}
		break
	}
	return nil
}

func (service *CollectEventService) AddEvent(event base.HashTagEvent) error {
	defer func() {
		if r := recover(); r != nil {
			recordTaskErrorV2(service.logger, service.metric, service.name, fmt.Errorf("%+v", r), "add_event_panic", nil)
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
			"collect event service buffer is full with limit %d, event %s is discarded",
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
	if atomic.CompareAndSwapInt32(&service.stop, 0, 1) {
		close(service.stopCh)
	}
	service.wg.Wait()
	if service.server != nil {
		if err := service.server.Close(); err != nil {
			service.logger.Error("close collect events server error", log.Error(err))
		} else {
			service.logger.Info("close collect events server success")
		}
	}
	service.drainEvents()
}

func (service *CollectEventService) drainEvents() {
	close(service.eventBuffer)
	for event := range service.eventBuffer {
		err := service.saveEvent(event)
		if err != nil {
			service.recordError(
				"add_event_to_db", err,
				map[string]string{"event": event.String()},
			)
		}
	}
}

func (service *CollectEventService) mointor(interval time.Duration) {
	defer func() {
		service.logger.Info("stop monitor in collect event service")
		service.wg.Done()
	}()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	metricName := "event_in_buffer.total"
loop:
	for {
		select {
		case <-ticker.C:
			service.recordGauge(metricName, atomic.LoadInt64(&service.eventCountInEventBuffer))
		case <-service.stopCh:
			break loop
		}
	}
}

func (service *CollectEventService) recordGauge(metricName string, count int64) {
	metricName = fmt.Sprintf("%s.%s", service.name, metricName)
	service.logger.Info(metricName, log.Int64("count", count))
	service.metric.MetricGauge(metricName, count)
}

func (service *CollectEventService) recordError(reason string, err error, info map[string]string) {
	logPairs := make([]log.LogPair, 0)
	logPairs = append(logPairs, log.String("service", service.name))
	if reason != "" {
		logPairs = append(logPairs, log.String("reason", reason))
	}
	for key, value := range info {
		logPairs = append(logPairs, log.String(key, value))
	}
	if err != nil {
		logPairs = append(logPairs, log.Error(err))
	}
	service.logger.Error("collect event service error", logPairs...)

	errorMetricName := fmt.Sprintf("%s.error", service.name)
	service.metric.MetricIncrease(errorMetricName)
	specificErrorMetricName := fmt.Sprintf("%s.%s", errorMetricName, reason)
	service.metric.MetricIncrease(specificErrorMetricName)
}

func (service *CollectEventService) recordWriteResponseError(err error, body []byte) {
	failedReasonWriteToClient := "write_to_client"
	service.recordError(failedReasonWriteToClient, err, map[string]string{"body": string(body)})
}

func (service *CollectEventService) recordSuccessWithDuration(info string, duration time.Duration) {
	metricName := fmt.Sprintf("%s.success.%s", service.name, info)
	service.logger.Info(metricName, log.String("duration", duration.String()))
	service.metric.MetricIncrease(metricName)
	if duration > time.Duration(0) {
		durationMetricName := fmt.Sprintf("%s.duration", metricName)
		service.metric.MetricTimeDuration(durationMetricName, duration)
	}
}

func (service *CollectEventService) recordSuccessWithCount(info string, count int) {
	metricName := fmt.Sprintf("%s.success.%s", service.name, info)
	service.logger.Info(metricName, log.Int("count", count))
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
	service.logger.Debug("collect_event_service receive event", log.String("body", string(body)))
	service.logger.Info("collect_event_service receive request", log.Int("body_length", len(body)))
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
		service.recordError("add_event_to_cache", err, map[string]string{"body": string(body)})
		if err = writeErrorResponse(writer, http.StatusInternalServerError, err); err != nil {
			service.recordWriteResponseError(err, body)
		}
		return
	}
	if err = writeSuccessResponse(writer, len(events)); err != nil {
		service.recordWriteResponseError(err, body)
	}
	service.recordSuccessWithDuration("add_event_to_cache", time.Since(startTime))
	service.recordSuccessWithCount("add_event_to_cache.events", len(events))
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
