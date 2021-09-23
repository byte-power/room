package service

import (
	"bufio"
	"bytepower_room/base"
	"bytepower_room/base/log"
	"bytepower_room/utility"
	"context"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"

	"errors"
	"fmt"
	"sync"
	"time"

	"go.uber.org/ratelimit"
)

const (
	HTTPHeaderContentType = "Content-Type"
	HTTPContentTypeJSON   = "application/json"
	eventFilePrefix       = "collect_event"
)

const (
	metricEventCountInEventBuffer          = "event_in_buffer.total"
	metricEventCountInCollectedEventBuffer = "event_in_collected_buffer.total"
	metricAggregatedEventCount             = "aggregated_event.total"
	metricEventFileCount                   = "event_file.total"
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
	serverRequestCtxCancel context.CancelFunc

	file *EventFile
}

func NewCollectEventService(
	config *base.RoomCollectEventConfig,
	logger *log.Logger, metric *base.MetricClient,
	db *base.DBCluster,
) (*CollectEventService, error) {

	if logger == nil {
		return nil, errors.New("logger should not be nil")
	}
	if metric == nil {
		return nil, errors.New("metric should not be nil")
	}
	if db == nil {
		return nil, errors.New("db should not be nil")
	}
	file, err := NewEventFile(
		logger, metric, config.SaveFile.FileDirectory,
		config.SaveFile.MaxEventCount, config.SaveFile.MaxFileAge)
	if err != nil {
		return nil, fmt.Errorf("new event file error %w", err)
	}
	logger.Info("create event file", log.String("name", file.Name()))
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

		file: file,
	}

	go service.file.StartFileRotation()

	mux := http.NewServeMux()
	mux.HandleFunc("/events", service.postEventsHandler)
	ctx, cancel := context.WithCancel(context.Background())
	server := &http.Server{
		Addr:         service.config.Server.URL,
		Handler:      mux,
		ReadTimeout:  time.Duration(service.config.Server.ReadTimeoutMS) * time.Millisecond,
		WriteTimeout: time.Duration(service.config.Server.WriteTimeoutMS) * time.Millisecond,
		IdleTimeout:  time.Duration(service.config.Server.IdleTimeoutMS) * time.Millisecond,
		BaseContext:  func(_ net.Listener) context.Context { return ctx },
	}
	service.server = server
	service.serverRequestCtxCancel = cancel

	return service, nil
}

func (service *CollectEventService) Config() *base.RoomCollectEventConfig {
	return service.config
}

func (service *CollectEventService) Run() {
	service.wg.Add(1)
	go service.startServer()

	service.wg.Add(1)
	go service.aggregateEvents()

	service.wg.Add(1)
	go service.collectAggregatedEvents()

	service.wg.Add(1)
	go service.saveEventsToFile()

	service.wg.Add(1)
	go service.saveEventsToDB()

	service.wg.Add(1)
	go service.mointor(service.config.MonitorInterval)
}

func (service *CollectEventService) startServer() {
	defer func() {
		service.logger.Info("stop server")
		service.wg.Done()
	}()
	if err := service.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		service.recordError("listen_serve", err, nil)
	}
}

// returns when channel `service.stopCh` is closed.
func (service *CollectEventService) aggregateEvents() {
	defer func() {
		service.logger.Info("stop events aggregation")
		service.wg.Done()
	}()

	for {
		select {
		case event := <-service.eventBuffer:
			atomic.AddInt64(&service.eventCountInEventBuffer, -1)
			if err := service.aggregateEvent(event); err != nil {
				service.recordError("agg_event", err, map[string]string{"event": event.String()})
			}
		case <-service.stopCh:
			return
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
	for {
		select {
		case <-ticker.C:
			events := service.collectEvents()
			for _, event := range events {
				service.collectedEventBuffer <- event
				atomic.AddInt64(&service.eventCountInCollectedEventBuffer, 1)
			}
		case <-service.stopCh:
			return
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

func (service *CollectEventService) saveEventsToFile() {
	defer func() {
		service.logger.Info("stop save events to file")
		service.wg.Done()
	}()
	for {
		select {
		case event := <-service.collectedEventBuffer:
			atomic.AddInt64(&service.eventCountInCollectedEventBuffer, -1)
			err := service.file.Write(event)
			if err != nil {
				service.recordError("save_events_to_file", err, map[string]string{"event": event.String()})
			}

		case <-service.stopCh:
			return
		}
	}
}

func (service *CollectEventService) saveEventsToDB() {
	defer func() {
		service.logger.Info("stop save events to db")
		service.wg.Done()
	}()

	directory := service.config.SaveFile.FileDirectory
	metricMsg := "save_events_to_db"
	interval := 5 * time.Second
	for {
		files, err := listEventFilesInDirectory(directory)
		if err != nil {
			service.recordError(metricMsg, err, map[string]string{"dir": directory})
			time.Sleep(interval)
			continue
		}
		for _, file := range files {
			quit := service.saveEventsFromFileToDB(file, time.Now(), metricMsg)
			if quit {
				service.logger.Info("quit signal received, stop save events to db")
				return
			}
		}
		time.Sleep(interval)
	}
}

func (service *CollectEventService) saveEventsFromFileToDB(file os.DirEntry, processStartTime time.Time, metricMsg string) bool {
	directory := service.config.SaveFile.FileDirectory
	needProcess, err := isEventFileNeededToProcess(file, service.config.SaveDB.FileAge, processStartTime)
	if err != nil {
		service.recordError(
			fmt.Sprintf("%s.check_need_process", metricMsg),
			err, map[string]string{"name": file.Name()},
		)
		return false
	}
	if !needProcess {
		return false
	}

	name := file.Name()
	service.logger.Info(
		"start to save events from file to database",
		log.String("name", name),
		log.String("start_time", processStartTime.String()),
	)
	fullName := path.Join(directory, name)
	count, quit, errs := service._saveEventsFromFileToDB(fullName)
	if len(errs) != 0 {
		service.recordError(
			fmt.Sprintf("%s.error_count", metricMsg),
			fmt.Errorf("%d errors", len(errs)),
			map[string]string{
				"name":  name,
				"count": fmt.Sprint(count),
			},
		)
	} else {
		service.logger.Info(
			"end to save events from file to database",
			log.String("name", name),
			log.Int("count", count),
			log.String("duration", time.Since(processStartTime).String()),
		)
		service.recordSuccessWithCount(metricMsg, count)
		service.recordSuccessWithDuration(metricMsg, time.Since(processStartTime))
	}
	if quit {
		return quit
	}
	// rename file if has errors
	if len(errs) != 0 {
		backupName := path.Join(directory, fmt.Sprintf("%s.bak", name))
		if err := os.Rename(fullName, backupName); err != nil {
			service.recordError(
				fmt.Sprintf("%s.backup_file", metricMsg),
				err,
				map[string]string{"name": fullName, "backup": backupName},
			)
		} else {
			service.logger.Info(
				"backup file success",
				log.String("name", fullName),
				log.String("backup", backupName),
			)
		}
		return quit
	}

	// remove file if has errors
	if err := os.Remove(fullName); err != nil {
		service.recordError(
			fmt.Sprintf("%s.remove_file", metricMsg),
			err,
			map[string]string{"name": fullName},
		)
	} else {
		service.logger.Info(
			"remove file success",
			log.String("name", fullName),
		)
	}
	return quit
}

func isEventFileNeededToProcess(file os.DirEntry, fileAge time.Duration, t time.Time) (bool, error) {
	info, err := file.Info()
	if err != nil {
		return false, err
	}
	return info.ModTime().Add(fileAge).Before(t), nil
}

func (service *CollectEventService) _saveEventsFromFileToDB(name string) (int, bool, []error) {
	var errors []error
	var successCount int
	var quit bool
	metricMsg := "save_events_to_db"
	file, err := os.Open(name)
	if err != nil {
		errors = append(errors, err)
		service.recordError(fmt.Sprintf("%s.open_file", metricMsg), err, map[string]string{"name": name})
		return successCount, quit, errors
	}
	defer func() {
		if err := file.Close(); err != nil {
			service.recordError(
				fmt.Sprintf("%s.close_file", metricMsg),
				err,
				map[string]string{"name": name},
			)
		}
	}()
	scanner := bufio.NewScanner(file)
	ratelimitBucket := ratelimit.New(service.config.SaveDB.RateLimitPerSecond)
loop:
	for scanner.Scan() {
		var event base.HashTagEvent
		err := json.Unmarshal(scanner.Bytes(), &event)
		if err != nil {
			errors = append(errors, err)
			service.recordError(
				fmt.Sprintf("%s.unmarshal_event", metricMsg),
				err,
				map[string]string{
					"name":  name,
					"event": scanner.Text(),
				},
			)
			continue
		}
		select {
		case <-service.stopCh:
			quit = true
			break loop
		default:
			ratelimitBucket.Take()
			if err := service.saveEvent(event); err != nil {
				errors = append(errors, err)
				service.recordError(
					fmt.Sprintf("%s.save_event", metricMsg),
					err,
					map[string]string{
						"name":  name,
						"event": scanner.Text(),
					})
				continue
			}
			successCount += 1
		}
	}
	if err := scanner.Err(); err != nil {
		service.recordError(fmt.Sprintf("%s.scan", metricMsg), err, map[string]string{"name": name})
		errors = append(errors, err)
	}
	return successCount, quit, errors
}

func (service *CollectEventService) saveEvent(event base.HashTagEvent) error {
	var err error
	if err = event.Check(); err != nil {
		return err
	}
	config := service.config.SaveDB
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.TimeoutMS)*time.Millisecond)
	defer cancel()
	retryInterval := time.Duration(config.RetryIntervalMS) * time.Millisecond
	for i := 0; i < config.RetryTimes; i++ {
		err = upsertHashTagKeysRecordByEvent(ctx, service.db, event, time.Now())
		if err != nil {
			if isRetryErrorForUpdateInTx(err) {
				service.logger.Warn(
					"save_event_to_db_retry",
					log.Error(err),
					log.String("event", event.String()),
					log.Int("retry_times", i),
				)
				service.recordSuccessWithCount("save_event_to_db_retry", 1)
				time.Sleep(retryInterval)
				continue
			}
			return err
		}
		break
	}
	return err
}

func (service *CollectEventService) mointor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer func() {
		service.logger.Info("stop monitor")
		ticker.Stop()
		service.wg.Done()
	}()
	for {
		select {
		case <-ticker.C:
			service.recordGauge(metricEventCountInEventBuffer, atomic.LoadInt64(&service.eventCountInEventBuffer))
			service.recordGauge(metricEventCountInCollectedEventBuffer, atomic.LoadInt64(&service.eventCountInCollectedEventBuffer))
			service.recordGauge(metricAggregatedEventCount, service.GetAggregatedEventCount())
			service.recordGauge(metricEventFileCount, service.GetEventFileCount())
		case <-service.stopCh:
			return
		}
	}
}

func (service *CollectEventService) GetAggregatedEventCount() int64 {
	service.mutex.Lock()
	defer service.mutex.Unlock()
	return int64(len(service.events))
}

func (service *CollectEventService) GetEventFileCount() int64 {
	directory := service.config.SaveFile.FileDirectory
	files, err := listEventFilesInDirectory(directory)
	if err != nil {
		service.recordError("get_event_file_count", err, map[string]string{"dir": directory})
		return 0
	}
	return int64(len(files))
}

func (service *CollectEventService) addEvent(event base.HashTagEvent) error {
	var err error
	if err = event.Check(); err != nil {
		return err
	}
	select {
	case service.eventBuffer <- event:
		atomic.AddInt64(&service.eventCountInEventBuffer, 1)
	default:
		err = fmt.Errorf(
			"buffer is full with limit %d, event %s is discarded",
			service.config.BufferLimit, event.String())
	}
	return err
}

func (service *CollectEventService) addEvents(events []base.HashTagEvent) error {
	for _, event := range events {
		if err := service.addEvent(event); err != nil {
			return err
		}
	}
	return nil
}

func (service *CollectEventService) Stop() {
	if atomic.CompareAndSwapInt32(&service.stop, 0, 1) {
		service.stopServer()
		close(service.stopCh)
		service.wg.Wait()
		service.drainEvents()
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

func (service *CollectEventService) drainEvents() {
	metricMsg := "drain_events"
	defer func() {
		if err := service.file.Close(); err != nil {
			service.recordError(
				fmt.Sprintf("%s.close_file", metricMsg),
				err,
				map[string]string{"name": service.file.Name()},
			)
		}
	}()

	startTime := time.Now()
	service.closeAndEmptifyChannel(service.collectedEventBuffer, &service.eventCountInCollectedEventBuffer)
	service.closeAndEmptifyChannel(service.eventBuffer, &service.eventCountInEventBuffer)

	service.mutex.Lock()
	defer service.mutex.Unlock()
	service.logger.Info("draining events", log.Int("count", len(service.events)))
	for _, event := range service.events {
		err := service.file.Write(event)
		if err != nil {
			service.recordError(
				fmt.Sprintf("%s.save_event_to_file", metricMsg),
				err,
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

func listEventFilesInDirectory(directory string) ([]os.DirEntry, error) {
	eventFiles := make([]os.DirEntry, 0)
	files, err := os.ReadDir(directory)
	if err != nil {
		return eventFiles, err
	}
	for _, file := range files {
		if isEventFile(file.Name()) {
			eventFiles = append(eventFiles, file)
		}
	}
	return eventFiles, nil
}

func isEventFile(name string) bool {
	return strings.HasPrefix(name, eventFilePrefix) && strings.HasSuffix(name, ".log")
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

	err = service.addEvents(events)
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

type EventFile struct {
	name      string
	directory string

	f *os.File

	eventCount    int32
	maxEventCount int32

	createdAt time.Time
	maxAge    time.Duration

	logger *log.Logger
	metric *base.MetricClient

	mutex sync.Mutex

	stopCh chan bool
}

func NewEventFile(
	logger *log.Logger, metric *base.MetricClient,
	directory string, maxEventCount int, maxAge time.Duration,
) (*EventFile, error) {

	if logger == nil {
		return nil, errors.New("logger should not be nil")
	}
	if metric == nil {
		return nil, errors.New("metric should not be nil")
	}
	if directory == "" {
		return nil, errors.New("directory is nil")
	}
	if maxEventCount <= 0 {
		return nil, errors.New("maxEventCount should be greater than 0")
	}
	if maxAge <= 0 {
		return nil, errors.New("duration should be greater than 0")
	}
	currentTime := time.Now()
	name := generateEventFileName(currentTime, os.Getpid())
	f, err := os.Create(filepath.Join(directory, name))
	if err != nil {
		return nil, err
	}
	file := &EventFile{
		directory: directory,
		name:      name,

		f: f,

		eventCount:    0,
		maxEventCount: int32(maxEventCount),

		createdAt: currentTime,
		maxAge:    maxAge,

		logger: logger,
		metric: metric,

		mutex: sync.Mutex{},

		stopCh: make(chan bool),
	}
	return file, nil
}

func generateEventFileName(t time.Time, pid int) string {
	return fmt.Sprintf(
		"%s_%d_%s_%s.log",
		eventFilePrefix, pid,
		t.Format("20060102_150405"),
		utility.GenerateFixedLengthRandomString(4),
	)
}

func (file *EventFile) Write(event base.HashTagEvent) error {
	bytes, err := json.Marshal(event)
	if err != nil {
		return err
	}
	file.mutex.Lock()
	defer file.mutex.Unlock()
	_, err = file.f.Write(append(bytes, '\n'))
	if err != nil {
		return err
	}
	atomic.AddInt32(&file.eventCount, 1)
	return nil
}

func (file *EventFile) StartFileRotation() {
	file.logger.Info("start file rotation")
	rotateCheckInterval := 5 * time.Second
	ticker := time.NewTicker(rotateCheckInterval)

	defer func() {
		ticker.Stop()
		file.logger.Info("stop file rotation")
	}()

	for {
		select {
		case <-ticker.C:
			currentTime := time.Now()
			if !file.needToRotateFile(currentTime) {
				continue
			}
			oldFileName := file.Name()
			err := file.rotateFile(currentTime)
			logInfo := map[string]string{
				"old_name": oldFileName,
				"name":     file.Name(),
			}
			if err != nil {
				file.recordError("rotate_file", err, logInfo)
			} else {
				file.recordSuccess("rotate_file", 1, logInfo)
			}
		case <-file.stopCh:
			return
		}
	}
}

func (file *EventFile) rotateFile(t time.Time) error {
	file.mutex.Lock()
	defer file.mutex.Unlock()

	if err := file.f.Close(); err != nil {
		return err
	}
	oldName := file.name
	eventCount := atomic.LoadInt32(&file.eventCount)
	createdAt := file.createdAt
	name := generateEventFileName(t, os.Getpid())
	file.name = name
	atomic.StoreInt32(&file.eventCount, 0)
	f, err := os.Create(filepath.Join(file.directory, name))
	if err != nil {
		return err
	}
	file.f = f
	file.createdAt = t
	file.logger.Info(
		"rotate file success",
		log.String("name", name),
		log.String("old_name", oldName),
		log.Int32("event_count", eventCount),
		log.String("created_at", createdAt.String()),
		log.String("rotate_at", t.String()),
	)
	return nil
}

func (file *EventFile) recordError(reason string, err error, info map[string]string) {
	logPairs := make([]log.LogPair, 0)
	for key, value := range info {
		logPairs = append(logPairs, log.String(key, value))
	}
	if err != nil {
		logPairs = append(logPairs, log.Error(err))
	}
	file.logger.Error(reason, logPairs...)

	errorMetricName := "error.event_file"
	file.metric.MetricIncrease(errorMetricName)
	specificErrorMetricName := fmt.Sprintf("%s.%s", errorMetricName, reason)
	file.metric.MetricIncrease(specificErrorMetricName)
}

func (file *EventFile) recordSuccess(metricName string, count int, info map[string]string) {
	metricName = fmt.Sprintf("event_file.%s", metricName)
	logPairs := make([]log.LogPair, 0)
	for key, value := range info {
		logPairs = append(logPairs, log.String(key, value))
	}
	file.logger.Info(metricName, logPairs...)
	file.metric.MetricCount(metricName, count)
}

func (file *EventFile) needToRotateFile(t time.Time) bool {
	return file.createdAt.Add(file.maxAge).Before(t) || atomic.LoadInt32(&file.eventCount) > file.maxEventCount
}

func (file *EventFile) Name() string {
	file.mutex.Lock()
	defer file.mutex.Unlock()
	return file.name
}

func (file *EventFile) FullName() string {
	file.mutex.Lock()
	defer file.mutex.Unlock()
	return filepath.Join(file.directory, file.name)
}

func (file *EventFile) Close() error {
	close(file.stopCh)
	file.mutex.Lock()
	defer file.mutex.Unlock()
	return file.f.Close()
}
