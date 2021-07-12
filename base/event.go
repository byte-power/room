package base

import (
	"bytepower_room/base/log"
	"bytepower_room/utility"

	"errors"
	"fmt"
	"math/rand"
	"sync"
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

type EventServiceConfig struct {
	TCP         utility.TCPWriterConfig `yaml:"tcp"`
	EventBuffer EventBufferConfig       `yaml:"event_buffer"`
}

const (
	defaultEventServiceWorker          = 2
	defaultEventServiceBulkSize        = 50
	defaultEventServiceProcessInterval = "5m"
	defaultWorkerDrainDuration         = "3s"
)

type EventBufferConfig struct {
	WorkerCount           int                         `yaml:"worker_count"`
	BufferLimit           int                         `ymal:"buffer_limit"`
	WorkerConfig          RawEventServiceWorkerConfig `yaml:",inline"`
	WorkerProcessInterval time.Duration
	WorkerDrainDuration   time.Duration
}

type RawEventServiceWorkerConfig struct {
	BulkSize               int    `yaml:"worker_process_bulk_size"`
	RawProcessInterval     string `yaml:"worker_process_interval"`
	RawWorkerDrainDuration string `yaml:"worker_drain_duration"`
}

type EventServiceWorkerConfig struct {
	TCP             utility.TCPWriterConfig
	BulkSize        int
	ProcessInterval time.Duration
	DrainDuration   time.Duration
}

type EventService struct {
	config        *EventServiceConfig
	eventBuffer   chan Event
	bufferWorkers []*eventServiceWorker
	logger        *log.Logger
	wg            sync.WaitGroup
}

func NewEventService(config EventServiceConfig, logger *log.Logger) (*EventService, error) {
	if config.EventBuffer.WorkerCount <= 0 {
		config.EventBuffer.WorkerCount = defaultEventServiceWorker
	}
	if config.EventBuffer.BufferLimit <= 0 {
		config.EventBuffer.BufferLimit = defaultEventServiceBufferLimit
	}
	if config.EventBuffer.WorkerConfig.BulkSize <= 0 {
		config.EventBuffer.WorkerConfig.BulkSize = defaultEventServiceBulkSize
	}
	if config.EventBuffer.WorkerConfig.RawProcessInterval == "" {
		config.EventBuffer.WorkerConfig.RawProcessInterval = defaultEventServiceProcessInterval
	}
	processInterval, err := time.ParseDuration(config.EventBuffer.WorkerConfig.RawProcessInterval)
	if err != nil {
		return nil, fmt.Errorf("event_service.event_buffer.worker_process_interval is invalid:%w", err)
	}
	config.EventBuffer.WorkerProcessInterval = processInterval
	if config.EventBuffer.WorkerConfig.RawWorkerDrainDuration == "" {
		config.EventBuffer.WorkerConfig.RawWorkerDrainDuration = defaultWorkerDrainDuration
	}
	drainDuration, err := time.ParseDuration(config.EventBuffer.WorkerConfig.RawWorkerDrainDuration)
	if err != nil {
		return nil, fmt.Errorf("event_service.event_buffer.worker_drain_duration is invalid:%w", err)
	}
	config.EventBuffer.WorkerDrainDuration = drainDuration

	if logger == nil {
		return nil, fmt.Errorf("logger should not be nil")
	}
	server := &EventService{
		config:      &config,
		eventBuffer: make(chan Event, config.EventBuffer.BufferLimit),
		logger:      logger,
		wg:          sync.WaitGroup{},
	}
	logger.Info(
		"new event service",
		log.String("config", fmt.Sprintf("%+v", config)))
	return server, nil
}

func (service *EventService) Run() {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < service.config.EventBuffer.WorkerCount; i++ {
		// add a random duration between [50, 100) ms, avoid workers to report events at the same time.
		processInterval := time.Duration(rand.Intn(50)+50)*time.Millisecond + service.config.EventBuffer.WorkerProcessInterval
		workerConfig := EventServiceWorkerConfig{
			TCP:             service.config.TCP,
			BulkSize:        service.config.EventBuffer.WorkerConfig.BulkSize,
			ProcessInterval: processInterval,
			DrainDuration:   service.config.EventBuffer.WorkerDrainDuration,
		}
		worker := NewEventServiceWorker(i, workerConfig, service.eventBuffer, service.logger, &service.wg)
		service.bufferWorkers = append(service.bufferWorkers, worker)
		service.wg.Add(1)
		go worker.start()
	}
}

func (service *EventService) SendWriteEvent(key string, accessTime time.Time) error {
	event, err := NewEvent(key, KeyAccessModeWrite, accessTime)
	if err != nil {
		return err
	}
	return service.send(event)
}

func (service *EventService) SendReadEvent(key string, accessTime time.Time) error {
	event, err := NewEvent(key, KeyAccessModeRead, accessTime)
	if err != nil {
		return err
	}
	return service.send(event)
}

func (service *EventService) send(event Event) error {
	select {
	case service.eventBuffer <- event:
		return nil
	default:
		return fmt.Errorf(
			"event service buffer is full with limit %d, event %s is discarded",
			service.config.EventBuffer.BufferLimit, event.String())
	}
}

func (service *EventService) Stop() {
	for _, worker := range service.bufferWorkers {
		go worker.stop()
	}
	service.wg.Wait()
}

func NewEventServiceWorker(workerIndex int, config EventServiceWorkerConfig, eventBuffer chan Event, logger *log.Logger, wg *sync.WaitGroup) *eventServiceWorker {
	tcpWriter := utility.NewTCPWriter(config.TCP)
	worker := &eventServiceWorker{
		index:             workerIndex,
		readEventMutex:    sync.Mutex{},
		writtenEventMutex: sync.Mutex{},
		readEvents:        make(map[string]time.Time),
		writtenEvents:     make(map[string]time.Time),
		eventBuffer:       eventBuffer,
		config:            config,
		logger:            logger,
		tcp:               tcpWriter,
		reportWg:          sync.WaitGroup{},
		stopCh:            make(chan bool),
		wg:                wg,
	}
	return worker
}

type eventServiceWorker struct {
	index             int
	readEventMutex    sync.Mutex
	writtenEventMutex sync.Mutex
	readEvents        map[string]time.Time
	writtenEvents     map[string]time.Time
	eventBuffer       chan Event
	config            EventServiceWorkerConfig
	logger            *log.Logger
	tcp               *utility.TCPWriter
	reportWg          sync.WaitGroup
	wg                *sync.WaitGroup
	stopCh            chan bool
}

func (worker *eventServiceWorker) start() {
	worker.logger.Info(
		"start event service worker",
		log.Int("index", worker.index),
		log.String("config", fmt.Sprintf("%+v", worker.config)),
		log.String("time", time.Now().String()),
	)
	ticker := time.NewTicker(worker.config.ProcessInterval)
	for {
		select {
		case event := <-worker.eventBuffer:
			worker.addEvent(event)
		case <-ticker.C:
			worker.reportWg.Add(1)
			go worker.reportEvents()
		case <-worker.stopCh:
			ticker.Stop()
			return
		}
	}
}

func (worker *eventServiceWorker) addEvent(event Event) {
	if event.AccessMode == KeyAccessModeWrite {
		worker.writtenEventMutex.Lock()
		defer worker.writtenEventMutex.Unlock()
		if worker.writtenEvents[event.Key].Before(event.AccessTime) {
			worker.writtenEvents[event.Key] = event.AccessTime
		}
	} else {
		worker.readEventMutex.Lock()
		defer worker.readEventMutex.Unlock()
		if worker.readEvents[event.Key].Before(event.AccessTime) {
			worker.readEvents[event.Key] = event.AccessTime
		}
	}
}

func (worker *eventServiceWorker) reportEvents() {
	defer worker.reportWg.Done()
	events := worker.collectEvents()
	chunks := eventsToChunks(events, worker.config.BulkSize)
	for _, evts := range chunks {
		sendBytes := []byte{}
		for _, evt := range evts {
			evtBytes, err := json.Marshal(evt)
			if err != nil {
				worker.logger.Error(
					"report event error",
					log.String("event", evt.String()),
					log.Error(err),
				)
			} else {
				sendBytes = append(append(sendBytes, evtBytes...), '\n')
			}
		}
		if len(sendBytes) > 0 {
			if _, err := worker.tcp.Write(sendBytes); err != nil {
				worker.logger.Error(
					"report event error",
					log.String("send_bytes", utility.BytesToString(sendBytes)),
					log.Error(err),
				)
			}
		}
	}
}

func eventsToChunks(events []Event, chunkSize int) [][]Event {
	var chunks [][]Event
	length := len(events)
	index := 0
	for index < length {
		endIndex := utility.IntMin(index+chunkSize, length)
		chunk := events[index:endIndex]
		chunks = append(chunks, chunk)
		index = endIndex
	}
	return chunks
}

func (worker *eventServiceWorker) collectEvents() []Event {
	worker.readEventMutex.Lock()
	readEvents := make([]Event, 0, len(worker.readEvents))
	for key, accessTime := range worker.readEvents {
		event := Event{Key: key, AccessMode: KeyAccessModeRead, AccessTime: accessTime}
		delete(worker.readEvents, key)
		readEvents = append(readEvents, event)
	}
	worker.readEventMutex.Unlock()

	worker.writtenEventMutex.Lock()
	writtenEvents := make([]Event, 0, len(worker.writtenEvents))
	for key, accessTime := range worker.writtenEvents {
		event := Event{Key: key, AccessMode: KeyAccessModeWrite, AccessTime: accessTime}
		delete(worker.writtenEvents, key)
		writtenEvents = append(writtenEvents, event)
	}
	worker.writtenEventMutex.Unlock()
	return append(readEvents, writtenEvents...)
}

func (worker *eventServiceWorker) stop() {
	worker.stopCh <- true
	worker.drainEvents()
	worker.reportWg.Wait()
	worker.logger.Info(
		"stop event service worker",
		log.Int("index", worker.index),
		log.String("time", time.Now().String()),
	)
	worker.wg.Done()
}

func (worker *eventServiceWorker) drainEvents() {
	timer := time.NewTimer(worker.config.DrainDuration)
	for {
		select {
		case event := <-worker.eventBuffer:
			worker.addEvent(event)
		case <-timer.C:
			worker.reportWg.Add(1)
			worker.reportEvents()
			return
		}
	}
}
