package service

import (
	"bufio"
	"bytepower_room/base"
	"bytepower_room/base/log"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"sync"
	"time"

	"go.uber.org/ratelimit"
)

const saveEventServiceName = "save_event"

type SaveEventService struct {
	config *base.RoomSaveEventConfig
	logger *log.Logger
	metric *base.MetricClient
	db     *base.DBCluster
	wg     sync.WaitGroup

	ctx         context.Context
	ctxCancelFn context.CancelFunc
}

func NewSaveEventService(config *base.RoomSaveEventConfig, logger *log.Logger, metric *base.MetricClient, db *base.DBCluster) (*SaveEventService, error) {
	if config == nil {
		return nil, errors.New("config should not be nil")
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
	ctx, cancel := context.WithCancel(context.Background())
	service := &SaveEventService{
		config:      config,
		logger:      logger,
		metric:      metric,
		db:          db,
		wg:          sync.WaitGroup{},
		ctx:         ctx,
		ctxCancelFn: cancel,
	}
	return service, nil
}

func (service *SaveEventService) Run() {
	service.logger.Info(
		fmt.Sprintf("start %s", saveEventServiceName),
		log.String("time", time.Now().String()),
	)
	service.wg.Add(1)
	go service.saveEventsToDB()
}

func (service *SaveEventService) Config() *base.RoomSaveEventConfig {
	return service.config
}

func (service *SaveEventService) Stop() {
	service.ctxCancelFn()
	service.wg.Wait()
	service.logger.Info(
		fmt.Sprintf("stop %s", saveEventServiceName),
		log.String("time", time.Now().String()),
	)
}

func (service *SaveEventService) saveEventsToDB() {
	defer func() {
		service.wg.Done()
	}()

	directory := service.config.SaveDB.FileDirectory
	interval := 5 * time.Second
	for {
		select {
		case <-service.ctx.Done():
			service.logger.Info("context is done, stop work")
			return
		default:
			files, err := listEventFilesInDirectory(directory)
			if err != nil {
				recordError(service.logger, service.metric, saveEventServiceName, err, map[string]string{"dir": directory})
				time.Sleep(interval)
				continue
			}
			for _, file := range files {
				quit := service.saveEventsFromFileToDB(file, time.Now())
				if quit {
					service.logger.Info("quit signal received, stop")
					return
				}
			}
		}
		time.Sleep(interval)
	}
}

func (service *SaveEventService) saveEventsFromFileToDB(file os.DirEntry, processStartTime time.Time) bool {
	directory := service.config.SaveDB.FileDirectory
	needProcess, err := isEventFileNeededToProcess(file, service.config.SaveDB.FileAge, processStartTime)
	if err != nil {
		recordError(
			service.logger,
			service.metric,
			"check_need_process",
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
		recordError(
			service.logger,
			service.metric,
			"error_count",
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
		service.metric.MetricCount("save_count", count)
		recordSuccessWithDuration(service.metric, "save_count", time.Since(processStartTime))
	}
	if quit {
		return quit
	}
	// rename file if has errors
	if len(errs) != 0 {
		backupName := path.Join(directory, fmt.Sprintf("%s.bak", name))
		if err := os.Rename(fullName, backupName); err != nil {
			recordError(
				service.logger,
				service.metric,
				"backup_file",
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

	// remove file if no errors
	if err := os.Remove(fullName); err != nil {
		recordError(
			service.logger,
			service.metric,
			"remove_file",
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

func (service *SaveEventService) _saveEventsFromFileToDB(name string) (int, bool, []error) {
	var errors []error
	var successCount int
	var quit bool
	file, err := os.Open(name)
	if err != nil {
		errors = append(errors, err)
		recordError(
			service.logger, service.metric,
			"open_file", err, map[string]string{"name": name})
		return successCount, quit, errors
	}
	defer func() {
		if err := file.Close(); err != nil {
			recordError(
				service.logger,
				service.metric,
				"close_file",
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
			recordError(
				service.logger,
				service.metric,
				"unmarshal_event",
				err,
				map[string]string{
					"name":  name,
					"event": scanner.Text(),
				},
			)
			continue
		}
		select {
		case <-service.ctx.Done():
			quit = true
			break loop
		default:
			ratelimitBucket.Take()
			if err := service.saveEvent(event); err != nil {
				errors = append(errors, err)
				recordError(
					service.logger,
					service.metric,
					"save_event_to_db",
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
		recordError(
			service.logger,
			service.metric,
			"scan",
			err,
			map[string]string{"name": name})
		errors = append(errors, err)
	}
	return successCount, quit, errors
}

func (service *SaveEventService) saveEvent(event base.HashTagEvent) error {
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
				service.metric.MetricIncrease("save_event_to_db_retry")
				time.Sleep(retryInterval)
				continue
			}
			return err
		}
		break
	}
	return err
}

func SaveEvent(ctx context.Context, db *base.DBCluster, event base.HashTagEvent, saveTime time.Time) error {
	return upsertHashTagKeysRecordByEvent(ctx, db, event, saveTime)
}
