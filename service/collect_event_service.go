package service

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
	"bytepower_room/utility"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	HTTPHeaderContentType = "Content-Type"
	HTTPContentTypeJSON   = "application/json"

	CollectEventsTaskName    = "collect_events"
	SyncHashtagKeysTaskName  = "sync_keys"
	CleanHashTagkeysTaskName = "clean_keys"
)

func CollectEvents() {
	logger := base.GetTaskLogger()
	metric := base.GetTaskMetricService()
	mux := http.NewServeMux()
	config := base.GetServerConfig().CollectEventService
	mux.HandleFunc("/events", postEventsHandler)
	server := &http.Server{
		Addr:         config.Service.URL,
		Handler:      mux,
		ReadTimeout:  time.Duration(config.Service.ReadTimeoutMS) * time.Millisecond,
		WriteTimeout: time.Duration(config.Service.WriteTimeoutMS) * time.Millisecond,
		IdleTimeout:  time.Duration(config.Service.IdleTimeoutMS) * time.Millisecond,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			recordTaskError2(logger, metric, CollectEventsTaskName, err, "listen_serve", nil)
		}
	}()
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	sig := <-signalCh
	logger.Info("signal received, closing collect events service...", log.String("signal", sig.String()))
	if err := server.Close(); err != nil {
		logger.Error("close collect events service error", log.Error(err))
	} else {
		logger.Info("close collect events service success")
	}
}

const failedReasonWriteToClient = "write_to_client"

func postEventsHandler(writer http.ResponseWriter, request *http.Request) {
	config := base.GetServerConfig().CollectEventService.AddEvent
	dep := base.GetTaskDependency()
	if request.Method != http.MethodPost {
		err := fmt.Errorf("method %s is not allowed", request.Method)
		if writeErr := writeErrorResponse(writer, http.StatusMethodNotAllowed, err); writeErr != nil {
			recordTaskError2(
				dep.Logger,
				dep.Metric,
				CollectEventsTaskName,
				writeErr,
				failedReasonWriteToClient,
				nil)
		}
		return
	}
	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		if writeErr := writeErrorResponse(writer, http.StatusInternalServerError, err); writeErr != nil {
			recordTaskError2(
				dep.Logger,
				dep.Metric,
				CollectEventsTaskName,
				writeErr,
				failedReasonWriteToClient,
				nil)
		}
		return
	}
	events := make([]base.Event, 0)
	if err := json.Unmarshal(body, &events); err != nil {
		if writeErr := writeErrorResponse(writer, http.StatusBadRequest, err); writeErr != nil {
			recordTaskError2(
				dep.Logger,
				dep.Metric,
				CollectEventsTaskName,
				writeErr,
				failedReasonWriteToClient,
				map[string]string{"body": utility.AnyToString(body)})
		}
		return
	}
	retryTimes := config.RetryTimes
	retryInterval := time.Duration(config.RetryIntervalMS) * time.Millisecond
	timeout := time.Duration(config.DBTimeoutMS) * time.Millisecond
	for _, event := range events {
		if err := event.Check(); err != nil {
			if writeErr := writeErrorResponse(writer, http.StatusBadRequest, err); writeErr != nil {
				recordTaskError2(
					dep.Logger,
					dep.Metric,
					CollectEventsTaskName,
					writeErr,
					failedReasonWriteToClient,
					map[string]string{"body": utility.AnyToString(body)})
			}
			return
		}
		err := addEventToDB(dep.DB, dep.Logger, dep.Metric, event, retryTimes, retryInterval, timeout)
		if err != nil {
			if writeErr := writeErrorResponse(writer, http.StatusInternalServerError, err); writeErr != nil {
				recordTaskError2(
					dep.Logger,
					dep.Metric,
					CollectEventsTaskName,
					writeErr,
					failedReasonWriteToClient,
					map[string]string{"body": utility.AnyToString(body)})
			}
			return
		}
	}
	if writeErr := writeSuccessResponse(writer, len(events)); writeErr != nil {
		recordTaskError2(
			dep.Logger,
			dep.Metric,
			CollectEventsTaskName,
			writeErr,
			failedReasonWriteToClient,
			map[string]string{"body": utility.AnyToString(body)})
	}
}

func addEventToDB(dbCluster *base.DBCluster, logger *log.Logger, metric *base.MetricClient, event base.Event, retryTimes int, retryInterval, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for i := 0; i < retryTimes; i++ {
		err := upsertHashTagKeysRecordByEvent(ctx, dbCluster, event)
		if err != nil {
			if errors.Is(err, base.DBTxError) {
				recordTaskError2(logger, metric, CollectEventsTaskName, err, "add_event_db_retry", map[string]string{"event": event.String()})
				time.Sleep(retryInterval)
				continue
			}
			return err
		}
		break
	}
	return nil
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
