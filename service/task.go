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

	ReportEventsTaskName     = "report_events"
	SyncHashtagKeysTaskName  = "sync_keys"
	CleanHashTagkeysTaskName = "clean_keys"
)

func ReportEvents() {
	logger := base.GetTaskLogger()
	metric := base.GetTaskMetricService()
	taskName := "report_events"
	mux := http.NewServeMux()
	mux.HandleFunc("/events", postEventsHandler)
	server := &http.Server{
		Addr:         "127.0.0.1:8089",
		Handler:      mux,
		ReadTimeout:  2000 * time.Millisecond,
		WriteTimeout: 2000 * time.Millisecond,
		IdleTimeout:  120 * time.Minute,
	}
	go func() {
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			recordTaskError2(logger, metric, taskName, err, "listen_serve", nil)
		}
	}()
	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	sig := <-signalCh
	logger.Info("signal received, closing report events service...", log.String("signal", sig.String()))
	if err := server.Close(); err != nil {
		logger.Error("close report events service error", log.Error(err))
	} else {
		logger.Info("close report events service success")
	}
}

const failedReasonWriteToClient = "write_to_client"

func postEventsHandler(writer http.ResponseWriter, request *http.Request) {
	dep := base.GetTaskDependency()
	if request.Method != http.MethodPost {
		err := fmt.Errorf("method %s is not allowed", request.Method)
		if writeErr := writeErrorResponse(writer, http.StatusMethodNotAllowed, err); writeErr != nil {
			recordTaskError2(
				dep.Logger,
				dep.Metric,
				ReportEventsTaskName,
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
				ReportEventsTaskName,
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
				ReportEventsTaskName,
				writeErr,
				failedReasonWriteToClient,
				map[string]string{"body": utility.AnyToString(body)})
		}
		return
	}
	retryTimes := 3
	retryInterval := 10 * time.Millisecond
	timeout := 2 * time.Second
	for _, event := range events {
		if err := event.Check(); err != nil {
			if writeErr := writeErrorResponse(writer, http.StatusBadRequest, err); writeErr != nil {
				recordTaskError2(
					dep.Logger,
					dep.Metric,
					ReportEventsTaskName,
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
					ReportEventsTaskName,
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
			ReportEventsTaskName,
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
				recordTaskError2(logger, metric, ReportEventsTaskName, err, "add_event_db_retry", map[string]string{"event": event.String()})
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

func SyncHashTagKeys() {

}

func CleanHashTagKeys() {

}
