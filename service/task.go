package service

import (
	"bytepower_room/base"
	"bytepower_room/base/log"
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

func postEventsHandler(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.Header().Set(HTTPHeaderContentType, HTTPContentTypeJSON)
		writer.WriteHeader(http.StatusMethodNotAllowed)
		writer.Write([]byte("{}"))
		return
	}
	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		writer.Header().Set(HTTPHeaderContentType, HTTPContentTypeJSON)
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write(generateErrorResp(err))
		return
	}
	events := make([]base.Event, 0)
	if err := json.Unmarshal(body, &events); err != nil {
		writer.Header().Set(HTTPHeaderContentType, HTTPContentTypeJSON)
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write(generateErrorResp(err))
		return
	}
	for _, event := range events {
		fmt.Println(event.String())
	}
	writer.Header().Set(HTTPHeaderContentType, HTTPContentTypeJSON)
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("{}"))
}

func generateErrorResp(err error) []byte {
	response := map[string]string{
		"error": err.Error(),
	}
	result, _ := json.Marshal(response)
	return result
}

func SyncHashTagKeys() {

}

func CleanHashTagKeys() {

}
