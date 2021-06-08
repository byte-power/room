package service

import (
	"bytepower_room/base"
	"errors"
	"net/http"
	"time"
)

const (
	HTTPHeaderContentType = "Content-Type"
	HTTPContentTypeJSON   = "application/json"
)

func SyncEvents() {
	logger := base.GetTaskLogger()
	metric := base.GetTaskMetricService()
	taskName := "sync_events"
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
}

func postEventsHandler(writer http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writer.Header().Set(HTTPHeaderContentType, HTTPContentTypeJSON)
		writer.WriteHeader(http.StatusMethodNotAllowed)
		writer.Write([]byte("{}"))
		return
	}
}

func SyncHashTagKeys() {

}

func CleanHashTagKeys() {

}
