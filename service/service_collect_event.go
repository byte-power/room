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

	"github.com/go-redis/redis/v8"
)

const (
	HTTPHeaderContentType = "Content-Type"
	HTTPContentTypeJSON   = "application/json"

	CollectEventsTaskName = "collect_events"

	readHashTagZSetName  = "room:hash_tag:read"
	writeHashTagZSetName = "room:hash_tag:write"
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
			recordTaskErrorV2(logger, metric, CollectEventsTaskName, err, "listen_serve", nil)
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

type CollectEventsRequestBody struct {
	Events []base.HashTagEvent `json:"events"`
}

func postEventsHandler(writer http.ResponseWriter, request *http.Request) {
	startTime := time.Now()
	config := base.GetServerConfig().CollectEventService.AddEventToRedis
	dep := base.GetTaskDependency()
	if request.Method != http.MethodPost {
		err := fmt.Errorf("method %s is not allowed", request.Method)
		recordTaskErrorV2(dep.Logger, dep.Metric, CollectEventsTaskName, err, "method_not_allowed", map[string]string{"method": request.Method})
		if writeErr := writeErrorResponse(writer, http.StatusMethodNotAllowed, err); writeErr != nil {
			recordTaskErrorV2(
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
		recordTaskErrorV2(dep.Logger, dep.Metric, CollectEventsTaskName, err, "read_body", nil)
		if writeErr := writeErrorResponse(writer, http.StatusInternalServerError, err); writeErr != nil {
			recordTaskErrorV2(
				dep.Logger,
				dep.Metric,
				CollectEventsTaskName,
				writeErr,
				failedReasonWriteToClient,
				nil)
		}
		return
	}
	dep.Logger.Debug("collect_event_service receive event", log.String("body", string(body)))
	dep.Logger.Info("collect_event_service receive request", log.Int("body_length", len(body)))
	requestBodyStruct := CollectEventsRequestBody{}
	if err := json.Unmarshal(body, &requestBodyStruct); err != nil {
		recordTaskErrorV2(dep.Logger, dep.Metric, CollectEventsTaskName, err, "unmarshal_body", map[string]string{"body": string(body)})
		if writeErr := writeErrorResponse(writer, http.StatusBadRequest, err); writeErr != nil {
			recordTaskErrorV2(
				dep.Logger,
				dep.Metric,
				CollectEventsTaskName,
				writeErr,
				failedReasonWriteToClient,
				map[string]string{"body": utility.AnyToString(body)})
		}
		return
	}
	events := requestBodyStruct.Events
	for _, event := range events {
		if err := event.Check(); err != nil {
			recordTaskErrorV2(dep.Logger, dep.Metric, CollectEventsTaskName, err, "event_check", map[string]string{"event": event.String()})
			if writeErr := writeErrorResponse(writer, http.StatusBadRequest, err); writeErr != nil {
				recordTaskErrorV2(
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

	timeout := time.Duration(config.TimeoutMS) * time.Millisecond
	err = addEventsToRedis(dep.Redis, events, config.BulkSize, timeout)
	if err != nil {
		recordTaskErrorV2(dep.Logger, dep.Metric, CollectEventsTaskName, err, "add_event_to_redis", map[string]string{"body": string(body)})
		if writeErr := writeErrorResponse(writer, http.StatusInternalServerError, err); writeErr != nil {
			recordTaskErrorV2(
				dep.Logger,
				dep.Metric,
				CollectEventsTaskName,
				writeErr,
				failedReasonWriteToClient,
				map[string]string{"body": utility.AnyToString(body)})
		}
		return
	}
	// for _, event := range events {
	// 	if err := event.Check(); err != nil {
	// 		recordTaskErrorV2(dep.Logger, dep.Metric, CollectEventsTaskName, err, "event_check", map[string]string{"event": event.String()})
	// 		if writeErr := writeErrorResponse(writer, http.StatusBadRequest, err); writeErr != nil {
	// 			recordTaskErrorV2(
	// 				dep.Logger,
	// 				dep.Metric,
	// 				CollectEventsTaskName,
	// 				writeErr,
	// 				failedReasonWriteToClient,
	// 				map[string]string{"body": utility.AnyToString(body)})
	// 		}
	// 		return
	// 	}
	// 	err := addEventToDB(dep.DB, dep.Logger, dep.Metric, event, retryTimes, retryInterval, timeout)
	// 	if err != nil {
	// 		recordTaskErrorV2(dep.Logger, dep.Metric, CollectEventsTaskName, err, "add_event", map[string]string{"event": event.String()})
	// 		if writeErr := writeErrorResponse(writer, http.StatusInternalServerError, err); writeErr != nil {
	// 			recordTaskErrorV2(
	// 				dep.Logger,
	// 				dep.Metric,
	// 				CollectEventsTaskName,
	// 				writeErr,
	// 				failedReasonWriteToClient,
	// 				map[string]string{"body": utility.AnyToString(body)})
	// 		}
	// 		return
	// 	}
	// }
	if writeErr := writeSuccessResponse(writer, len(events)); writeErr != nil {
		recordTaskErrorV2(
			dep.Logger,
			dep.Metric,
			CollectEventsTaskName,
			writeErr,
			failedReasonWriteToClient,
			map[string]string{"body": utility.AnyToString(body)})
	}
	recordTaskSuccessV2(dep.Logger, dep.Metric, CollectEventsTaskName, time.Since(startTime))
	recordTaskSuccessInfo(dep.Logger, dep.Metric, CollectEventsTaskName, "add_event_to_redis", len(events))
}

func addEventsToRedis(client *redis.ClusterClient, events []base.HashTagEvent, bulkSize int, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	readValidEvents := make([]interface{}, 0)
	writeValidEvents := make([]interface{}, 0)
	for _, event := range events {
		if err := event.Check(); err != nil {
			return err
		}
		if event.AccessMode == base.HashTagAccessModeRead {
			readValidEvents = append(readValidEvents, event)
		} else {
			writeValidEvents = append(writeValidEvents, event)
		}
	}
	err := addEventsToRedisByAccessMode(ctx, client, readValidEvents, base.HashTagAccessModeRead, bulkSize)
	if err != nil {
		return err
	}
	return addEventsToRedisByAccessMode(ctx, client, writeValidEvents, base.HashTagAccessModeWrite, bulkSize)
}

const scriptSrc = `
	local key = KEYS[1]
	if key == nil or key == '' then
		return {err: 'key is empty'}
	end

	local field_count = tonumber(ARGV[1])
	if field_count == nil then
		return {err: 'filed count is empty'}
	end

	if field_count <= 0 then 
		return {err: 'field count should be greater than 0'}
	end

	local processed_count = 0
	for i=2,field_count*2,2 do
		local field = ARGV[i]
		local score = ARGV[i+1]
		local existed_score = redis.call('zscore', field)
		if existed_score == nil or tonumber(existed_score) < tonumber(score) then
			processed_count = processed_count + 1
			redis.call('zadd', key, score, field)
		end
	end
	return processed_count
`

func addEventsToRedisByAccessMode(ctx context.Context, client *redis.ClusterClient, events []interface{}, mode base.HashTagAccessMode, bulkSize int) error {
	ess, err := utility.SplitSliceBySize(events, bulkSize)
	if err != nil {
		return err
	}
	var redisKey string
	if mode == base.HashTagAccessModeRead {
		redisKey = readHashTagZSetName
	} else {
		redisKey = writeHashTagZSetName
	}
	script := redis.NewScript(scriptSrc)
	for _, es := range ess {
		args := make([]interface{}, 0, len(es))
		for _, e := range es {
			if event, ok := e.(base.HashTagEvent); ok {
				args = append(args, event.HashTag, utility.TimestampInMS(event.AccessTime))
			} else {
				return fmt.Errorf("invalid event %+v\n", e)
			}
		}
		_, err := script.Run(ctx, client, []string{redisKey}, args...).Result()
		if err != nil {
			return err
		}
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
