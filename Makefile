serverBIN="linux_room_server"
taskBIN="linux_room_task"
collectEventBIN="linux_room_collect_event"
testTransactionBIN="linux_trans"

build-server:
	CGO_ENABLED=0 GOOS=linux GOARCH=386 go build -ldflags '-w -s' -v -o $(serverBIN) cmd/server/main.go

build-collect-event:
	CGO_ENABLED=0 GOOS=linux GOARCH=386 go build -ldflags '-w -s' -v -o $(collectEventBIN) cmd/collect_event/main.go

build-task:
	CGO_ENABLED=0 GOOS=linux GOARCH=386 go build -ldflags '-w -s' -v -o $(taskBIN) cmd/task/main.go

build-trans:
	CGO_ENABLED=0 GOOS=linux GOARCH=386 go build -ldflags '-w -s' -v -o $(testTransactionBIN) cmd/tools/transaction.go

build-all: build-server build-collect-event build-task

