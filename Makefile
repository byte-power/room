roomBIN="linux_room_service"
syncBIN="linux_room_sync"
collectEventsBIN="linux_room_collect_event"
testTransactionBin="linux_trans"

build-room:
	CGO_ENABLED=0 GOOS=linux GOARCH=386 go build -ldflags '-w -s' -v -o $(roomBIN) cmd/room/main.go

build-sync:
	CGO_ENABLED=0 GOOS=linux GOARCH=386 go build -ldflags '-w -s' -v -o $(syncBIN) cmd/sync/main.go

build-collect-event:
	CGO_ENABLED=0 GOOS=linux GOARCH=386 go build -ldflags '-w -s' -v -o $(collectEventsBIN) cmd/collect_event/main.go

build-trans:
	CGO_ENABLED=0 GOOS=linux GOARCH=386 go build -ldflags '-w -s' -v -o $(testTransactionBin) cmd/tools/transaction.go

build-all: build-room build-sync build-collect-event

