roomBIN="linux_room_service"
syncBIN="linux_room_sync"
testTransactionBin="linux_trans"

build-room:
	CGO_ENABLED=0 GOOS=linux GOARCH=386 go build -ldflags '-w -s' -v -o $(roomBIN) cmd/room/main.go

build-sync:
	CGO_ENABLED=0 GOOS=linux GOARCH=386 go build -ldflags '-w -s' -v -o $(syncBIN) cmd/sync/main.go

build-trans:
	CGO_ENABLED=0 GOOS=linux GOARCH=386 go build -ldflags '-w -s' -v -o $(testTransactionBin) cmd/tools/transaction.go
