version=1.4.1
serverExec=room_server
taskExec=room_task
collectEventExec=room_collect_event
testTxExec=room_trans

clean:
	rm -f ${serverExec}* ${taskExec}* ${collectEventExec}* ${testTxExec}*

build-prod-server:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags '-w -s -X "main.version=${version}"' -v -o $(serverExec)_${version} cmd/server/main.go

build-prod-collect-event:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags '-w -s -X "main.version=${version}"' -v -o $(collectEventExec)_${version} cmd/collect_event/main.go

build-prod-task:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags '-w -s -X "main.version=${version}"' -v -o $(taskExec)_${version} cmd/task/main.go

build-prod-all: build-prod-server build-prod-collect-event build-prod-task

build-local-server:
	go build -ldflags '-X "main.version=${version}"' -v -o $(serverExec)_${version} cmd/server/main.go

build-local-collect-event:
	go build -ldflags '-X "main.version=${version}"' -v -o $(collectEventExec)_${version} cmd/collect_event/main.go

build-local-task:
	go build -ldflags '-X "main.version=${version}"' -v -o $(taskExec)_${version} cmd/task/main.go

build-local-tx:
	go build -v -o $(testTxExec) cmd/tools/transaction.go

build-local-all: build-local-server build-local-collect-event build-local-task

build-linux-server:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags '-X "main.version=${version}"' -v -o $(serverExec)_${version} cmd/server/main.go

build-linux-collect-event:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags '-X "main.version=${version}"' -v -o $(collectEventExec)_${version} cmd/collect_event/main.go

build-linux-task:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags '-X "main.version=${version}"' -v -o $(taskExec)_${version} cmd/task/main.go

build-linux-tx:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -v -o $(testTxExec) cmd/tools/transaction.go

build-linux-all: build-linux-server build-linux-collect-event build-linux-task
