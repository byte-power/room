#!/bin/sh
cd ..
go test bytepower_room/base -v
go test bytepower_room/utility -v
go test bytepower_room/commands -v
go test bytepower_room/service -v