module github.com/srleyva/raft-group-mq

replace github.com/srleyva/raft-group-mq/pkg/queue => ./pkg/queue

replace github.com/srleyva/raft-group-mq/pkg/message => ./pkg/message

replace github.com/srleyva/raft-group-mq/pkg/statemachine => ./pkg/statemachine

go 1.13

require (
	git.apache.org/thrift.git v0.13.0 // indirect
	github.com/buraksezer/consistent v0.0.0-20191006190839-693edf70fd72
	github.com/cespare/xxhash v1.1.0
	github.com/cockroachdb/pebble v0.0.0-20191126181905-c3a0c4720a96 // indirect
	github.com/golang/protobuf v1.3.2
	github.com/lni/dragonboat v2.1.7+incompatible // indirect
	github.com/lni/dragonboat/v3 v3.1.4
	github.com/lni/goutils v1.0.3
	github.com/prometheus/client_golang v1.2.1
	google.golang.org/grpc v1.25.1
)
