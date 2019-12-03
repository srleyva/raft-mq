package server

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	empty "github.com/golang/protobuf/ptypes/empty"
	"github.com/lni/dragonboat/v3"
	"github.com/srleyva/raft-group-mq/pkg/statemachine"
	"google.golang.org/grpc"

	"github.com/srleyva/raft-group-mq/pkg/message"
)

type Server struct {
	nodehost *dragonboat.NodeHost
	addr       string
	ring 	   *consistent.Consistent
	ln         net.Listener
}

type ClusterID uint64

func (c ClusterID) Uint64() uint64 {
	return uint64(c)
}

func (c ClusterID) String() string {
	return fmt.Sprintf("%d", c)
}

// consistent package doesn't provide a default hashing function.
// You should provide a proper one to distribute keys/members uniformly.
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	// you should use a proper hash function for uniformity.
	return xxhash.Sum64(data)
}

func NewServer(addr string, clusters []consistent.Member, nodehost *dragonboat.NodeHost) *Server {
	ringConfig := consistent.Config{
		PartitionCount:    7,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
	clusterRing := consistent.New(clusters, ringConfig)
	return &Server{
		nodehost: nodehost,
		addr:       addr,
		ring: clusterRing,
	}
}

func (s *Server) Start() error {
	svr := grpc.NewServer()
	lis, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.ln = lis
	message.RegisterMessageBusServer(svr, s)
	go func() {
		if err := svr.Serve(s.ln); err != nil {
			log.Fatalf("GRPC: %s", err)
		}
	}()
	return nil
}

func (s *Server) Close() {
	s.ln.Close()
}

func (s *Server) Join(ctx context.Context, joinReq *message.JoinRequest) (*empty.Empty, error) {
	// TODO Implement join for raft group
	//if err := s.messageBus.Join(joinReq.NodeID, joinReq.RaftAddr); err != nil {
	//	return nil, err
	//}
	return &empty.Empty{}, nil
}

func (s *Server) NewMessage(ctx context.Context, message *message.Message) (*empty.Empty, error) {
	event := message.Event
	topic := message.Topic
	newMsg := &statemachine.Message{
		Event: event,
		Topic: topic,
	}

	op := statemachine.Cmd{
		Op:      statemachine.ADD,
		Message: newMsg,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	key := []byte(newMsg.Topic)
	clusterSession := s.nodehost.GetNoOPSession(s.ring.LocateKey(key).(ClusterID).Uint64())

	var opbytes bytes.Buffer
	enc := gob.NewEncoder(&opbytes)
	if err := enc.Encode(op); err != nil {
		return &empty.Empty{}, fmt.Errorf("err serializing data: %s", err)
	}

	_, err := s.nodehost.SyncPropose(ctx, clusterSession, opbytes.Bytes())

	cancel()
	if err != nil {
		return &empty.Empty{}, err
	}

	return &empty.Empty{}, nil
}

func (s *Server) ProccessMessage(ctx context.Context, _ *empty.Empty) (*message.Message, error) {
	msg, err := s.messageBus.ProccessMessage()
	if err != nil {
		return nil, err
	}
	return &message.Message{
		Event: msg.String(),
	}, nil
}

func (s *Server) ListMessages(_ *empty.Empty, stream message.MessageBus_ListMessagesServer) error {
	for _, item := range s.messageBus.ListMessages() {
		msg := &message.Message{
			Event: item.(store.Message).String(),
		}
		if err := stream.Send(msg); err != nil {
			return err
		}
	}
	return nil
}