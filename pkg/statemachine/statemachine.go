// Copyright 2017,2018 Lei Ni (nilei81@gmail.com).
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package statemachine

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/lni/dragonboat/v3/logger"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/srleyva/raft-group-mq/pkg/queue"
)

const (
	ADD = "ADD"
	POP = "POP"
)

var recvProccessCount = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Name: "recv_proccess_count",
		Help: "Number messages processed by recvs",
	},
	[]string{"name"})

var queueMessageCount = prometheus.NewGauge(
	prometheus.GaugeOpts{
		Name: "message_queue_count",
	})

func init() {
	prometheus.MustRegister(recvProccessCount)
	prometheus.MustRegister(queueMessageCount)
}

type Message struct {
	Event string
}

// StateMachine is the IStateMachine implementation used in the example
// for handling all inputs not ends with "?".
// See https://github.com/lni/dragonboat/blob/master/statemachine/rsm.go for
// more details of the IStateMachine interface.
type StateMachine struct {
	ClusterID uint64
	NodeID    uint64
	Count     uint64
	queue     map[string]*queue.Queue
}

type Cmd struct {
	Op      string   `json:"op"`
	Topic   string   `json:"topic"`
	Message *Message `json:"message"`
}

// NewStateMachine creates and return a new StateMachine object.
func NewStateMachine(clusterID uint64, nodeID uint64) sm.IStateMachine {
	return &StateMachine{
		ClusterID: clusterID,
		NodeID:    nodeID,
		Count:     0,
		queue:     map[string]*queue.Queue{},
	}
}

// Lookup performs local lookup on the StateMachine instance. In this example,
// we always return the Count value as a little endian binary encoded byte
// slice.
func (s *StateMachine) Lookup(query interface{}) (interface{}, error) {
	if s.queue[query.(string)] == nil {
		return nil, fmt.Errorf("no such queue")
	}
	return s.queue[query.(string)], nil
}

// Update updates the object using the specified committed raft entry.
func (s *StateMachine) Update(data []byte) (res sm.Result, err error) {
	var command Cmd
	var opBytes bytes.Buffer
	opBytes.Write(data)
	dec := gob.NewDecoder(&opBytes)
	if err := dec.Decode(&command); err != nil {
		logger.GetLogger("rsm").Errorf("err marshalling Json: %s", err)
	}

	switch command.Op {
	case ADD:
		// Check if queue exists and create if not
		if s.queue[command.Topic] == nil {
			s.queue[command.Topic], err = queue.NewQueue(command.Topic, nil)
			if err != nil {
				return sm.Result{
					Value: 0,
					Data:  []byte(fmt.Sprintf("err creating topic: %s", err)),
				}, nil
			}
		}
		s.queue[command.Topic].InsertItem(command.Message)
		queueMessageCount.Inc()
		recvProccessCount.WithLabelValues(fmt.Sprintf("raft-%d", s.ClusterID)).Inc()
		return sm.Result{
			Value: uint64(len(command.Message.Event)),
			Data:  []byte(fmt.Sprintf("Message Written: %s", command.Message.Event)),
		}, nil
	case POP:
		if s.queue[command.Topic] == nil {
			return sm.Result{
				Value: 0,
				Data:  []byte(fmt.Sprintf("err no such topic")),
			}, nil
		}
		message, err := s.queue[command.Topic].Pop()
		if err != nil {
			return sm.Result{
				Value: 0,
				Data:  []byte(fmt.Sprintf("err popping off message: %s", err)),
			}, nil
		}
		queueMessageCount.Dec()
		msg := message.(*Message).Event
		return sm.Result{
			Value: uint64(len(msg)),
			Data:  []byte(msg),
		}, nil
	default:
		return sm.Result{
			Value: 0,
			Data:  []byte(fmt.Sprintf("cmd not known: %s", command.Op)),
		}, nil
	}
}

// TODO snapshot stuff
// SaveSnapshot saves the current IStateMachine state into a snapshot using the
// specified io.Writer object.
func (s *StateMachine) SaveSnapshot(w io.Writer,
	fc sm.ISnapshotFileCollection, done <-chan struct{}) error {
	// as shown above, the only state that can be saved is the Count variable
	// there is no external file in this IStateMachine example, we thus leave
	// the fc untouched
	data := make([]byte, 8)
	binary.LittleEndian.PutUint64(data, s.Count)
	_, err := w.Write(data)
	return err
}

// RecoverFromSnapshot recovers the state using the provided snapshot.
func (s *StateMachine) RecoverFromSnapshot(r io.Reader,
	files []sm.SnapshotFile, done <-chan struct{}) error {
	// restore the Count variable, that is the only state we maintain in this
	// example, the input files is expected to be empty
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return err
	}
	v := binary.LittleEndian.Uint64(data)
	s.Count = v
	return nil
}

// Close closes the IStateMachine instance. There is nothing for us to cleanup
// or release as this is a pure in memory data store. Note that the Close
// method is not guaranteed to be called as node can crash at any time.
func (s *StateMachine) Close() error { return nil }

// GetHash returns a uint64 representing the current object state.
func (s *StateMachine) GetHash() (uint64, error) {
	// the only state we have is that Count variable. that uint64 value pretty much
	// represents the state of this IStateMachine
	return s.Count, nil
}
