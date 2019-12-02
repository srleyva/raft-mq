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
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"

	"github.com/lni/dragonboat/v3/logger"
	sm "github.com/lni/dragonboat/v3/statemachine"

	"github.com/srleyva/raft-group-mq/pkg/queue"
)

// StateMachine is the IStateMachine implementation used in the example
// for handling all inputs not ends with "?".
// See https://github.com/lni/dragonboat/blob/master/statemachine/rsm.go for
// more details of the IStateMachine interface.
type StateMachine struct {
	ClusterID uint64
	NodeID    uint64
	Count     uint64
	Queue *queue.Queue
}

type cmd struct {
	Op string `json:"op"`
	Message string `json:"message"`
}

// NewStateMachine creates and return a new StateMachine object.
func NewStateMachine(clusterID uint64, nodeID uint64) sm.IStateMachine {
	queue, err := queue.NewQueue("internal-queue", nil)
	if err != nil {
		log.Fatalf("error creating statemachine: %s", err)
	}
	return &StateMachine{
		ClusterID: clusterID,
		NodeID:    nodeID,
		Count:     0,
		Queue: queue,
	}
}

// Lookup performs local lookup on the StateMachine instance. In this example,
// we always return the Count value as a little endian binary encoded byte
// slice.
func (s *StateMachine) Lookup(query interface{}) (interface{}, error) {
	result := make([]byte, 8)
	binary.LittleEndian.PutUint64(result, s.Count)
	return result, nil
}

// Update updates the object using the specified committed raft entry.
func (s *StateMachine) Update(data []byte) (sm.Result, error) {
	// in this example, we print out the following message for each
	// incoming update request. we also increase the counter by one to remember
	// how many updates we have applied
	var command cmd
	if err := json.Unmarshal(data, &command); err != nil {
		logger.GetLogger("rsm").Errorf("err marshalling Json: %s", err)
		return sm.Result{}, nil
	}
	s.Count++
	fmt.Printf("from StateMachine.Update(), cluster: %d, nodeID: %d, op: %s, msg: %s, count:%d\n",
		s.ClusterID,
		s.NodeID,
		command.Op,
		command.Message,
		s.Count)
	return sm.Result{Value: uint64(len(data))}, nil
}

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
