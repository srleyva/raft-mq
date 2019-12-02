package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/lni/goutils/syncutil"

	"github.com/srleyva/raft-group-mq/pkg/statemachine"
)

// Default Ports
const (
	DEFAULT_GRPC_PORT = 12000
)

// Command line parameters
var addr string
var port int
var joinAddr string
var nodeID uint64
var clusterCount uint64
var nodes []string

func main() {

	// Parse flags
	configureFlags()

	// Create the raft dir
	args := flag.Args()
	if len(args) == 0 || args[0] == "" {
		log.Fatal("Please specify raft dir")
	}

	raftDir := args[0]
	if err := os.MkdirAll(raftDir, 0700); err != nil {
		log.Fatalf("err creating raft dir: %s", err)
	}

	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.WARNING)
	logger.GetLogger("transport").SetLevel(logger.WARNING)
	logger.GetLogger("grpc").SetLevel(logger.WARNING)

	// Cluster config
	raftConfig := config.Config{
		NodeID:             nodeID,
		ElectionRTT:        5,
		HeartbeatRTT:       1,
		CheckQuorum:        true,
		SnapshotEntries:    10,
		CompactionOverhead: 5,
	}

	// Node Host config
	datadir := filepath.Join(
		raftDir,
		fmt.Sprintf("node%d", nodeID))

	initialMembers := make(map[uint64]string)
	for idx, v := range nodes {
		// key is the NodeID, NodeID is not allowed to be 0
		// value is the raft address
		initialMembers[uint64(idx+1)] = v
	}
	nodeAddr := fmt.Sprintf("%s:%d", addr, port)
	log.Printf("Node Address: %s", nodeAddr)
	nodeHostConfig := config.NodeHostConfig{
		WALDir:         datadir,
		NodeHostDir:    datadir,
		RTTMillisecond: 200,
		RaftAddress:    nodeAddr,
		// RaftRPCFactory: rpc.NewRaftGRPC,
	}

	// Create the nodehost
	nodeHost, err := dragonboat.NewNodeHost(nodeHostConfig)
	if err != nil {
		logger.CreateCapnsLog("main").Errorf("err creating nodehost: %s", err)
		syscall.Exit(1)
	}
	defer nodeHost.Stop()

	clusterID := uint64(100)
	clusters := []consistent.Member{}

	for i := uint64(0); i < clusterCount; i++ {
		raftConfig.ClusterID = clusterID
		log.Print(initialMembers)
		if err := nodeHost.StartCluster(initialMembers, false, statemachine.NewStateMachine, raftConfig); err != nil {
			fmt.Fprintf(os.Stderr, "failed to add cluster, %v\n", err)
			os.Exit(1)
		}
		clusters = append(clusters, ClusterID(clusterID))
		clusterID++
	}

	raftStopper := syncutil.NewStopper()
	consoleStopper := syncutil.NewStopper()
	ch := make(chan string, 16)
	consoleStopper.RunWorker(func() {
		reader := bufio.NewReader(os.Stdin)
		for {
			s, err := reader.ReadString('\n')
			if err != nil {
				close(ch)
				return
			}
			if s == "exit\n" {
				raftStopper.Stop()
				// no data will be lost/corrupted if nodehost.Stop() is not called
				nodeHost.Stop()
				return
			}
			ch <- s
		}
	})

	raftStopper.RunWorker(func() {
		// use NO-OP client session here
		// check the example in godoc to see how to use a regular client session
		ringConfig := consistent.Config{
			PartitionCount:    7,
			ReplicationFactor: 20,
			Load:              1.25,
			Hasher:            hasher{},
		}
		clusterRing := consistent.New(clusters, ringConfig)

		for {
			select {
			case v, ok := <-ch:
				if !ok {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				// remove the \n char
				msg := strings.Replace(strings.TrimSpace(v), "\n", "", 1)
				key := []byte(msg)
				clusterSession := nodeHost.GetNoOPSession(clusterRing.LocateKey(key).(ClusterID).Uint64())

				_, err := nodeHost.SyncPropose(ctx, clusterSession, key)

				cancel()
				if err != nil {
					fmt.Fprintf(os.Stderr, "SyncPropose returned error %v\n", err)
				}
			case <-raftStopper.ShouldStop():
				return
			}
		}
	})
	raftStopper.Wait()
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

func configureFlags() {

	var strNodes string

	flag.StringVar(&addr, "host", "localhost", "Set the address")
	flag.IntVar(&port, "port", DEFAULT_GRPC_PORT, "Port for the service")
	flag.StringVar(&strNodes, "nodes", "", ", delimited list of nodes in the cluster") // TODO: Implement surf service discovery
	flag.StringVar(&joinAddr, "join", "", "Set join address, if any")
	flag.Uint64Var(&nodeID, "node-id", uint64(1), "Node ID")
	flag.Uint64Var(&clusterCount, "cluster-count", uint64(1), "How many raft clusters to run")
	flag.Parse()
	nodes = strings.Split(strNodes, ",")

}
