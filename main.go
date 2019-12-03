package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/buraksezer/consistent"
	"github.com/gorilla/mux"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/logger"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/srleyva/raft-group-mq/pkg/server"
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
		clusters = append(clusters, server.ClusterID(clusterID))
		clusterID++
	}

	grpcAddr := fmt.Sprintf("%s:%d", addr, port+1)
	log.Printf("GRPC Addr: %s", grpcAddr)
	server := server.NewServer(grpcAddr, clusters, nodeHost)
	if err := server.Start(); err != nil {
		log.Fatalf("failed to start GRPC service: %s", err.Error())
	}
	go func() {
		mux := mux.NewRouter()
		AttachProfiler(mux)
		httpAddr := fmt.Sprintf("%s:%d", addr, port+2)
		if err := http.ListenAndServe(httpAddr, mux); err != nil {
			log.Fatal(err)
		}
		log.Printf("HTTP Server is running on %s", httpAddr)
	}()

	terminate := make(chan os.Signal, 1)
	signal.Notify(terminate, os.Interrupt)
	<-terminate
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

func AttachProfiler(router *mux.Router) {
	router.Handle("/metrics", promhttp.Handler())
	router.HandleFunc("/debug/pprof/", pprof.Index)
	router.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	router.HandleFunc("/debug/pprof/profile", pprof.Profile)
	router.HandleFunc("/debug/pprof/symbol", pprof.Symbol)

	// Manually add support for paths linked to by index page at /debug/pprof/
	router.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	router.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	router.Handle("/debug/pprof/block", pprof.Handler("block"))
}
