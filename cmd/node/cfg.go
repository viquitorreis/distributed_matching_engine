package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
)

// run as i.e.:
// LISTEN_ADDR=localhost:9001 go run ./cmd/node localhost:9002 localhost:9003

// # terminal 2
// LISTEN_ADDR=localhost:9002 go run ./cmd/node localhost:9001 localhost:9003

// # terminal 3
// LISTEN_ADDR=localhost:9003 go run ./cmd/node localhost:9001 localhost:9002

// # then on one terminal: "order bid 100 10" and "order ask 100 10" on another one
// to cancel just "cancel <order_uuid>""

type Config struct {
	BindAddr      string
	AdvertiseAddr string // identity announced to peers, ex: "engine-0.engine-headless:8080"
	PeerAddrs     []string
}

func resolveConfig() Config {
	if replicaCount := os.Getenv("REPLICA_COUNT"); replicaCount != "" {
		return resolveK8sConfig(replicaCount)
	}

	// old path, local, keeps the same
	return Config{
		AdvertiseAddr: os.Getenv("LISTEN_ADDR"),
		PeerAddrs:     os.Args[1:],
	}
}

func resolveK8sConfig(replicaCountStr string) Config {
	n, err := strconv.Atoi(replicaCountStr)
	if err != nil {
		log.Fatalf("invalid REPLICA_COUNT: %v", err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("failed to get hostname: %v", err)
	}

	svcName := os.Getenv("SERVICE_NAME")
	if svcName == "" {
		log.Fatal("SERVICE_NAME must be set in k8s mode")
	}

	var peers []string
	for i := 0; i < n; i++ {
		peerHost := fmt.Sprintf("engine-%d", i)
		if peerHost == hostname {
			continue
		}
		peers = append(peers, fmt.Sprintf("%s.%s:8080", peerHost, svcName))
	}

	return Config{
		BindAddr:      ":8080",
		AdvertiseAddr: fmt.Sprintf("%s.%s:8080", hostname, svcName),
		PeerAddrs:     peers,
	}
}
