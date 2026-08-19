package main

import (
	"context"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"raft_orderbook/cluster"
	"raft_orderbook/framing"
	"raft_orderbook/orderbook"
	"raft_orderbook/peer"
	"raft_orderbook/raft"
	"raft_orderbook/storage"
	"syscall"
	"time"
)

func main() {
	cfg := resolveConfig()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storageDir := os.Getenv("STORAGE_DIR")
	if storageDir == "" {
		storageDir = "."
	}

	f := framing.NewFraming(4)
	ob := orderbook.NewOrderBook("BTC-USD")
	totalNodes := len(cfg.PeerAddrs) + 1
	cl := cluster.NewCluster(
		ctx,
		cfg.AdvertiseAddr,
		ob,
		raft.NewRaft(
			uint64(totalNodes),
			storage.NewStorage(storageDir, cfg.AdvertiseAddr),
		),
	)

	go listenLoop(ctx, cfg.BindAddr, cfg.AdvertiseAddr, f, cl)

	for _, addr := range cfg.PeerAddrs {
		if cfg.AdvertiseAddr < addr {
			go dialWithRetry(ctx, cfg.AdvertiseAddr, normalizeAddr(addr), f, cl)
		}
	}

	go startCLI(cl)

	slog.Info("node is up and running", "port", cfg.AdvertiseAddr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	<-sigCh
	slog.Info("shutting down node")
	cancel()
}

// listenLoop accepts inbound connections from others on the cluster
func listenLoop(ctx context.Context, bindAddr, advertiseAddr string, f *framing.Framing, cl *cluster.Cluster) {
	ln, err := net.Listen("tcp", bindAddr)
	if err != nil {
		log.Fatalf("err starting listener: %v", err)
	}

	go func() {
		<-ctx.Done()
		ln.Close()
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				slog.Error("accept failed", "error", err)
				continue
			}
		}

		// remove nagles algorithm
		if tc, ok := conn.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}

		registerPeer(ctx, advertiseAddr, conn, f, cl)
	}
}

// dialWithRetry disks to a peer configured, retrying until it succeeds
// or the context closes. Without it, an up order of the nodes would matter
func dialWithRetry(ctx context.Context, ownAddr, peerAddr string, f *framing.Framing, cl *cluster.Cluster) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		conn, err := net.Dial("tcp", peerAddr)
		if err != nil {
			slog.Warn("dial failed, retrying", "addr", ownAddr, "error", err)
			select {
			case <-time.After(time.Second):
				continue
			case <-ctx.Done():
				return
			}
		}

		if tc, ok := conn.(*net.TCPConn); ok {
			tc.SetNoDelay(true)
		}

		registerPeer(ctx, ownAddr, conn, f, cl)

		return
	}
}

func registerPeer(
	ctx context.Context,
	localAddr string,
	conn net.Conn,
	f *framing.Framing,
	cl *cluster.Cluster,
) {
	// Handshake happens BEFORE any Peer goroutines start, conn is used
	// synchronously here, then handed off to peer.NewPeer only after
	// identity is confirmed.
	identity, err := peer.Handshake(conn, f, localAddr)
	if err != nil {
		slog.Error("handshake failed", "error", err)
		conn.Close()
		return
	}

	p := peer.NewPeer(ctx, identity, conn, f, cl.InboundChan())

	if !cl.TryRegister(identity, p) {
		slog.Warn("duplicate connection to already-registered peer, closing", "peer", identity)
		p.Close()
		return
	}

	slog.Info("peer registered after handshake", "peer", identity)
}

func normalizeAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}

	if host == "" || host == "localhost" || host == "127.0.0.1" {
		host = "127.0.0.1"
	}

	return net.JoinHostPort(host, port)
}
