package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	gracefulshutdown "github.com/museop/graceful-shutdown"
	gracefulv1 "github.com/museop/graceful-shutdown/gen/graceful/v1"
	"google.golang.org/grpc"
)

func main() {
	addr := flag.String("addr", ":50051", "listen address")
	serverID := flag.String("server-id", hostname(), "server id published to clients")
	drainTimeout := flag.Duration("drain-timeout", 30*time.Second, "maximum time to remain in DRAINING before STOPPING")
	flag.Parse()

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen %s: %v", *addr, err)
	}

	lifecycle := gracefulshutdown.NewLifecycle(*serverID)
	grpcServer := grpc.NewServer(grpc.StatsHandler(lifecycle))
	gracefulv1.RegisterGracefulServiceServer(grpcServer, gracefulshutdown.NewService(lifecycle, nil))

	serveErr := make(chan error, 1)
	go func() {
		log.Printf("server %s listening on %s", *serverID, listener.Addr())
		serveErr <- grpcServer.Serve(listener)
	}()

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()

	select {
	case <-signalCtx.Done():
		log.Printf("signal received: publishing DRAINING for up to %s", *drainTimeout)
	case err := <-serveErr:
		if err != nil {
			log.Fatalf("grpc serve: %v", err)
		}
		return
	}

	if err := gracefulshutdown.GracefulShutdown(context.Background(), grpcServer, lifecycle, *drainTimeout); err != nil {
		log.Printf("shutdown completed after interrupted drain: %v", err)
	} else {
		log.Printf("shutdown completed gracefully")
	}

	if err := <-serveErr; err != nil && err != grpc.ErrServerStopped {
		log.Fatalf("grpc serve: %v", err)
	}
}

func hostname() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return fmt.Sprintf("server-%d", time.Now().UnixNano())
	}
	return host
}
