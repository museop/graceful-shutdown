package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	gracefulv1 "github.com/museop/graceful-shutdown/gen/graceful/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type backend struct {
	addr   string
	client gracefulv1.GracefulServiceClient
	conn   *grpc.ClientConn

	mu     sync.RWMutex
	server string
	status gracefulv1.ServiceStatus
}

func main() {
	serversFlag := flag.String("servers", "localhost:50051,localhost:50052,localhost:50053", "comma-separated backend addresses")
	clientID := flag.String("client-id", fmt.Sprintf("client-%d", time.Now().UnixNano()), "client identifier sent on status streams")
	concurrency := flag.Int("concurrency", 8, "number of concurrent work streams")
	interval := flag.Duration("interval", 200*time.Millisecond, "delay between requests per worker")
	duration := flag.Duration("duration", 0, "optional run duration; 0 means until interrupted")
	flag.Parse()

	ctx := context.Background()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	backends := dialBackends(strings.Split(*serversFlag, ","))
	defer func() {
		for _, backend := range backends {
			_ = backend.conn.Close()
		}
	}()

	for _, backend := range backends {
		go watchStatus(ctx, backend, *clientID)
	}

	var wg sync.WaitGroup
	for worker := 0; worker < *concurrency; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			runWorker(ctx, worker, backends, *interval)
		}(worker)
	}
	wg.Wait()
}

func dialBackends(addrs []string) []*backend {
	backends := make([]*backend, 0, len(addrs))
	for _, rawAddr := range addrs {
		addr := strings.TrimSpace(rawAddr)
		if addr == "" {
			continue
		}
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("dial %s: %v", addr, err)
		}
		backends = append(backends, &backend{
			addr:   addr,
			conn:   conn,
			client: gracefulv1.NewGracefulServiceClient(conn),
			status: gracefulv1.ServiceStatus_SERVICE_STATUS_UNSPECIFIED,
		})
	}
	if len(backends) == 0 {
		log.Fatal("no backend addresses configured")
	}
	return backends
}

func watchStatus(ctx context.Context, backend *backend, clientID string) {
	for ctx.Err() == nil {
		stream, err := backend.client.WatchStatus(ctx, &gracefulv1.WatchStatusRequest{ClientId: clientID})
		if err != nil {
			backend.setStatus("", gracefulv1.ServiceStatus_SERVICE_STATUS_UNSPECIFIED)
			sleep(ctx, time.Second)
			continue
		}

		for {
			update, err := stream.Recv()
			if err != nil {
				if err != io.EOF && ctx.Err() == nil {
					log.Printf("status stream %s ended: %v", backend.addr, err)
				}
				backend.setStatus("", gracefulv1.ServiceStatus_SERVICE_STATUS_UNSPECIFIED)
				break
			}
			backend.setStatus(update.GetServerId(), update.GetStatus())
			log.Printf("backend %s/%s -> %s", backend.addr, update.GetServerId(), update.GetStatus())
		}
		sleep(ctx, time.Second)
	}
}

func runWorker(ctx context.Context, worker int, backends []*backend, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for requestNo := 0; ctx.Err() == nil; requestNo++ {
		eligible := servingBackends(backends)
		if len(eligible) == 0 {
			log.Printf("worker %d: no SERVING backend available", worker)
		} else {
			backend := eligible[rand.Intn(len(eligible))]
			if err := sendWork(ctx, backend, worker, requestNo); err != nil {
				log.Printf("worker %d: %s request failed: %v", worker, backend.addr, err)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func sendWork(ctx context.Context, backend *backend, worker, requestNo int) error {
	stream, err := backend.client.Work(ctx)
	if err != nil {
		return err
	}

	requestID := fmt.Sprintf("w%d-%d", worker, requestNo)
	if err := stream.Send(&gracefulv1.WorkRequest{RequestId: requestID, Payload: []byte(requestID)}); err != nil {
		return err
	}
	if err := stream.CloseSend(); err != nil {
		return err
	}

	resp, err := stream.Recv()
	if err != nil {
		return err
	}
	log.Printf("worker %d: %s handled by %s status=%s", worker, requestID, resp.GetServerId(), resp.GetObservedStatus())
	_, err = stream.Recv()
	if err == io.EOF {
		return nil
	}
	return err
}

func servingBackends(backends []*backend) []*backend {
	serving := make([]*backend, 0, len(backends))
	for _, backend := range backends {
		if backend.currentStatus() == gracefulv1.ServiceStatus_SERVICE_STATUS_SERVING {
			serving = append(serving, backend)
		}
	}
	return serving
}

func (b *backend) setStatus(server string, status gracefulv1.ServiceStatus) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.server = server
	b.status = status
}

func (b *backend) currentStatus() gracefulv1.ServiceStatus {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.status
}

func sleep(ctx context.Context, duration time.Duration) {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
