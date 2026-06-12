package gracefulshutdown

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	gracefulv1 "github.com/museop/graceful-shutdown/gen/graceful/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type testServer struct {
	listener   net.Listener
	grpcServer *grpc.Server
	lifecycle  *Lifecycle
	conn       *grpc.ClientConn
	client     gracefulv1.GracefulServiceClient
	done       chan error
}

func startTestServer(t *testing.T, handle WorkHandler) *testServer {
	return startTestServerWithOptions(t, handle, ServiceOptions{})
}

func startTestServerWithOptions(t *testing.T, handle WorkHandler, options ServiceOptions) *testServer {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	lifecycle := NewLifecycle("server-a")
	grpcServer := grpc.NewServer(grpc.StatsHandler(lifecycle))
	gracefulv1.RegisterGracefulServiceServer(grpcServer, NewServiceWithOptions(lifecycle, handle, options))

	done := make(chan error, 1)
	go func() {
		done <- grpcServer.Serve(listener)
	}()

	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		listener.Close()
		t.Fatalf("dial: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		grpcServer.Stop()
		<-done
	})

	return &testServer{
		listener:   listener,
		grpcServer: grpcServer,
		lifecycle:  lifecycle,
		conn:       conn,
		client:     gracefulv1.NewGracefulServiceClient(conn),
		done:       done,
	}
}

func TestWatchStatusReceivesDrainingThenStopping(t *testing.T) {
	ts := startTestServer(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stream, err := ts.client.WatchStatus(ctx, &gracefulv1.WatchStatusRequest{ClientId: "test-client"})
	if err != nil {
		t.Fatalf("WatchStatus: %v", err)
	}

	assertStatus(t, stream, gracefulv1.ServiceStatus_SERVICE_STATUS_SERVING)

	shutdownErr := make(chan error, 1)
	go func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		shutdownErr <- GracefulShutdownWithOptions(shutdownCtx, ts.grpcServer, ts.lifecycle, ShutdownOptions{
			MaxDrainDuration:        20 * time.Millisecond,
			ConnectionCheckInterval: 5 * time.Millisecond,
			ZeroConnectionsDuration: time.Second,
		})
	}()

	assertStatus(t, stream, gracefulv1.ServiceStatus_SERVICE_STATUS_DRAINING)
	assertStatus(t, stream, gracefulv1.ServiceStatus_SERVICE_STATUS_STOPPING)

	_, err = stream.Recv()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected status stream EOF after STOPPING, got %v", err)
	}

	if err := <-shutdownErr; err != nil {
		t.Fatalf("GracefulShutdown: %v", err)
	}
}

func TestWatchStatusRepeatsCurrentStatusPeriodically(t *testing.T) {
	ts := startTestServerWithOptions(t, nil, ServiceOptions{StatusHeartbeatInterval: 10 * time.Millisecond})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stream, err := ts.client.WatchStatus(ctx, &gracefulv1.WatchStatusRequest{ClientId: "test-client"})
	if err != nil {
		t.Fatalf("WatchStatus: %v", err)
	}

	first := recvStatus(t, stream, gracefulv1.ServiceStatus_SERVICE_STATUS_SERVING)
	second := recvStatus(t, stream, gracefulv1.ServiceStatus_SERVICE_STATUS_SERVING)

	if second.GetUnixMillis() <= first.GetUnixMillis() {
		t.Fatalf("periodic status unix_millis = %d, want greater than first %d", second.GetUnixMillis(), first.GetUnixMillis())
	}
}

func TestGracefulShutdownCompletesInFlightWorkBeforeStopping(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})

	ts := startTestServer(t, func(ctx context.Context, req *gracefulv1.WorkRequest) ([]byte, error) {
		close(started)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return []byte("completed:" + req.GetRequestId()), nil
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stream, err := ts.client.Work(ctx)
	if err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := stream.Send(&gracefulv1.WorkRequest{RequestId: "in-flight", Payload: []byte("payload")}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("work handler did not start")
	}

	shutdownErr := make(chan error, 1)
	go func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		shutdownErr <- GracefulShutdown(shutdownCtx, ts.grpcServer, ts.lifecycle, 0)
	}()

	select {
	case err := <-shutdownErr:
		t.Fatalf("shutdown finished before in-flight work completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)

	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv response: %v", err)
	}
	if got, want := string(resp.GetPayload()), "completed:in-flight"; got != want {
		t.Fatalf("payload = %q, want %q", got, want)
	}
	if resp.GetObservedStatus() != gracefulv1.ServiceStatus_SERVICE_STATUS_SERVING {
		t.Fatalf("observed status = %s, want SERVING", resp.GetObservedStatus())
	}

	_, err = stream.Recv()
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected stream to finish with Unavailable after graceful state, got %v", err)
	}

	if err := <-shutdownErr; err != nil {
		t.Fatalf("GracefulShutdown: %v", err)
	}
}

func TestWorkAcceptsDuringDrainingAndRejectsAfterStopping(t *testing.T) {
	ts := startTestServer(t, nil)

	ts.lifecycle.SetStatus(gracefulv1.ServiceStatus_SERVICE_STATUS_DRAINING)
	resp := sendOneWork(t, ts.client, "draining")
	if resp.GetObservedStatus() != gracefulv1.ServiceStatus_SERVICE_STATUS_DRAINING {
		t.Fatalf("observed status = %s, want DRAINING", resp.GetObservedStatus())
	}

	ts.lifecycle.SetStatus(gracefulv1.ServiceStatus_SERVICE_STATUS_STOPPING)
	stream, err := ts.client.Work(context.Background())
	if err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := stream.Send(&gracefulv1.WorkRequest{RequestId: "rejected"}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable after STOPPING, got %v", err)
	}
}

func TestGracefulShutdownDoesNotHangOnIdleWorkStream(t *testing.T) {
	ts := startTestServer(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stream, err := ts.client.Work(ctx)
	if err != nil {
		t.Fatalf("Work: %v", err)
	}

	shutdownErr := make(chan error, 1)
	go func() {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer shutdownCancel()
		shutdownErr <- GracefulShutdown(shutdownCtx, ts.grpcServer, ts.lifecycle, 0)
	}()

	_, err = stream.Recv()
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected idle stream to be interrupted with Unavailable, got %v", err)
	}

	if err := <-shutdownErr; err != nil {
		t.Fatalf("GracefulShutdown: %v", err)
	}
}

func TestGracefulShutdownStopsAfterSustainedZeroConnections(t *testing.T) {
	ts := startTestServer(t, nil)

	started := time.Now()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer shutdownCancel()

	if err := GracefulShutdownWithOptions(shutdownCtx, ts.grpcServer, ts.lifecycle, ShutdownOptions{
		MaxDrainDuration:        time.Second,
		ConnectionCheckInterval: 5 * time.Millisecond,
		ZeroConnectionsDuration: 20 * time.Millisecond,
	}); err != nil {
		t.Fatalf("GracefulShutdownWithOptions: %v", err)
	}

	if elapsed := time.Since(started); elapsed >= 500*time.Millisecond {
		t.Fatalf("shutdown elapsed = %s, want well before max drain duration", elapsed)
	}
	if got := ts.lifecycle.Status(); got != gracefulv1.ServiceStatus_SERVICE_STATUS_STOPPING {
		t.Fatalf("status = %s, want STOPPING", got)
	}
}

func sendOneWork(t *testing.T, client gracefulv1.GracefulServiceClient, requestID string) *gracefulv1.WorkResponse {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	stream, err := client.Work(ctx)
	if err != nil {
		t.Fatalf("Work: %v", err)
	}
	if err := stream.Send(&gracefulv1.WorkRequest{RequestId: requestID, Payload: []byte(requestID)}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	return resp
}

func assertStatus(t *testing.T, stream gracefulv1.GracefulService_WatchStatusClient, want gracefulv1.ServiceStatus) {
	t.Helper()

	_ = recvStatus(t, stream, want)
}

func recvStatus(t *testing.T, stream gracefulv1.GracefulService_WatchStatusClient, want gracefulv1.ServiceStatus) *gracefulv1.StatusUpdate {
	t.Helper()

	update, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv status %s: %v", want, err)
	}
	if got := update.GetStatus(); got != want {
		t.Fatalf("status = %s, want %s", got, want)
	}
	return update
}
