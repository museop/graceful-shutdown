package gracefulshutdown

import (
	"context"
	"time"

	gracefulv1 "github.com/museop/graceful-shutdown/gen/graceful/v1"
	"google.golang.org/grpc"
)

// GracefulShutdown implements the lifecycle requested by the service owner:
//
//  1. publish DRAINING immediately so clients stop choosing this backend,
//  2. keep accepting gRPC work during the drain window,
//  3. publish GRACEFUL_SHUTDOWN after drainDelay,
//  4. call grpc.Server.GracefulStop so no new RPCs are accepted while active
//     RPC handlers finish,
//  5. force Stop only if ctx expires.
func GracefulShutdown(ctx context.Context, grpcServer *grpc.Server, lifecycle *Lifecycle, drainDelay time.Duration) error {
	lifecycle.SetStatus(gracefulv1.ServiceStatus_SERVICE_STATUS_DRAINING)

	if drainDelay > 0 {
		timer := time.NewTimer(drainDelay)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-ctx.Done():
			lifecycle.SetStatus(gracefulv1.ServiceStatus_SERVICE_STATUS_GRACEFUL_SHUTDOWN)
			grpcServer.Stop()
			return ctx.Err()
		}
	}

	lifecycle.SetStatus(gracefulv1.ServiceStatus_SERVICE_STATUS_GRACEFUL_SHUTDOWN)

	stopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(stopped)
	}()

	select {
	case <-stopped:
		return nil
	case <-ctx.Done():
		grpcServer.Stop()
		<-stopped
		return ctx.Err()
	}
}
