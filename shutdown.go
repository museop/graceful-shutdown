package gracefulshutdown

import (
	"context"
	"time"

	gracefulv1 "github.com/museop/graceful-shutdown/gen/graceful/v1"
	"google.golang.org/grpc"
)

const (
	defaultConnectionCheckInterval = time.Second
	defaultZeroConnectionsDuration = 5 * time.Second
)

// ShutdownOptions controls the DRAINING phase before the server enters
// STOPPING and asks gRPC to finish existing RPCs gracefully.
type ShutdownOptions struct {
	// MaxDrainDuration is the maximum time to stay in DRAINING. Once this
	// duration elapses, the server transitions to STOPPING even if connections
	// still exist. A non-positive value skips the DRAINING wait.
	MaxDrainDuration time.Duration
	// ConnectionCheckInterval controls how often active gRPC connections are
	// sampled while DRAINING. The production default is one second.
	ConnectionCheckInterval time.Duration
	// ZeroConnectionsDuration is the required continuous period with zero active
	// connections before STOPPING may begin. The production default is five
	// seconds.
	ZeroConnectionsDuration time.Duration
}

// GracefulShutdown implements the shutdown lifecycle:
//
//  1. publish DRAINING immediately so clients stop choosing this backend,
//  2. once per second, check the active gRPC connection count,
//  3. publish STOPPING after either five continuous seconds with zero
//     connections or maxDrainDuration has elapsed,
//  4. call grpc.Server.GracefulStop so new RPCs are rejected while active RPC
//     handlers finish.
//
// This function intentionally does not force Stop on timeout. If GracefulStop
// cannot complete, process supervision (for example systemd or the container
// runtime) owns killing the process.
func GracefulShutdown(ctx context.Context, grpcServer *grpc.Server, lifecycle *Lifecycle, maxDrainDuration time.Duration) error {
	return GracefulShutdownWithOptions(ctx, grpcServer, lifecycle, ShutdownOptions{
		MaxDrainDuration: maxDrainDuration,
	})
}

func GracefulShutdownWithOptions(ctx context.Context, grpcServer *grpc.Server, lifecycle *Lifecycle, options ShutdownOptions) error {
	options = normalizeShutdownOptions(options)

	lifecycle.SetStatus(gracefulv1.ServiceStatus_SERVICE_STATUS_DRAINING)
	drainErr := waitUntilStopping(ctx, lifecycle, options)

	lifecycle.SetStatus(gracefulv1.ServiceStatus_SERVICE_STATUS_STOPPING)
	grpcServer.GracefulStop()
	return drainErr
}

func normalizeShutdownOptions(options ShutdownOptions) ShutdownOptions {
	if options.ConnectionCheckInterval <= 0 {
		options.ConnectionCheckInterval = defaultConnectionCheckInterval
	}
	if options.ZeroConnectionsDuration <= 0 {
		options.ZeroConnectionsDuration = defaultZeroConnectionsDuration
	}
	return options
}

func waitUntilStopping(ctx context.Context, lifecycle *Lifecycle, options ShutdownOptions) error {
	if options.MaxDrainDuration <= 0 {
		return nil
	}

	var zeroSince time.Time
	if lifecycle.ActiveConnections() == 0 {
		zeroSince = time.Now()
	}
	ticker := time.NewTicker(options.ConnectionCheckInterval)
	defer ticker.Stop()
	maxDrain := time.NewTimer(options.MaxDrainDuration)
	defer maxDrain.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-maxDrain.C:
			return nil
		case now := <-ticker.C:
			if lifecycle.ActiveConnections() == 0 {
				if zeroSince.IsZero() {
					zeroSince = now
				}
				if now.Sub(zeroSince) >= options.ZeroConnectionsDuration {
					return nil
				}
				continue
			}
			zeroSince = time.Time{}
		}
	}
}
