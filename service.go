package gracefulshutdown

import (
	"context"
	"errors"
	"io"
	"time"

	gracefulv1 "github.com/museop/graceful-shutdown/gen/graceful/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrServerGracefulShutdown = errors.New("server is gracefully shutting down")

// WorkHandler processes one logical work request from the bidirectional Work
// stream. Returning bytes lets applications replace the demo echo handler with
// real business logic without changing lifecycle behavior.
type WorkHandler func(context.Context, *gracefulv1.WorkRequest) ([]byte, error)

type Service struct {
	gracefulv1.UnimplementedGracefulServiceServer
	lifecycle         *Lifecycle
	handleWork        WorkHandler
	heartbeatInterval time.Duration
}

func NewService(lifecycle *Lifecycle, handleWork WorkHandler) *Service {
	return NewServiceWithOptions(lifecycle, handleWork, ServiceOptions{})
}

type ServiceOptions struct {
	// StatusHeartbeatInterval controls how often WatchStatus repeats the
	// current state even when no lifecycle transition occurs. Repeating the
	// state prevents missed updates from leaving clients stale and keeps quiet
	// streams active through middleboxes.
	StatusHeartbeatInterval time.Duration
}

func NewServiceWithOptions(lifecycle *Lifecycle, handleWork WorkHandler, options ServiceOptions) *Service {
	if handleWork == nil {
		handleWork = EchoWorkHandler
	}
	if options.StatusHeartbeatInterval <= 0 {
		options.StatusHeartbeatInterval = time.Second
	}
	return &Service{lifecycle: lifecycle, handleWork: handleWork, heartbeatInterval: options.StatusHeartbeatInterval}
}

func EchoWorkHandler(ctx context.Context, req *gracefulv1.WorkRequest) ([]byte, error) {
	if req.GetDelayMillis() > 0 {
		timer := time.NewTimer(time.Duration(req.GetDelayMillis()) * time.Millisecond)
		defer timer.Stop()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return req.GetPayload(), nil
}

func (s *Service) WatchStatus(_ *gracefulv1.WatchStatusRequest, stream gracefulv1.GracefulService_WatchStatusServer) error {
	current, updates, unsubscribe := s.lifecycle.subscribe()
	defer unsubscribe()

	heartbeat := time.NewTicker(s.heartbeatInterval)
	defer heartbeat.Stop()

	for {
		if err := stream.Send(s.statusUpdate(current)); err != nil {
			return err
		}
		if current == gracefulv1.ServiceStatus_SERVICE_STATUS_STOPPING {
			return nil
		}

		select {
		case next := <-updates:
			current = next
		case <-heartbeat.C:
			current = s.lifecycle.Status()
		case <-stream.Context().Done():
			return nil
		}
	}
}

func (s *Service) Work(stream gracefulv1.GracefulService_WorkServer) error {
	for {
		if s.lifecycle.Status() == gracefulv1.ServiceStatus_SERVICE_STATUS_STOPPING {
			return status.Error(codes.Unavailable, ErrServerGracefulShutdown.Error())
		}

		req, err := s.recvWork(stream)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}

		observedStatus, done, ok := s.lifecycle.TryBeginRequest()
		if !ok {
			return status.Error(codes.Unavailable, ErrServerGracefulShutdown.Error())
		}

		payload, handleErr := s.handleWork(stream.Context(), req)
		done()
		if handleErr != nil {
			return handleErr
		}

		if err := stream.Send(&gracefulv1.WorkResponse{
			RequestId:      req.GetRequestId(),
			Payload:        payload,
			ServerId:       s.lifecycle.ServerID(),
			ObservedStatus: observedStatus,
		}); err != nil {
			return err
		}
	}
}

type workRecvResult struct {
	req *gracefulv1.WorkRequest
	err error
}

func (s *Service) recvWork(stream gracefulv1.GracefulService_WorkServer) (*gracefulv1.WorkRequest, error) {
	recv := make(chan workRecvResult, 1)
	go func() {
		req, err := stream.Recv()
		recv <- workRecvResult{req: req, err: err}
	}()

	select {
	case result := <-recv:
		return result.req, result.err
	case <-s.lifecycle.GracefulDone():
		return nil, status.Error(codes.Unavailable, ErrServerGracefulShutdown.Error())
	case <-stream.Context().Done():
		return nil, stream.Context().Err()
	}
}

func (s *Service) statusUpdate(status gracefulv1.ServiceStatus) *gracefulv1.StatusUpdate {
	return &gracefulv1.StatusUpdate{
		ServerId:   s.lifecycle.ServerID(),
		Status:     status,
		UnixMillis: time.Now().UnixMilli(),
	}
}
