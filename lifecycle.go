package gracefulshutdown

import (
	"context"
	"sync"
	"time"

	gracefulv1 "github.com/museop/graceful-shutdown/gen/graceful/v1"
)

// Lifecycle owns the server state that is published to status-stream clients and
// consulted before starting each unit of work.
type Lifecycle struct {
	serverID string

	mu             sync.Mutex
	status         gracefulv1.ServiceStatus
	subscribers    map[chan gracefulv1.ServiceStatus]struct{}
	activeRequests int
	gracefulDone   chan struct{}
}

func NewLifecycle(serverID string) *Lifecycle {
	return &Lifecycle{
		serverID:     serverID,
		status:       gracefulv1.ServiceStatus_SERVICE_STATUS_SERVING,
		subscribers:  make(map[chan gracefulv1.ServiceStatus]struct{}),
		gracefulDone: make(chan struct{}),
	}
}

func (l *Lifecycle) ServerID() string {
	return l.serverID
}

func (l *Lifecycle) Status() gracefulv1.ServiceStatus {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.status
}

// SetStatus updates the lifecycle and broadcasts the transition to active
// status watchers. Repeating the current state is a no-op.
func (l *Lifecycle) SetStatus(next gracefulv1.ServiceStatus) {
	l.mu.Lock()
	if l.status == next {
		l.mu.Unlock()
		return
	}
	l.status = next
	if next == gracefulv1.ServiceStatus_SERVICE_STATUS_GRACEFUL_SHUTDOWN {
		select {
		case <-l.gracefulDone:
		default:
			close(l.gracefulDone)
		}
	}
	subscribers := make([]chan gracefulv1.ServiceStatus, 0, len(l.subscribers))
	for subscriber := range l.subscribers {
		subscribers = append(subscribers, subscriber)
	}
	l.mu.Unlock()

	for _, subscriber := range subscribers {
		select {
		case subscriber <- next:
		default:
			// Keep the newest state if a slow watcher fills its small buffer.
			select {
			case <-subscriber:
			default:
			}
			select {
			case subscriber <- next:
			default:
			}
		}
	}
}

func (l *Lifecycle) GracefulDone() <-chan struct{} {
	return l.gracefulDone
}

func (l *Lifecycle) subscribe() (gracefulv1.ServiceStatus, <-chan gracefulv1.ServiceStatus, func()) {
	updates := make(chan gracefulv1.ServiceStatus, 8)

	l.mu.Lock()
	current := l.status
	l.subscribers[updates] = struct{}{}
	l.mu.Unlock()

	unsubscribe := func() {
		l.mu.Lock()
		delete(l.subscribers, updates)
		l.mu.Unlock()
	}

	return current, updates, unsubscribe
}

// TryBeginRequest reserves capacity for one work item unless the server has
// already reached GRACEFUL_SHUTDOWN. The returned status is the state observed at
// admission time and should be included in the response for traceability.
func (l *Lifecycle) TryBeginRequest() (gracefulv1.ServiceStatus, func(), bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.status == gracefulv1.ServiceStatus_SERVICE_STATUS_GRACEFUL_SHUTDOWN {
		return l.status, nil, false
	}

	l.activeRequests++
	observed := l.status
	done := func() {
		l.mu.Lock()
		l.activeRequests--
		l.mu.Unlock()
	}
	return observed, done, true
}

func (l *Lifecycle) ActiveRequests() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.activeRequests
}

// WaitForNoActiveRequests is useful for tests and for non-gRPC embedders that
// want the same message-level completion guarantee as grpc.Server.GracefulStop.
func (l *Lifecycle) WaitForNoActiveRequests(ctx context.Context) error {
	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()

	for {
		l.mu.Lock()
		active := l.activeRequests
		l.mu.Unlock()
		if active == 0 {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-poll.C:
		}
	}
}
