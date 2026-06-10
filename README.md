# graceful-shutdown

Go gRPC graceful shutdown example for a client that continuously watches multiple backend service states and sends work only to backends that are still `SERVING`.

## Scenario

A client keeps streaming service-state updates from servers A, B, and C while concurrently sending work RPCs to the backends. When server A receives `SIGINT` or `SIGTERM`, the desired behavior is:

1. A immediately publishes `DRAINING` on the status stream.
2. Clients stop selecting A for new work after receiving `DRAINING`.
3. A still handles work that arrives during the drain window.
4. After `N` seconds, A publishes `GRACEFUL_SHUTDOWN`.
5. A stops accepting new RPCs but lets already-processing requests complete.
6. A exits after in-flight work is done, while client work continues on B and C.

## RPC model

The protobuf contract is in [`proto/graceful/v1/graceful.proto`](proto/graceful/v1/graceful.proto).

Service states:

| State | Meaning |
| --- | --- |
| `SERVICE_STATUS_SERVING` | Healthy backend. Clients may route new work here. |
| `SERVICE_STATUS_DRAINING` | Backend is being removed. Clients should not choose it for new work, but the server still accepts requests that arrive during the drain window. |
| `SERVICE_STATUS_GRACEFUL_SHUTDOWN` | Backend no longer accepts new work. Existing in-flight work may complete. |

RPCs:

- `WatchStatus(WatchStatusRequest) returns (stream StatusUpdate)`
  - Sends the current status immediately and then every state transition.
- `Work(stream WorkRequest) returns (stream WorkResponse)`
  - Demo bidirectional stream RPC for work.
  - Each accepted work item reports the server ID and status observed at admission time.

## Implementation overview

- [`lifecycle.go`](lifecycle.go)
  - Owns the server status, broadcasts status updates, and tracks active work items.
- [`service.go`](service.go)
  - Implements `WatchStatus` and `Work`.
  - Rejects work after `GRACEFUL_SHUTDOWN`.
  - Interrupts idle work streams when graceful shutdown begins so shutdown cannot hang forever on a stream that is open but not sending messages.
- [`shutdown.go`](shutdown.go)
  - Implements the shutdown sequence:
    1. set `DRAINING`,
    2. wait `drainDelay`,
    3. set `GRACEFUL_SHUTDOWN`,
    4. call `grpc.Server.GracefulStop`,
    5. force `Stop` only if the shutdown context expires.
- [`cmd/server`](cmd/server)
  - Runs one gRPC backend and handles `SIGINT`/`SIGTERM`.
- [`cmd/client`](cmd/client)
  - Watches all configured backends and randomly sends work only to `SERVING` backends.

## Requirements

- Go 1.26.4 or compatible with this module's `go.mod`.
- Optional, only if regenerating protobuf files:
  - `protoc`
  - `protoc-gen-go`
  - `protoc-gen-go-grpc`

## Run tests

```bash
go test ./...
go vet ./...
go test -race ./...
```

## Manual demo

Start three servers:

```bash
go run ./cmd/server -addr :50051 -server-id A -drain-delay 5s
go run ./cmd/server -addr :50052 -server-id B -drain-delay 5s
go run ./cmd/server -addr :50053 -server-id C -drain-delay 5s
```

Start a client:

```bash
go run ./cmd/client \
  -servers localhost:50051,localhost:50052,localhost:50053 \
  -concurrency 8 \
  -interval 200ms
```

Then press `Ctrl-C` in server A's terminal. The client should log A changing to `DRAINING` and then continue handling work on B and C.

## Automated demo script

Run:

```bash
./scripts/demo-graceful-shutdown.sh
```

The script:

1. builds temporary server/client binaries,
2. starts servers A, B, and C on free local ports,
3. starts the load-generating client,
4. sends `SIGINT` to server A,
5. checks that A publishes `DRAINING` and `GRACEFUL_SHUTDOWN`,
6. checks that no client work is routed to A after the client observes A as `DRAINING`.

Useful environment variables:

```bash
DRAIN_DELAY=2s CLIENT_DURATION=9s CONCURRENCY=6 INTERVAL=100ms ./scripts/demo-graceful-shutdown.sh
```

The script prints the temporary log directory. Set `KEEP_LOGS=1` to keep logs after success:

```bash
KEEP_LOGS=1 ./scripts/demo-graceful-shutdown.sh
```

## Regenerate protobuf code

```bash
protoc \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  proto/graceful/v1/graceful.proto

mv proto/graceful/v1/graceful.pb.go gen/graceful/v1/graceful.pb.go
mv proto/graceful/v1/graceful_grpc.pb.go gen/graceful/v1/graceful_grpc.pb.go
```

## Notes

This is a local demonstration of the shutdown protocol. Production deployments should usually add service discovery, health checks, metrics, TLS, retries/backoff, and load-balancer integration.
