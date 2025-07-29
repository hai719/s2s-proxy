# Stream Debug Endpoint

The s2s-proxy now provides a debug HTTP endpoint that shows active gRPC streams, particularly `StreamWorkflowReplicationMessages`.

## Usage

If your proxy configuration includes a profiling section:

```yaml
profiling:
  pprofAddress: "localhost:6060"
```

You can access the debug endpoint at:

```bash
curl http://localhost:6060/debug/connections
```

## Response Format

The endpoint returns JSON with the following structure:

```json
{
  "timestamp": "2024-01-15T10:30:00Z",
  "connections": [
    {
      "name": "mux-transport-1",
      "type": "mux",
      "status": "connected",
      "local_addr": "127.0.0.1:8080",
      "remote_addr": "127.0.0.1:9090",
      "connected": true,
      "start_time": "2024-01-15T10:25:00Z",
      "last_seen": "2024-01-15T10:29:58Z",
      "streams": 3,
      "active_streams": []
    }
  ],
  "active_streams": [
    {
      "id": "cluster-1-shard-0-cluster-2-shard-1-inbound-1642243800000000000",
      "method": "StreamWorkflowReplicationMessages",
      "direction": "inbound",
      "client_shard": "cluster-1-shard-0",
      "server_shard": "cluster-2-shard-1",
      "start_time": "2024-01-15T10:30:00Z",
      "last_seen": "2024-01-15T10:29:59Z"
    }
  ],
  "stream_count": 1
}
```

## Fields Description

### Connection Information
- `name`: Transport connection name
- `type`: Connection type (tcp, mux)
- `status`: Connection status (initialized, started, connected, connecting, stopped)
- `local_addr`/`remote_addr`: Connection endpoints
- `connected`: Boolean connection state
- `start_time`: When the connection was established
- `last_seen`: Last activity timestamp for the connection
- `streams`: Number of active yamux streams (for mux connections)

### Active Streams
- `id`: Unique stream identifier (format: `client-shard-server-shard-direction-timestamp`)
- `method`: gRPC method name (e.g., StreamWorkflowReplicationMessages)
- `direction`: Stream direction (inbound/outbound)
- `client_shard`/`server_shard`: Cluster shard identifiers
- `start_time`: When the stream was initiated
- `last_seen`: Last activity timestamp

## Stream Tracking

The proxy tracks stream activity by:
1. Registering streams when `StreamWorkflowReplicationMessages` starts
2. Updating `last_seen` timestamp on each message received/sent
3. Unregistering streams when they complete

This provides real-time visibility into active replication streams for debugging and monitoring purposes. 