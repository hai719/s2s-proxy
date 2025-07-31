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
  "stream_count": 1,
  "shard_info": {
    "enabled": true,
    "forwarding_enabled": true,
    "node_name": "proxy-node-1",
    "local_shards": [
      {
        "cluster_id": 1,
        "shard_id": 0
      },
      {
        "cluster_id": 1,
        "shard_id": 3
      }
    ],
    "local_shard_count": 2,
    "cluster_nodes": ["proxy-node-1", "proxy-node-2", "proxy-node-3"],
    "cluster_size": 3,
    "remote_shards": {
      "1:1": "proxy-node-2",
      "1:2": "proxy-node-2", 
      "1:4": "proxy-node-3",
      "1:5": "proxy-node-3"
    },
    "remote_shard_counts": {
      "proxy-node-1": 2,
      "proxy-node-2": 2,
      "proxy-node-3": 2
    }
  }
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

### Shard Information
- `enabled`: Whether memberlist shard management is enabled
- `forwarding_enabled`: Whether proxy-to-proxy forwarding is enabled
- `node_name`: This proxy instance's node name in the cluster  
- `local_shards`: Array of shards currently handled by this proxy
- `local_shard_count`: Number of shards handled locally
- `cluster_nodes`: All active proxy nodes in the cluster
- `cluster_size`: Total number of proxy nodes in the cluster
- `remote_shards`: Map of shard identifiers to the proxy node that owns them (format: "cluster_id:shard_id" -> "node_name")
- `remote_shard_counts`: Map of proxy node names to the number of shards each node is currently handling
- `start_time`: When the stream was initiated
- `last_seen`: Last activity timestamp

## Stream Tracking

The proxy tracks stream activity by:
1. Registering streams when `StreamWorkflowReplicationMessages` starts
2. Updating `last_seen` timestamp on each message received/sent
3. Unregistering streams when they complete

This provides real-time visibility into active replication streams for debugging and monitoring purposes.

## Distributed Shard Management

When memberlist is enabled, the proxy provides distributed shard ownership tracking:

- **Local Shards**: Shards currently handled by this proxy instance
- **Remote Shards**: Real-time view of which proxy nodes own specific shards across the cluster
- **Shard Counts**: Total number of shards handled by each proxy node in the cluster

This information is useful for:
- Understanding shard distribution across the proxy cluster
- Debugging proxy-to-proxy forwarding issues
- Monitoring cluster health and load balancing