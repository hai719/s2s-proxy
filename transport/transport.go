package transport

import (
	"fmt"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"

	"github.com/temporalio/s2s-proxy/config"
)

type (
	ClientTransport interface {
		Connect(clientMetrics *grpcprom.ClientMetrics) (*grpc.ClientConn, error)
	}

	ServerTransport interface {
		Serve(server *grpc.Server) error
	}

	Closable interface {
		CloseChan() <-chan struct{}
		IsClosed() bool
	}

	MuxTransport interface {
		ClientTransport
		ServerTransport
		Closable
	}

	// StreamInfo represents information about an active gRPC stream
	StreamInfo struct {
		ID          string    `json:"id"`
		Method      string    `json:"method"`
		Direction   string    `json:"direction"`
		ClientShard string    `json:"client_shard"`
		ServerShard string    `json:"server_shard"`
		StartTime   time.Time `json:"start_time"`
		LastSeen    time.Time `json:"last_seen"`
	}

	// ConnectionInfo represents debug information about a connection
	ConnectionInfo struct {
		Name          string       `json:"name"`
		Type          string       `json:"type"`
		Status        string       `json:"status"`
		LocalAddr     string       `json:"local_addr,omitempty"`
		RemoteAddr    string       `json:"remote_addr,omitempty"`
		Connected     bool         `json:"connected"`
		StartTime     time.Time    `json:"start_time,omitempty"`
		LastSeen      time.Time    `json:"last_seen,omitempty"`
		Streams       int          `json:"streams,omitempty"`
		ActiveStreams []StreamInfo `json:"active_streams,omitempty"`
	}

	TransportManager struct {
		muxConnManagers map[string]*muxConnectMananger
		logger          log.Logger
	}
)

func NewTransportManager(
	configProvider config.ConfigProvider,
	logger log.Logger,
) *TransportManager {

	muxConnManagers := make(map[string]*muxConnectMananger)
	s2sConfig := configProvider.GetS2SProxyConfig()
	for _, cfg := range s2sConfig.MuxTransports {
		muxConnManagers[cfg.Name] = newMuxConnectManager(cfg, logger)
	}

	return &TransportManager{
		muxConnManagers: muxConnManagers,
		logger:          logger,
	}
}

func (tm *TransportManager) openMuxTransport(transportName string) (MuxTransport, error) {
	mux := tm.muxConnManagers[transportName]
	if mux == nil {
		return nil, fmt.Errorf("multiplexed transport %s is not found", transportName)
	}

	return mux.open()
}
func (tm *TransportManager) IsMuxActive(name string) bool {
	return tm.muxConnManagers[name].status.Load() == int32(statusStarted)
}

func (tm *TransportManager) OpenClient(clientConfig config.ProxyClientConfig) (ClientTransport, error) {
	if clientConfig.Type == config.MuxTransport {
		return tm.openMuxTransport(clientConfig.MuxTransportName)
	}

	return &tcpClient{
		config: clientConfig.TCPClientSetting,
	}, nil
}

func (tm *TransportManager) OpenServer(serverConfig config.ProxyServerConfig) (ServerTransport, error) {
	if serverConfig.Type == config.MuxTransport {
		return tm.openMuxTransport(serverConfig.MuxTransportName)
	}

	return &tcpServer{
		config: serverConfig.TCPServerSetting,
	}, nil
}

func (tm *TransportManager) Start() error {
	tm.logger.Info("Starting TransportManager")
	defer tm.logger.Info("TransportManager started")
	for _, cm := range tm.muxConnManagers {
		if err := cm.start(); err != nil {
			return err
		}
	}

	return nil
}

func (tm *TransportManager) Stop() {
	tm.logger.Info("Stopping TransportManager")
	defer tm.logger.Info("TransportManager stopped")
	for _, cm := range tm.muxConnManagers {
		cm.stop()
	}
}

// GetConnectionInfo returns debug information about all active connections
func (tm *TransportManager) GetConnectionInfo() []ConnectionInfo {
	var connections []ConnectionInfo

	for name, manager := range tm.muxConnManagers {
		info := manager.getConnectionInfo(name)
		connections = append(connections, info...)
	}

	return connections
}
