package proxy

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-middleware/providers/prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/temporalio/s2s-proxy/auth"
	"github.com/temporalio/s2s-proxy/client"
	adminclient "github.com/temporalio/s2s-proxy/client/admin"
	"github.com/temporalio/s2s-proxy/common"
	"github.com/temporalio/s2s-proxy/config"
	"github.com/temporalio/s2s-proxy/encryption"
	"github.com/temporalio/s2s-proxy/interceptor"
	"github.com/temporalio/s2s-proxy/metrics"
	"github.com/temporalio/s2s-proxy/transport"
)

type (
	// ChannelDebugInfo holds debug information about channels
	ChannelDebugInfo struct {
		RemoteSendChannels map[string]int `json:"remote_send_channels"` // shard ID -> buffer size
		LocalAckChannels   map[string]int `json:"local_ack_channels"`   // shard ID -> buffer size
		TotalSendChannels  int            `json:"total_send_channels"`
		TotalAckChannels   int            `json:"total_ack_channels"`
	}

	ProxyServer struct {
		proxy        *Proxy
		config       config.ProxyConfig
		opts         proxyOptions
		logger       log.Logger
		server       *TemporalAPIServer
		adminClient  adminservice.AdminServiceClient
		transManager *transport.TransportManager
		metricLabels prometheus.Labels
		shardManager ShardManager
		shutDownCh   chan struct{}
	}

	// RoutedAck wraps an ACK with the target shard it originated from
	RoutedAck struct {
		TargetShard history.ClusterShardID
		Req         *adminservice.StreamWorkflowReplicationMessagesRequest
	}

	// RoutedMessage wraps a replication response with originating client shard info
	RoutedMessage struct {
		SourceShard history.ClusterShardID
		Resp        *adminservice.StreamWorkflowReplicationMessagesResponse
	}

	Proxy struct {
		config                    config.S2SProxyConfig
		transManager              *transport.TransportManager
		outboundServer            *ProxyServer
		inboundServer             *ProxyServer
		healthCheckServer         *http.Server
		outboundHealthCheckServer *http.Server
		metricsServer             *http.Server
		shardManager              ShardManager
		logger                    log.Logger
		intraMgr                  *intraProxyManager

		// remoteSendChannels maps shard IDs to send channels for replication message routing
		remoteSendChannels   map[history.ClusterShardID]chan RoutedMessage
		remoteSendChannelsMu sync.RWMutex

		// localAckChannels maps shard IDs to ack channels for local acknowledgment handling
		localAckChannels   map[history.ClusterShardID]chan RoutedAck
		localAckChannelsMu sync.RWMutex

		// localReceiverCancelFuncs maps shard IDs to context cancel functions for local receiver termination
		localReceiverCancelFuncs   map[history.ClusterShardID]context.CancelFunc
		localReceiverCancelFuncsMu sync.RWMutex
	}

	proxyOptions struct {
		IsInbound bool
		Config    config.S2SProxyConfig
	}
)

func makeServerOptions(
	logger log.Logger,
	cfg config.ProxyConfig,
	proxyOpts proxyOptions,
) ([]grpc.ServerOption, error) {
	unaryInterceptors := []grpc.UnaryServerInterceptor{}
	streamInterceptors := []grpc.StreamServerInterceptor{}

	labelGenerator := grpcprom.WithLabelsFromContext(func(_ context.Context) (labels prometheus.Labels) {
		return prometheus.Labels{"direction": proxyOpts.directionLabel()}
	})

	// Ordering matters! These metrics happen BEFORE the translations/acl
	unaryInterceptors = append(unaryInterceptors, metrics.GRPCServerMetrics.UnaryServerInterceptor(labelGenerator))
	streamInterceptors = append(streamInterceptors, metrics.GRPCServerMetrics.StreamServerInterceptor(labelGenerator))

	var translators []interceptor.Translator
	if tln := proxyOpts.Config.NamespaceNameTranslation; tln.IsEnabled() {
		// NamespaceNameTranslator needs to be called before namespace access control so that
		// local name can be used in namespace allowed list.
		reqMap, respMap := tln.ToMaps(proxyOpts.IsInbound)
		translators = append(translators, interceptor.NewNamespaceNameTranslator(logger, reqMap, respMap))
	}

	if tln := proxyOpts.Config.SearchAttributeTranslation; tln.IsEnabled() {
		logger.Info("search attribute translation enabled", tag.NewAnyTag("mappings", tln.NamespaceMappings))
		if len(tln.NamespaceMappings) > 1 {
			panic("multiple namespace search attribute mappings are not supported")
		}
		reqMap, respMap := tln.ToMaps(proxyOpts.IsInbound)
		translators = append(translators, interceptor.NewSearchAttributeTranslator(logger, reqMap, respMap))
	}

	if len(translators) > 0 {
		tr := interceptor.NewTranslationInterceptor(logger, translators)
		unaryInterceptors = append(unaryInterceptors, tr.Intercept)
		streamInterceptors = append(streamInterceptors, tr.InterceptStream)
	}

	if proxyOpts.IsInbound && cfg.ACLPolicy != nil {
		aclInterceptor := interceptor.NewAccessControlInterceptor(logger, cfg.ACLPolicy)
		unaryInterceptors = append(unaryInterceptors, aclInterceptor.Intercept)
		streamInterceptors = append(streamInterceptors, aclInterceptor.StreamIntercept)
	}

	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		grpc.ChainStreamInterceptor(streamInterceptors...),
	}

	if cfg.Server.TLS.IsEnabled() {
		tlsConfig, err := encryption.GetServerTLSConfig(cfg.Server.TLS, logger)
		if err != nil {
			return opts, err
		}
		opts = append(opts, grpc.Creds(credentials.NewTLS(tlsConfig)))
	}

	return opts, nil
}

func (ps *ProxyServer) makeNamespaceACL() *auth.AccessControl {
	if ps.opts.IsInbound && ps.config.ACLPolicy != nil {
		return auth.NewAccesControl(ps.config.ACLPolicy.AllowedNamespaces)
	}
	return nil
}

func (ps *ProxyServer) startServer(
	serverTransport transport.ServerTransport,
	clientTransport transport.ClientTransport,
) error {
	cfg := ps.config
	opts := ps.opts
	logger := ps.logger

	serverOpts, err := makeServerOptions(logger, cfg, opts)
	if err != nil {
		return err
	}

	clientMetrics := metrics.GRPCOutboundClientMetrics
	if ps.opts.IsInbound {
		clientMetrics = metrics.GRPCInboundClientMetrics
	}

	clientFactory := client.NewClientFactory(clientTransport, clientMetrics, logger)
	clientProvider := client.NewClientProvider(cfg.Client, clientFactory, logger)
	ps.adminClient = adminclient.NewLazyClient(clientProvider)
	ps.server = NewTemporalAPIServer(
		cfg.Name,
		cfg.Server,
		NewAdminServiceProxyServer(ps, cfg.Name, cfg.Client, clientFactory, ps.shardManager, opts, ps.adminClient, logger),
		NewWorkflowServiceProxyServer(ps, cfg.Name, cfg.Client, clientFactory, ps.makeNamespaceACL(), logger),
		serverOpts,
		serverTransport,
		logger,
	)

	ps.logger.Info(fmt.Sprintf("Starting ProxyServer %s with ServerConfig: %v, ClientConfig: %v", cfg.Name, cfg.Server, cfg.Client))
	ps.server.Start()
	return nil
}

func (ps *ProxyServer) stopServer() {
	if ps.server != nil {
		ps.server.Stop()
	}
}

func (ps *ProxyServer) GetAdminClient() adminservice.AdminServiceClient {
	return ps.adminClient
}

func monitorClosable(closable transport.Closable, retryCh chan struct{}, shutDownCh <-chan struct{}) {
	select {
	case <-shutDownCh:
		return
	// Stop monitor if retryCh is already closed
	case <-retryCh:
		return
	case <-closable.CloseChan():
		// TODO: avoid retryCh to be closed twice.
		close(retryCh)
	}
}

func (opts *proxyOptions) directionLabel() string {
	directionValue := "outbound"
	if opts.IsInbound {
		directionValue = "inbound"
	}
	return directionValue
}

func (ps *ProxyServer) start() error {
	serverConfig := ps.config.Server
	clientConfig := ps.config.Client

	go func() {
		ps.logger.Info("Starting ProxyServer")
		defer ps.logger.Info("ProxyServer started")
		for {
			metrics.ProxyServiceCreated.With(ps.metricLabels).Inc()
			// If using mux transport underneath, Open call will be blocked until
			// underlying connection is established.
			// Also note: GRPC requires the client interceptors (like metrics) to be defined on the transport, not on the client.
			clientTransport, err := ps.transManager.OpenClient(clientConfig)
			if err != nil {
				ps.logger.Error("Open client transport is failed", tag.Error(err))
				ps.stopServer()
				<-time.After(100 * time.Millisecond)
				continue
			}

			serverTransport, err := ps.transManager.OpenServer(serverConfig)
			if err != nil {
				ps.logger.Error("Open server transport is failed", tag.Error(err))
				ps.stopServer()
				<-time.After(100 * time.Millisecond)
				continue
			}

			if err := ps.startServer(serverTransport, clientTransport); err != nil {
				ps.logger.Error("Failed to start server", tag.Error(err))
				ps.stopServer()
				<-time.After(100 * time.Millisecond)
				continue
			}

			retryCh := make(chan struct{})
			if closable, ok := clientTransport.(transport.Closable); ok {
				go monitorClosable(closable, retryCh, ps.shutDownCh)
			}

			if closable, ok := serverTransport.(transport.Closable); ok {
				go monitorClosable(closable, retryCh, ps.shutDownCh)
			}

			select {
			case <-ps.shutDownCh:
				metrics.ProxyServiceStopped.With(ps.metricLabels).Inc()
				ps.stopServer()
				return
			case <-retryCh:
				// If any closable transport is closed, try to restart the proxy server.
				metrics.ProxyServiceRestarted.With(ps.metricLabels).Inc()
				ps.stopServer()
			}
		}
	}()

	return nil
}

func (ps *ProxyServer) stop() {
	ps.logger.Info("Stop ProxyServer")
	close(ps.shutDownCh)
}

func newProxyServer(
	proxy *Proxy,
	cfg config.ProxyConfig,
	opts proxyOptions,
	transManager *transport.TransportManager,
	shardManager ShardManager,
	logger log.Logger,
) *ProxyServer {
	return &ProxyServer{
		proxy:        proxy,
		config:       cfg,
		opts:         opts,
		transManager: transManager,
		shardManager: shardManager,
		logger:       logger,
		metricLabels: prometheus.Labels{"direction": opts.directionLabel()},
		shutDownCh:   make(chan struct{}),
	}
}

func NewProxy(
	configProvider config.ConfigProvider,
	transManager *transport.TransportManager,
	shardManager ShardManager,
	logger log.Logger,
) *Proxy {
	s2sConfig := configProvider.GetS2SProxyConfig()
	proxy := &Proxy{
		config:       s2sConfig,
		transManager: transManager,
		shardManager: shardManager,
		logger: log.NewThrottledLogger(
			logger,
			func() float64 {
				return s2sConfig.Logging.GetThrottleMaxRPS()
			},
		),
		remoteSendChannels:       make(map[history.ClusterShardID]chan RoutedMessage),
		localAckChannels:         make(map[history.ClusterShardID]chan RoutedAck),
		localReceiverCancelFuncs: make(map[history.ClusterShardID]context.CancelFunc),
	}

	if s2sConfig.ShardCountConfig.Mode == config.ShardCountRouting {
		// Initialize intra-proxy manager for peer communication
		proxy.intraMgr = newIntraProxyManager(logger, proxy)

		// Wire memberlist peer-join callback to reconcile intra-proxy receivers for local/remote pairs
		shardManager.SetOnPeerJoin(func(nodeName string) {
			logger.Info("OnPeerJoin", tag.NewStringTag("nodeName", nodeName))
			defer logger.Info("OnPeerJoin done", tag.NewStringTag("nodeName", nodeName))
			proxy.intraMgr.Notify()
			// proxy.intraMgr.ReconcilePeerStreams(proxy, nodeName)
		})

		// Wire peer-leave to cleanup intra-proxy resources for that peer
		shardManager.SetOnPeerLeave(func(nodeName string) {
			logger.Info("OnPeerLeave", tag.NewStringTag("nodeName", nodeName))
			defer logger.Info("OnPeerLeave done", tag.NewStringTag("nodeName", nodeName))
			proxy.intraMgr.Notify()
			// proxy.intraMgr.ReconcilePeerStreams(proxy, nodeName)
		})

		// Wire local shard changes to reconcile intra-proxy receivers
		shardManager.SetOnLocalShardChange(func(shard history.ClusterShardID, added bool) {
			logger.Info("OnLocalShardChange", tag.NewStringTag("shard", ClusterShardIDtoString(shard)), tag.NewStringTag("added", strconv.FormatBool(added)))
			defer logger.Info("OnLocalShardChange done", tag.NewStringTag("shard", ClusterShardIDtoString(shard)), tag.NewStringTag("added", strconv.FormatBool(added)))
			proxy.intraMgr.Notify()
			// proxy.intraMgr.ReconcilePeerStreams(proxy, "")
		})

		// Wire remote shard changes to reconcile intra-proxy receivers
		shardManager.SetOnRemoteShardChange(func(peer string, shard history.ClusterShardID, added bool) {
			logger.Info("OnRemoteShardChange", tag.NewStringTag("peer", peer), tag.NewStringTag("shard", ClusterShardIDtoString(shard)), tag.NewStringTag("added", strconv.FormatBool(added)))
			defer logger.Info("OnRemoteShardChange done", tag.NewStringTag("peer", peer), tag.NewStringTag("shard", ClusterShardIDtoString(shard)), tag.NewStringTag("added", strconv.FormatBool(added)))
			proxy.intraMgr.Notify()
			// proxy.intraMgr.ReconcilePeerStreams(proxy, peer)
		})

	}

	// Proxy consists of two grpc servers: inbound and outbound. The flow looks like the following:
	//    local server -> proxy(outbound) -> remote server
	//    local server <- proxy(inbound) <- remote server
	//
	// Here a remote server can be another proxy as well.
	//    server-a <-> proxy-a <-> proxy-b <-> server-b
	if s2sConfig.Outbound != nil {
		proxy.outboundServer = newProxyServer(
			proxy,
			*s2sConfig.Outbound,
			proxyOptions{
				IsInbound: false,
				Config:    s2sConfig,
			},
			transManager,
			shardManager,
			proxy.logger,
		)
	}

	if s2sConfig.Inbound != nil {
		proxy.inboundServer = newProxyServer(
			proxy,
			*s2sConfig.Inbound,
			proxyOptions{
				IsInbound: true,
				Config:    s2sConfig,
			},
			transManager,
			shardManager,
			proxy.logger,
		)
	}

	metrics.ProxyStartCount.Inc()

	return proxy
}

// startHealthCheckHandler has some fancier arguments: healthChecker is the health check to register. storeReference will
// receive the http.Server and put it somewhere so we can shut it down later.
func (s *Proxy) startHealthCheckHandler(healthChecker HealthChecker, storeReference func(*http.Server), cfg config.HealthCheckConfig) error {
	if cfg.Protocol != config.HTTP {
		return fmt.Errorf("unsupported health check protocol %s", cfg.Protocol)
	}

	// Set up the handler. Avoid the global ServeMux so that we can create N of these in unit test suites
	mux := http.NewServeMux()
	mux.HandleFunc("/health", healthChecker.createHandler())
	// Define the server and its settings
	healthCheckServer := &http.Server{
		Addr:    cfg.ListenAddress,
		Handler: mux,
	}
	storeReference(healthCheckServer)

	go func() {
		s.logger.Info("Starting health check server", tag.Address(cfg.ListenAddress))
		if err := s.healthCheckServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("Error starting server", tag.Error(err))
		}
	}()

	return nil
}

func (s *Proxy) startMetricsHandler(cfg config.MetricsConfig) error {
	// Why not use the global ServeMux? So that it can be used in unit tests
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.NewMetricsHandler(s.logger))
	s.metricsServer = &http.Server{
		Addr:    cfg.Prometheus.ListenAddress,
		Handler: mux,
	}

	go func() {
		s.logger.Info("Starting metrics server", tag.Address(cfg.Prometheus.ListenAddress))
		if err := s.metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("Error starting server", tag.Error(err))
		}
	}()
	return nil
}

func (s *Proxy) Start() error {
	s.logger.Info("Starting Proxy")
	if s.config.HealthCheck != nil {
		setHealthFn := func(server *http.Server) { s.healthCheckServer = server }
		if err := s.startHealthCheckHandler(newInboundHealthCheck(s.logger), setHealthFn, *s.config.HealthCheck); err != nil {
			return err
		}
	} else {
		s.logger.Warn("Started up without inbound health check! Double-check the YAML config," +
			" it needs at least the following path: healthCheck.listenAddress")
	}

	if s.config.OutboundHealthCheck != nil {
		healthFn := func() bool {
			// s.config.Outbound.Server.MuxTransportName: There's one mux per outbound server right now.
			return s.transManager.IsMuxActive(s.config.Outbound.Server.MuxTransportName)
		}
		setHealthFn := func(server *http.Server) { s.outboundHealthCheckServer = server }
		if err := s.startHealthCheckHandler(newOutboundHealthCheck(healthFn, s.logger), setHealthFn, *s.config.OutboundHealthCheck); err != nil {
			return err
		}
	} else {
		s.logger.Warn("Started up without outbound health check! Double-check the YAML config," +
			" it needs at least the following path: outboundHealthCheck.listenAddress")
	}

	if s.config.Metrics != nil {
		if err := s.startMetricsHandler(*s.config.Metrics); err != nil {
			return err
		}
	} else {
		s.logger.Warn(`Started up without metrics! Double-check the YAML config,` +
			` it needs at least the following path: metrics.prometheus.listenAddress`)
	}

	if err := s.shardManager.Start(); err != nil {
		return err
	}

	if s.intraMgr != nil {
		if err := s.intraMgr.Start(); err != nil {
			return err
		}
	}

	if err := s.transManager.Start(); err != nil {
		return err
	}

	if s.outboundServer != nil {
		if err := s.outboundServer.start(); err != nil {
			return err
		}
	}

	if s.inboundServer != nil {
		if err := s.inboundServer.start(); err != nil {
			return err
		}
	}

	s.logger.Info("Proxy started")
	return nil
}

func (s *Proxy) Stop() {
	s.logger.Info("Stopping Proxy")
	if s.healthCheckServer != nil {
		// Close without waiting for in-flight requests to complete.
		_ = s.healthCheckServer.Close()
	}

	if s.metricsServer != nil {
		_ = s.metricsServer.Close()
	}

	if s.inboundServer != nil {
		s.inboundServer.stop()
	}
	if s.outboundServer != nil {
		s.outboundServer.stop()
	}
	s.transManager.Stop()

	// Stop shard manager
	s.shardManager.Stop()
	s.logger.Info("Proxy stopped")
}

// GetConnectionInfo returns debug information about active connections
func (s *Proxy) GetConnectionInfo() []common.ConnectionInfo {
	return s.transManager.GetConnectionInfo()
}

// GetShardInfo returns debug information about shard distribution
func (s *Proxy) GetShardInfo() ShardDebugInfo {
	return s.shardManager.GetShardInfo()
}

// GetChannelInfo returns debug information about active channels
func (s *Proxy) GetChannelInfo() ChannelDebugInfo {
	remoteSendChannels := make(map[string]int)
	var totalSendChannels int

	// Collect remote send channel info first
	s.remoteSendChannelsMu.RLock()
	for shardID, ch := range s.remoteSendChannels {
		shardKey := ClusterShardIDtoString(shardID)
		remoteSendChannels[shardKey] = len(ch)
	}
	totalSendChannels = len(s.remoteSendChannels)
	s.remoteSendChannelsMu.RUnlock()

	localAckChannels := make(map[string]int)
	var totalAckChannels int

	// Collect local ack channel info separately
	s.localAckChannelsMu.RLock()
	for shardID, ch := range s.localAckChannels {
		shardKey := ClusterShardIDtoString(shardID)
		localAckChannels[shardKey] = len(ch)
	}
	totalAckChannels = len(s.localAckChannels)
	s.localAckChannelsMu.RUnlock()

	return ChannelDebugInfo{
		RemoteSendChannels: remoteSendChannels,
		LocalAckChannels:   localAckChannels,
		TotalSendChannels:  totalSendChannels,
		TotalAckChannels:   totalAckChannels,
	}
}

// GetIntraProxyManager returns the intra-proxy manager instance
func (s *Proxy) GetIntraProxyManager() *intraProxyManager {
	return s.intraMgr
}

// SetRemoteSendChan registers a send channel for a specific shard ID
func (s *Proxy) SetRemoteSendChan(shardID history.ClusterShardID, sendChan chan RoutedMessage) {
	s.logger.Info("Register remote send channel for shard", tag.NewStringTag("shardID", ClusterShardIDtoString(shardID)))
	s.remoteSendChannelsMu.Lock()
	defer s.remoteSendChannelsMu.Unlock()
	s.remoteSendChannels[shardID] = sendChan
}

// GetRemoteSendChan retrieves the send channel for a specific shard ID
func (s *Proxy) GetRemoteSendChan(shardID history.ClusterShardID) (chan RoutedMessage, bool) {
	s.remoteSendChannelsMu.RLock()
	defer s.remoteSendChannelsMu.RUnlock()
	ch, exists := s.remoteSendChannels[shardID]
	return ch, exists
}

// GetAllRemoteSendChans returns a map of all remote send channels
func (s *Proxy) GetAllRemoteSendChans() map[history.ClusterShardID]chan RoutedMessage {
	s.remoteSendChannelsMu.RLock()
	defer s.remoteSendChannelsMu.RUnlock()

	// Create a copy of the map
	result := make(map[history.ClusterShardID]chan RoutedMessage, len(s.remoteSendChannels))
	for k, v := range s.remoteSendChannels {
		result[k] = v
	}
	return result
}

// GetRemoteSendChansByCluster returns a copy of remote send channels filtered by clusterID
func (s *Proxy) GetRemoteSendChansByCluster(clusterID int32) map[history.ClusterShardID]chan RoutedMessage {
	s.remoteSendChannelsMu.RLock()
	defer s.remoteSendChannelsMu.RUnlock()

	result := make(map[history.ClusterShardID]chan RoutedMessage)
	for k, v := range s.remoteSendChannels {
		if k.ClusterID == clusterID {
			result[k] = v
		}
	}
	return result
}

// RemoveRemoteSendChan removes the send channel for a specific shard ID only if it matches the provided channel
func (s *Proxy) RemoveRemoteSendChan(shardID history.ClusterShardID, expectedChan chan RoutedMessage) {
	s.remoteSendChannelsMu.Lock()
	defer s.remoteSendChannelsMu.Unlock()
	if currentChan, exists := s.remoteSendChannels[shardID]; exists && currentChan == expectedChan {
		delete(s.remoteSendChannels, shardID)
		s.logger.Info("Removed remote send channel for shard", tag.NewStringTag("shardID", ClusterShardIDtoString(shardID)))
	} else {
		s.logger.Info("Skipped removing remote send channel for shard (channel mismatch or already removed)", tag.NewStringTag("shardID", ClusterShardIDtoString(shardID)))
	}
}

// SetLocalAckChan registers an ack channel for a specific shard ID
func (s *Proxy) SetLocalAckChan(shardID history.ClusterShardID, ackChan chan RoutedAck) {
	s.logger.Info("Register local ack channel for shard", tag.NewStringTag("shardID", ClusterShardIDtoString(shardID)))
	s.localAckChannelsMu.Lock()
	defer s.localAckChannelsMu.Unlock()
	s.localAckChannels[shardID] = ackChan
}

// GetLocalAckChan retrieves the ack channel for a specific shard ID
func (s *Proxy) GetLocalAckChan(shardID history.ClusterShardID) (chan RoutedAck, bool) {
	s.localAckChannelsMu.RLock()
	defer s.localAckChannelsMu.RUnlock()
	ch, exists := s.localAckChannels[shardID]
	return ch, exists
}

// RemoveLocalAckChan removes the ack channel for a specific shard ID only if it matches the provided channel
func (s *Proxy) RemoveLocalAckChan(shardID history.ClusterShardID, expectedChan chan RoutedAck) {
	s.logger.Info("Remove local ack channel for shard", tag.NewStringTag("shardID", ClusterShardIDtoString(shardID)))
	s.localAckChannelsMu.Lock()
	defer s.localAckChannelsMu.Unlock()
	if currentChan, exists := s.localAckChannels[shardID]; exists && currentChan == expectedChan {
		delete(s.localAckChannels, shardID)
	} else {
		s.logger.Info("Skipped removing local ack channel for shard (channel mismatch or already removed)", tag.NewStringTag("shardID", ClusterShardIDtoString(shardID)))
	}
}

// ForceRemoveLocalAckChan unconditionally removes the ack channel for a specific shard ID
func (s *Proxy) ForceRemoveLocalAckChan(shardID history.ClusterShardID) {
	s.logger.Info("Force remove local ack channel for shard", tag.NewStringTag("shardID", ClusterShardIDtoString(shardID)))
	s.localAckChannelsMu.Lock()
	defer s.localAckChannelsMu.Unlock()
	delete(s.localAckChannels, shardID)
}

// SetLocalReceiverCancelFunc registers a cancel function for a local receiver for a specific shard ID
func (s *Proxy) SetLocalReceiverCancelFunc(shardID history.ClusterShardID, cancelFunc context.CancelFunc) {
	s.logger.Info("Register local receiver cancel function for shard", tag.NewStringTag("shardID", ClusterShardIDtoString(shardID)))
	s.localReceiverCancelFuncsMu.Lock()
	defer s.localReceiverCancelFuncsMu.Unlock()
	s.localReceiverCancelFuncs[shardID] = cancelFunc
}

// GetLocalReceiverCancelFunc retrieves the cancel function for a local receiver for a specific shard ID
func (s *Proxy) GetLocalReceiverCancelFunc(shardID history.ClusterShardID) (context.CancelFunc, bool) {
	s.localReceiverCancelFuncsMu.RLock()
	defer s.localReceiverCancelFuncsMu.RUnlock()
	cancelFunc, exists := s.localReceiverCancelFuncs[shardID]
	return cancelFunc, exists
}

// RemoveLocalReceiverCancelFunc unconditionally removes the cancel function for a local receiver for a specific shard ID
// Note: Functions cannot be compared in Go, so we use unconditional removal.
// The race condition is primarily with channels; TerminatePreviousLocalReceiver handles forced cleanup.
func (s *Proxy) RemoveLocalReceiverCancelFunc(shardID history.ClusterShardID) {
	s.logger.Info("Remove local receiver cancel function for shard", tag.NewStringTag("shardID", ClusterShardIDtoString(shardID)))
	s.localReceiverCancelFuncsMu.Lock()
	defer s.localReceiverCancelFuncsMu.Unlock()
	delete(s.localReceiverCancelFuncs, shardID)
}

// TerminatePreviousLocalReceiver checks if there is a previous local receiver for this shard and terminates it if needed
func (s *Proxy) TerminatePreviousLocalReceiver(serverShardID history.ClusterShardID) {
	// Check if there's a previous cancel function for this shard
	if prevCancelFunc, exists := s.GetLocalReceiverCancelFunc(serverShardID); exists {
		s.logger.Info("Terminating previous local receiver for shard", tag.NewStringTag("shardID", ClusterShardIDtoString(serverShardID)))

		// Cancel the previous receiver's context
		prevCancelFunc()

		// Force remove the cancel function and ack channel from tracking
		s.RemoveLocalReceiverCancelFunc(serverShardID)
		s.ForceRemoveLocalAckChan(serverShardID)
	}
}
