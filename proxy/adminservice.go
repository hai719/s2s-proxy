package proxy

import (
	"context"
	"fmt"
	"sync"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/client/history"
	servercommon "go.temporal.io/server/common"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/s2s-proxy/client"
	"github.com/temporalio/s2s-proxy/common"
	"github.com/temporalio/s2s-proxy/config"
	"github.com/temporalio/s2s-proxy/metrics"
)

type (
	adminServiceProxyServer struct {
		ps *ProxyServer
		adminservice.UnimplementedAdminServiceServer
		adminClient  adminservice.AdminServiceClient
		shardManager ShardManager
		logger       log.Logger
		proxyOptions
	}
)

func NewAdminServiceProxyServer(
	ps *ProxyServer,
	serviceName string,
	clientConfig config.ProxyClientConfig,
	clientFactory client.ClientFactory,
	shardManager ShardManager,
	opts proxyOptions,
	adminClient adminservice.AdminServiceClient,
	logger log.Logger,
) adminservice.AdminServiceServer {
	logger = log.With(logger, common.ServiceTag(serviceName))
	// Prefer a concrete admin client from factory for tests and non-lazy usage
	if clientFactory != nil {
		if c, err := clientFactory.NewRemoteAdminClient(clientConfig); err == nil && c != nil {
			adminClient = c
		}
	}
	return &adminServiceProxyServer{
		ps:           ps,
		adminClient:  adminClient,
		shardManager: shardManager,
		logger:       logger,
		proxyOptions: opts,
	}
}

func (s *adminServiceProxyServer) AddOrUpdateRemoteCluster(ctx context.Context, in0 *adminservice.AddOrUpdateRemoteClusterRequest) (*adminservice.AddOrUpdateRemoteClusterResponse, error) {
	if !common.IsRequestTranslationDisabled(ctx) {
		if outbound := s.Config.Outbound; s.IsInbound && outbound != nil && len(outbound.Server.ExternalAddress) > 0 {
			// Override this address so that cross-cluster connections flow through the proxy.
			// Use a separate "external address" config option because the outbound.listenerAddress may not be routable
			// from the local temporal server, or the proxy may be deployed behind a load balancer.
			in0.FrontendAddress = outbound.Server.ExternalAddress
		}
	}
	return s.adminClient.AddOrUpdateRemoteCluster(ctx, in0)
}

func (s *adminServiceProxyServer) AddSearchAttributes(ctx context.Context, in0 *adminservice.AddSearchAttributesRequest) (*adminservice.AddSearchAttributesResponse, error) {
	return s.adminClient.AddSearchAttributes(ctx, in0)
}

func (s *adminServiceProxyServer) AddTasks(ctx context.Context, in0 *adminservice.AddTasksRequest) (*adminservice.AddTasksResponse, error) {
	return s.adminClient.AddTasks(ctx, in0)
}

func (s *adminServiceProxyServer) CancelDLQJob(ctx context.Context, in0 *adminservice.CancelDLQJobRequest) (*adminservice.CancelDLQJobResponse, error) {
	return s.adminClient.CancelDLQJob(ctx, in0)
}

func (s *adminServiceProxyServer) CloseShard(ctx context.Context, in0 *adminservice.CloseShardRequest) (*adminservice.CloseShardResponse, error) {
	return s.adminClient.CloseShard(ctx, in0)
}

func (s *adminServiceProxyServer) DeleteWorkflowExecution(ctx context.Context, in0 *adminservice.DeleteWorkflowExecutionRequest) (*adminservice.DeleteWorkflowExecutionResponse, error) {
	return s.adminClient.DeleteWorkflowExecution(ctx, in0)
}

func (s *adminServiceProxyServer) DescribeCluster(ctx context.Context, in0 *adminservice.DescribeClusterRequest) (*adminservice.DescribeClusterResponse, error) {
	resp, err := s.adminClient.DescribeCluster(ctx, in0)
	if common.IsRequestTranslationDisabled(ctx) {
		return resp, err
	}

	var overrides *config.APIOverridesConfig
	if s.IsInbound {
		if s.Config.Inbound != nil {
			overrides = s.Config.Inbound.APIOverrides
		}
	} else {
		if s.Config.Outbound != nil {
			overrides = s.Config.Outbound.APIOverrides
		}
	}

	if overrides != nil && overrides.AdminSerivce.DescribeCluster != nil {
		responseOverride := overrides.AdminSerivce.DescribeCluster.Response
		if resp != nil && responseOverride.FailoverVersionIncrement != nil {
			resp.FailoverVersionIncrement = *responseOverride.FailoverVersionIncrement
		}
	}

	if cfg := s.Config.ShardCountConfig; resp != nil {
		switch cfg.Mode {
		case config.ShardCountLCM:
			// Present a fake number of shards. In LCM mode, we present the least
			// common multiple of both cluster shard counts.
			resp.HistoryShardCount = common.LCM(cfg.RemoteShardCount, cfg.LocalShardCount)
		case config.ShardCountRouting:
			if s.IsInbound {
				resp.HistoryShardCount = cfg.RemoteShardCount
			} else {
				resp.HistoryShardCount = cfg.LocalShardCount
			}
		}
	}

	return resp, err
}

func (s *adminServiceProxyServer) DescribeDLQJob(ctx context.Context, in0 *adminservice.DescribeDLQJobRequest) (*adminservice.DescribeDLQJobResponse, error) {
	return s.adminClient.DescribeDLQJob(ctx, in0)
}

func (s *adminServiceProxyServer) DescribeHistoryHost(ctx context.Context, in0 *adminservice.DescribeHistoryHostRequest) (*adminservice.DescribeHistoryHostResponse, error) {
	return s.adminClient.DescribeHistoryHost(ctx, in0)
}

func (s *adminServiceProxyServer) DescribeMutableState(ctx context.Context, in0 *adminservice.DescribeMutableStateRequest) (*adminservice.DescribeMutableStateResponse, error) {
	return s.adminClient.DescribeMutableState(ctx, in0)
}

func (s *adminServiceProxyServer) GetDLQMessages(ctx context.Context, in0 *adminservice.GetDLQMessagesRequest) (*adminservice.GetDLQMessagesResponse, error) {
	return s.adminClient.GetDLQMessages(ctx, in0)
}

func (s *adminServiceProxyServer) GetDLQReplicationMessages(ctx context.Context, in0 *adminservice.GetDLQReplicationMessagesRequest) (*adminservice.GetDLQReplicationMessagesResponse, error) {
	return s.adminClient.GetDLQReplicationMessages(ctx, in0)
}

func (s *adminServiceProxyServer) GetDLQTasks(ctx context.Context, in0 *adminservice.GetDLQTasksRequest) (*adminservice.GetDLQTasksResponse, error) {
	return s.adminClient.GetDLQTasks(ctx, in0)
}

func (s *adminServiceProxyServer) GetNamespace(ctx context.Context, in0 *adminservice.GetNamespaceRequest) (*adminservice.GetNamespaceResponse, error) {
	return s.adminClient.GetNamespace(ctx, in0)
}

func (s *adminServiceProxyServer) GetNamespaceReplicationMessages(ctx context.Context, in0 *adminservice.GetNamespaceReplicationMessagesRequest) (*adminservice.GetNamespaceReplicationMessagesResponse, error) {
	return s.adminClient.GetNamespaceReplicationMessages(ctx, in0)
}

func (s *adminServiceProxyServer) GetReplicationMessages(ctx context.Context, in0 *adminservice.GetReplicationMessagesRequest) (*adminservice.GetReplicationMessagesResponse, error) {
	return s.adminClient.GetReplicationMessages(ctx, in0)
}

func (s *adminServiceProxyServer) GetSearchAttributes(ctx context.Context, in0 *adminservice.GetSearchAttributesRequest) (*adminservice.GetSearchAttributesResponse, error) {
	return s.adminClient.GetSearchAttributes(ctx, in0)
}

func (s *adminServiceProxyServer) GetShard(ctx context.Context, in0 *adminservice.GetShardRequest) (*adminservice.GetShardResponse, error) {
	return s.adminClient.GetShard(ctx, in0)
}

func (s *adminServiceProxyServer) GetTaskQueueTasks(ctx context.Context, in0 *adminservice.GetTaskQueueTasksRequest) (*adminservice.GetTaskQueueTasksResponse, error) {
	return s.adminClient.GetTaskQueueTasks(ctx, in0)
}

func (s *adminServiceProxyServer) GetWorkflowExecutionRawHistory(ctx context.Context, in0 *adminservice.GetWorkflowExecutionRawHistoryRequest) (*adminservice.GetWorkflowExecutionRawHistoryResponse, error) {
	return s.adminClient.GetWorkflowExecutionRawHistory(ctx, in0)
}

func (s *adminServiceProxyServer) GetWorkflowExecutionRawHistoryV2(ctx context.Context, in0 *adminservice.GetWorkflowExecutionRawHistoryV2Request) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {
	return s.adminClient.GetWorkflowExecutionRawHistoryV2(ctx, in0)
}

func (s *adminServiceProxyServer) ImportWorkflowExecution(ctx context.Context, in0 *adminservice.ImportWorkflowExecutionRequest) (*adminservice.ImportWorkflowExecutionResponse, error) {
	return s.adminClient.ImportWorkflowExecution(ctx, in0)
}

func (s *adminServiceProxyServer) ListClusterMembers(ctx context.Context, in0 *adminservice.ListClusterMembersRequest) (*adminservice.ListClusterMembersResponse, error) {
	return s.adminClient.ListClusterMembers(ctx, in0)
}

func (s *adminServiceProxyServer) ListClusters(ctx context.Context, in0 *adminservice.ListClustersRequest) (*adminservice.ListClustersResponse, error) {
	return s.adminClient.ListClusters(ctx, in0)
}

func (s *adminServiceProxyServer) ListHistoryTasks(ctx context.Context, in0 *adminservice.ListHistoryTasksRequest) (*adminservice.ListHistoryTasksResponse, error) {
	return s.adminClient.ListHistoryTasks(ctx, in0)
}

func (s *adminServiceProxyServer) ListQueues(ctx context.Context, in0 *adminservice.ListQueuesRequest) (*adminservice.ListQueuesResponse, error) {
	return s.adminClient.ListQueues(ctx, in0)
}

func (s *adminServiceProxyServer) MergeDLQMessages(ctx context.Context, in0 *adminservice.MergeDLQMessagesRequest) (*adminservice.MergeDLQMessagesResponse, error) {
	return s.adminClient.MergeDLQMessages(ctx, in0)
}

func (s *adminServiceProxyServer) MergeDLQTasks(ctx context.Context, in0 *adminservice.MergeDLQTasksRequest) (*adminservice.MergeDLQTasksResponse, error) {
	return s.adminClient.MergeDLQTasks(ctx, in0)
}

func (s *adminServiceProxyServer) PurgeDLQMessages(ctx context.Context, in0 *adminservice.PurgeDLQMessagesRequest) (*adminservice.PurgeDLQMessagesResponse, error) {
	return s.adminClient.PurgeDLQMessages(ctx, in0)
}

func (s *adminServiceProxyServer) PurgeDLQTasks(ctx context.Context, in0 *adminservice.PurgeDLQTasksRequest) (*adminservice.PurgeDLQTasksResponse, error) {
	return s.adminClient.PurgeDLQTasks(ctx, in0)
}

func (s *adminServiceProxyServer) ReapplyEvents(ctx context.Context, in0 *adminservice.ReapplyEventsRequest) (*adminservice.ReapplyEventsResponse, error) {
	return s.adminClient.ReapplyEvents(ctx, in0)
}

func (s *adminServiceProxyServer) RebuildMutableState(ctx context.Context, in0 *adminservice.RebuildMutableStateRequest) (*adminservice.RebuildMutableStateResponse, error) {
	return s.adminClient.RebuildMutableState(ctx, in0)
}

func (s *adminServiceProxyServer) RefreshWorkflowTasks(ctx context.Context, in0 *adminservice.RefreshWorkflowTasksRequest) (*adminservice.RefreshWorkflowTasksResponse, error) {
	return s.adminClient.RefreshWorkflowTasks(ctx, in0)
}

func (s *adminServiceProxyServer) RemoveRemoteCluster(ctx context.Context, in0 *adminservice.RemoveRemoteClusterRequest) (*adminservice.RemoveRemoteClusterResponse, error) {
	return s.adminClient.RemoveRemoteCluster(ctx, in0)
}

func (s *adminServiceProxyServer) RemoveSearchAttributes(ctx context.Context, in0 *adminservice.RemoveSearchAttributesRequest) (*adminservice.RemoveSearchAttributesResponse, error) {
	return s.adminClient.RemoveSearchAttributes(ctx, in0)
}

func (s *adminServiceProxyServer) RemoveTask(ctx context.Context, in0 *adminservice.RemoveTaskRequest) (*adminservice.RemoveTaskResponse, error) {
	return s.adminClient.RemoveTask(ctx, in0)
}

func (s *adminServiceProxyServer) ResendReplicationTasks(ctx context.Context, in0 *adminservice.ResendReplicationTasksRequest) (*adminservice.ResendReplicationTasksResponse, error) {
	return s.adminClient.ResendReplicationTasks(ctx, in0)
}

func ClusterShardIDtoString(sd history.ClusterShardID) string {
	return fmt.Sprintf("(id: %d, shard: %d)", sd.ClusterID, sd.ShardID)
}

func ClusterShardIDtoShortString(sd history.ClusterShardID) string {
	return fmt.Sprintf("%d:%d", sd.ClusterID, sd.ShardID)
}

// StreamWorkflowReplicationMessages establishes an HTTP/2 stream. gRPC passes us a stream that represents the initiating server,
// and we can freely Send and Recv on that "server". Because this is a proxy, we also establish a bidirectional
// stream using our configured adminClient. When we Recv on the initiator, we Send to the client.
// When we Recv on the client, we Send to the initiator
func (s *adminServiceProxyServer) StreamWorkflowReplicationMessages(
	streamServer adminservice.AdminService_StreamWorkflowReplicationMessagesServer,
) (retError error) {
	defer log.CapturePanic(s.logger, &retError)

	targetMetadata, ok := metadata.FromIncomingContext(streamServer.Context())
	if !ok {
		return serviceerror.NewInvalidArgument("missing cluster & shard ID metadata")
	}
	clientShardID, serverShardID, err := history.DecodeClusterShardMD(
		headers.NewGRPCHeaderGetter(streamServer.Context()),
	)
	if err != nil {
		return err
	}

	// Detect intra-proxy streams early for logging/behavior toggles
	isIntraProxy := common.IsIntraProxy(streamServer.Context())

	logger := log.With(s.logger,
		tag.NewStringTag("client", ClusterShardIDtoString(clientShardID)),
		tag.NewStringTag("server", ClusterShardIDtoString(serverShardID)),
		tag.NewStringTag("stream-source-shard", ClusterShardIDtoString(serverShardID)),
		tag.NewStringTag("stream-target-shard", ClusterShardIDtoString(clientShardID)),
	)

	// Record streams active
	directionLabel := "outbound"
	if isIntraProxy {
		directionLabel = "intra-proxy"
	} else if s.IsInbound {
		directionLabel = "inbound"
	}
	logger.Info("AdminStreamReplicationMessages started.", tag.NewStringTag("direction", directionLabel), tag.NewBoolTag("isIntraProxy", isIntraProxy))
	defer logger.Info("AdminStreamReplicationMessages stopped.", tag.NewStringTag("direction", directionLabel), tag.NewBoolTag("isIntraProxy", isIntraProxy))

	streamsActiveGauge := metrics.AdminServiceStreamsActive.WithLabelValues(directionLabel)
	streamsActiveGauge.Inc()
	metrics.AdminServiceStreamsOpenedCount.WithLabelValues(directionLabel).Inc()
	defer streamsActiveGauge.Dec()

	if cfg := s.Config.ShardCountConfig; cfg.Mode == config.ShardCountLCM {
		// Abitrary shard count support.
		//
		// Temporal only supports shard counts where one shard count is an even multiple of the other.
		// The trick in this mode is the proxy will present the Least Common Multiple of both cluster shard counts.
		// Temporal establishes outbound replication streams to the proxy for all unqiue shard id pairs between
		// itself and the proxy's shard count. Then the proxy directly forwards those streams along to the target
		// cluster, remapping proxy stream shard ids to the target cluster shard ids.
		newClientShardID := history.ClusterShardID{
			ClusterID: clientShardID.ClusterID,
			ShardID:   clientShardID.ShardID, // proxy fake shard id
		}
		newServerShardID := history.ClusterShardID{
			ClusterID: serverShardID.ClusterID,
			ShardID:   serverShardID.ShardID,
		}
		LCM := common.LCM(cfg.LocalShardCount, cfg.RemoteShardCount)
		if s.IsInbound {
			// Stream is going to local server. Remap shard id by local server shard count.
			newServerShardID.ShardID = mapShardIDUnique(LCM, cfg.LocalShardCount, serverShardID.ShardID)
			serverShardID = newServerShardID
			// targetMetadata.Set(history.MetadataKeyServerClusterID, fmt.Sprintf("%d", newServerShardID.ClusterID))
			targetMetadata.Set(history.MetadataKeyServerShardID, fmt.Sprintf("%d", newServerShardID.ShardID))
		} else {
			// Stream is going to remote server.
			newClientShardID.ShardID = serverShardID.ShardID
			clientShardID = newClientShardID
			// targetMetadata.Set(history.MetadataKeyClientClusterID, fmt.Sprintf("%d", newClientShardID.ClusterID))
			targetMetadata.Set(history.MetadataKeyClientShardID, fmt.Sprintf("%d", newClientShardID.ShardID))
		}

		logger = log.With(logger,
			tag.NewStringTag("newClient", ClusterShardIDtoString(newClientShardID)),
			tag.NewStringTag("newServer", ClusterShardIDtoString(newServerShardID)))

	}

	if isIntraProxy {
		return s.streamIntraProxyRouting(logger, streamServer, targetMetadata, clientShardID, serverShardID, directionLabel)
	}

	if s.Config.ShardCountConfig.Mode == config.ShardCountRouting {
		return s.streamRouting(logger, streamServer, targetMetadata, clientShardID, serverShardID, directionLabel)
	}

	return s.streamForwarding(logger, streamServer, targetMetadata, clientShardID, serverShardID, directionLabel)
}

func (s *adminServiceProxyServer) streamForwarding(
	logger log.Logger,
	sourceStreamServer adminservice.AdminService_StreamWorkflowReplicationMessagesServer,
	targetMetadata metadata.MD,
	clientShardID history.ClusterShardID,
	serverShardID history.ClusterShardID,
	directionLabel string,
) error {
	logger.Info("streamForwarding started")
	defer logger.Info("streamForwarding finished")

	outgoingContext := metadata.NewOutgoingContext(sourceStreamServer.Context(), targetMetadata)
	outgoingContext, cancel := context.WithCancel(outgoingContext)
	defer cancel()

	sourceStreamClient, err := s.adminClient.StreamWorkflowReplicationMessages(outgoingContext)
	if err != nil {
		logger.Error("remoteAdminServiceClient.StreamWorkflowReplicationMessages encountered error", tag.Error(err))
		return err
	}

	shutdownChan := channel.NewShutdownOnce()
	forwarder := &proxyStreamForwarder{logger: logger, cancel: cancel}
	forwarder.Run(
		directionLabel,
		clientShardID,
		serverShardID,
		sourceStreamServer,
		sourceStreamClient,
		shutdownChan,
	)

	return nil
}

func (s *adminServiceProxyServer) streamIntraProxyRouting(
	logger log.Logger,
	streamServer adminservice.AdminService_StreamWorkflowReplicationMessagesServer,
	targetMetadata metadata.MD,
	clientShardID history.ClusterShardID,
	serverShardID history.ClusterShardID,
	directionLabel string,
) error {
	logger.Info("streamIntraProxyRouting started")
	defer logger.Info("streamIntraProxyRouting finished")

	// Determine remote peer identity from intra-proxy headers
	peerNodeName := ""
	if md, ok := metadata.FromIncomingContext(streamServer.Context()); ok {
		vals := md.Get(common.IntraProxyOriginProxyIDHeader)
		if len(vals) > 0 {
			peerNodeName = vals[0]
		}
	}

	// Only allow intra-proxy when at least one shard is local to this proxy instance
	isLocalClient := s.shardManager.IsLocalShard(clientShardID)
	isLocalServer := s.shardManager.IsLocalShard(serverShardID)
	if isLocalClient || !isLocalServer {
		logger.Info("Skipping intra-proxy between two local shards or two remote shards. Client may use outdated shard info.",
			tag.NewStringTag("client", ClusterShardIDtoString(clientShardID)),
			tag.NewStringTag("server", ClusterShardIDtoString(serverShardID)),
			tag.NewBoolTag("isLocalClient", isLocalClient),
			tag.NewBoolTag("isLocalServer", isLocalServer),
		)
		return nil
	}

	// Sender: handle ACKs coming from peer and forward to original owner
	sender := &intraProxyStreamSender{
		logger:        logger,
		shardManager:  s.shardManager,
		proxy:         s.ps.proxy,
		intraMgr:      s.ps.proxy.intraMgr,
		peerNodeName:  peerNodeName,
		targetShardID: clientShardID,
		sourceShardID: serverShardID,
	}

	shutdownChan := channel.NewShutdownOnce()
	go func() {
		if err := sender.Run(streamServer, shutdownChan); err != nil {
			logger.Error("intraProxyStreamSender.Run error", tag.Error(err))
		}
	}()

	<-shutdownChan.Channel()
	return nil
}

// streamRouting handles stream routing for fixed shard count mode with forwarding enabled.
// This function manages both inbound and outbound stream connections to ensure proper
// shard ownership and paired stream lifecycle management.
func (s *adminServiceProxyServer) streamRouting(
	logger log.Logger,
	sourceStreamServer adminservice.AdminService_StreamWorkflowReplicationMessagesServer,
	targetMetadata metadata.MD,
	targetShardID history.ClusterShardID,
	sourceShardID history.ClusterShardID,
	directionLabel string,
) error {
	logger.Info("streamRouting started")
	defer logger.Info("streamRouting stopped")

	// client: stream receiver
	// server: stream sender
	proxyStreamSender := &proxyStreamSender{
		logger:         logger,
		shardManager:   s.shardManager,
		proxy:          s.ps.proxy,
		sourceShardID:  sourceShardID,
		targetShardID:  targetShardID,
		directionLabel: directionLabel,
	}

	var localShardCount int32
	if s.IsInbound {
		localShardCount = s.Config.ShardCountConfig.LocalShardCount
	} else {
		localShardCount = s.Config.ShardCountConfig.RemoteShardCount
	}
	// receiver for reverse direction
	proxyStreamReceiverReverse := &proxyStreamReceiver{
		logger:          s.logger,
		shardManager:    s.shardManager,
		proxyServer:     s.ps,
		proxy:           s.ps.proxy,
		localShardCount: localShardCount,
		sourceShardID:   targetShardID,
		targetShardID:   sourceShardID,
		directionLabel:  directionLabel,
	}

	shutdownChan := channel.NewShutdownOnce()
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		proxyStreamSender.Run(sourceStreamServer, shutdownChan)
	}()
	go func() {
		defer wg.Done()
		proxyStreamReceiverReverse.Run(shutdownChan)
	}()
	wg.Wait()
	return nil
}

func mapShardIDUnique(sourceShardCount, targetShardCount, sourceShardID int32) int32 {
	targetShardID := servercommon.MapShardID(sourceShardCount, targetShardCount, sourceShardID)
	if len(targetShardID) != 1 {
		panic(fmt.Sprintf("remapping shard count error: sourceShardCount=%d targetShardCount=%d sourceShardID=%d targetShardID=%v\n",
			sourceShardCount, targetShardCount, sourceShardID, targetShardID))
	}
	return targetShardID[0]
}
