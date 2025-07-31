package proxy

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/client/history"
	servercommon "go.temporal.io/server/common"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/temporalio/s2s-proxy/client"
	adminclient "github.com/temporalio/s2s-proxy/client/admin"
	"github.com/temporalio/s2s-proxy/common"
	"github.com/temporalio/s2s-proxy/config"
	"github.com/temporalio/s2s-proxy/metrics"
)

type (
	adminServiceProxyServer struct {
		adminservice.UnimplementedAdminServiceServer
		adminClient  adminservice.AdminServiceClient
		shardManager ShardManager
		logger       log.Logger
		proxyOptions
	}
)

func NewAdminServiceProxyServer(
	serviceName string,
	clientConfig config.ProxyClientConfig,
	clientFactory client.ClientFactory,
	shardManager ShardManager,
	opts proxyOptions,
	logger log.Logger,
) adminservice.AdminServiceServer {
	logger = log.With(logger, common.ServiceTag(serviceName))
	clientProvider := client.NewClientProvider(clientConfig, clientFactory, logger)
	return &adminServiceProxyServer{
		adminClient:  adminclient.NewLazyClient(clientProvider),
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
		case config.ShardCountFixed:
			if !s.IsInbound {
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

// forwardToProxy forwards a stream to another proxy instance
func (s *adminServiceProxyServer) forwardToProxy(
	targetStreamServer adminservice.AdminService_StreamWorkflowReplicationMessagesServer,
	ownerNode string,
	clientShardID history.ClusterShardID,
) error {
	// Get the proxy address for the owner node
	proxyAddr, found := s.shardManager.GetProxyAddress(ownerNode)
	if !found {
		s.logger.Error("No proxy address found for owner node",
			tag.NewStringTag("owner", ownerNode),
			tag.NewStringTag("clientShard", ClusterShardIDtoString(clientShardID)))
		if s.Config.MemberlistConfig != nil {
			metrics.ShardForwardingCounter.WithLabelValues(s.Config.MemberlistConfig.NodeName, ownerNode, "no_address").Inc()
		}
		return serviceerror.NewInternal(fmt.Sprintf("no proxy address found for node: %s", ownerNode))
	}

	s.logger.Info("Forwarding stream to proxy",
		tag.NewStringTag("clientShard", ClusterShardIDtoString(clientShardID)),
		tag.NewStringTag("owner", ownerNode),
		tag.NewStringTag("address", proxyAddr))

	// Create connection to the target proxy
	conn, err := grpc.NewClient(proxyAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		s.logger.Error("Failed to connect to target proxy",
			tag.Error(err),
			tag.NewStringTag("address", proxyAddr))
		if s.Config.MemberlistConfig != nil {
			metrics.ShardForwardingCounter.WithLabelValues(s.Config.MemberlistConfig.NodeName, ownerNode, "connection_failed").Inc()
		}
		return serviceerror.NewInternal(fmt.Sprintf("failed to connect to proxy: %v", err))
	}
	defer func() {
		if err := conn.Close(); err != nil {
			s.logger.Error("Failed to close connection", tag.Error(err))
		}
	}()

	// Create admin service client for the target proxy
	targetProxyClient := adminservice.NewAdminServiceClient(conn)

	// Forward the stream context and metadata
	ctx := targetStreamServer.Context()
	outgoingContext, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start the forwarded stream
	forwardedStream, err := targetProxyClient.StreamWorkflowReplicationMessages(outgoingContext)
	if err != nil {
		s.logger.Error("Failed to start forwarded stream",
			tag.Error(err),
			tag.NewStringTag("address", proxyAddr))
		if s.Config.MemberlistConfig != nil {
			metrics.ShardForwardingCounter.WithLabelValues(s.Config.MemberlistConfig.NodeName, ownerNode, "stream_failed").Inc()
		}
		return serviceerror.NewInternal(fmt.Sprintf("failed to start forwarded stream: %v", err))
	}

	if s.Config.MemberlistConfig != nil {
		metrics.ShardForwardingCounter.WithLabelValues(s.Config.MemberlistConfig.NodeName, ownerNode, "success").Inc()
	}

	// Set up bidirectional forwarding
	shutdownChan := channel.NewShutdownOnce()

	// Forward from target server to forwarded stream
	go func() {
		defer func() {
			s.logger.Debug("Shutdown target->forwarded forwarding loop")
			shutdownChan.Shutdown()
			if err := forwardedStream.CloseSend(); err != nil {
				s.logger.Error("Failed to close forwarded stream", tag.Error(err))
			}
		}()

		for !shutdownChan.IsShutdown() {
			req, err := targetStreamServer.Recv()
			if err == io.EOF {
				s.logger.Debug("Target stream recv EOF")
				return
			}
			if err != nil {
				s.logger.Error("Target stream recv error", tag.Error(err))
				return
			}

			if err := forwardedStream.Send(req); err != nil {
				if err != io.EOF {
					s.logger.Error("Forwarded stream send error", tag.Error(err))
				}
				return
			}
		}
	}()

	// Forward from forwarded stream to target server
	go func() {
		defer func() {
			s.logger.Debug("Shutdown forwarded->target forwarding loop")
			shutdownChan.Shutdown()
		}()

		for !shutdownChan.IsShutdown() {
			resp, err := forwardedStream.Recv()
			if err == io.EOF {
				s.logger.Debug("Forwarded stream recv EOF")
				return
			}
			if err != nil {
				s.logger.Error("Forwarded stream recv error", tag.Error(err))
				return
			}

			if err := targetStreamServer.Send(resp); err != nil {
				if err != io.EOF {
					s.logger.Error("Target stream send error", tag.Error(err))
				}
				return
			}
		}
	}()

	// Wait for shutdown
	select {
	case <-shutdownChan.Channel():
		return nil
	case <-outgoingContext.Done():
		return outgoingContext.Err()
	}
}

func (s *adminServiceProxyServer) StreamWorkflowReplicationMessages(
	targetStreamServer adminservice.AdminService_StreamWorkflowReplicationMessagesServer,
) (retError error) {
	defer log.CapturePanic(s.logger, &retError)

	targetMetadata, ok := metadata.FromIncomingContext(targetStreamServer.Context())
	if !ok {
		return serviceerror.NewInvalidArgument("missing cluster & shard ID metadata")
	}
	clientShardID, serverShardID, err := history.DecodeClusterShardMD(
		headers.NewGRPCHeaderGetter(targetStreamServer.Context()),
	)
	if err != nil {
		return err
	}

	// Register this shard as handled by this proxy
	if !s.IsInbound {
		s.shardManager.RegisterShard(clientShardID)
		defer s.shardManager.UnregisterShard(clientShardID)
	}

	logger := log.With(s.logger,
		tag.NewStringTag("client", ClusterShardIDtoString(clientShardID)),
		tag.NewStringTag("server", ClusterShardIDtoString(serverShardID)),
	)

	// Record streams active
	directionLabel := "inbound"
	if !s.IsInbound {
		directionLabel = "outbound"
	}
	logger.Info("AdminStreamReplicationMessages started.")
	streamsActiveGauge := metrics.AdminServiceStreamsActive.WithLabelValues(directionLabel)
	streamsActiveGauge.Inc()
	defer streamsActiveGauge.Dec()

	// Register stream with tracker for debugging
	streamID := fmt.Sprintf("%s-%s-%s-%d",
		ClusterShardIDtoString(clientShardID),
		ClusterShardIDtoString(serverShardID),
		directionLabel,
		time.Now().UnixNano(),
	)
	streamTracker := GetGlobalStreamTracker()
	streamTracker.RegisterStream(
		streamID,
		"StreamWorkflowReplicationMessages",
		directionLabel,
		ClusterShardIDtoString(clientShardID),
		ClusterShardIDtoString(serverShardID),
	)
	defer streamTracker.UnregisterStream(streamID)

	defer logger.Info("AdminStreamReplicationMessages stopped.")

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
		} else {
			// Stream is going to remote server.
			newClientShardID.ShardID = serverShardID.ShardID
		}

		logger = log.With(logger,
			tag.NewStringTag("newClient", ClusterShardIDtoString(newClientShardID)),
			tag.NewStringTag("newServer", ClusterShardIDtoString(newServerShardID)))

		// Maybe there's a cleaner way. Trying to preserve any other metadata.
		targetMetadata.Set(history.MetadataKeyClientClusterID, strconv.Itoa(int(newClientShardID.ClusterID)))
		targetMetadata.Set(history.MetadataKeyClientShardID, strconv.Itoa(int(newClientShardID.ShardID)))
		targetMetadata.Set(history.MetadataKeyServerClusterID, strconv.Itoa(int(newServerShardID.ClusterID)))
		targetMetadata.Set(history.MetadataKeyServerShardID, strconv.Itoa(int(newServerShardID.ShardID)))

		serverShardID = newServerShardID
	}

	// Check if we need to forward to another proxy for the target shard (only for inbound connections with forwarding enabled)
	if s.IsInbound && s.Config.MemberlistConfig != nil && s.Config.MemberlistConfig.EnableForwarding && !s.shardManager.IsLocalShard(serverShardID) {
		ownerNode, found := s.shardManager.GetShardOwner(serverShardID)
		if found {
			s.logger.Info("Forwarding inbound stream to target shard owner",
				tag.NewStringTag("serverShard", ClusterShardIDtoString(serverShardID)),
				tag.NewStringTag("owner", ownerNode))
			return s.forwardToProxy(targetStreamServer, ownerNode, serverShardID)
		}
		s.logger.Warn("No owner found for target shard, handling locally",
			tag.NewStringTag("serverShard", ClusterShardIDtoString(serverShardID)))
	}

	outgoingContext := metadata.NewOutgoingContext(targetStreamServer.Context(), targetMetadata)
	outgoingContext, cancel := context.WithCancel(outgoingContext)
	defer cancel()

	sourceStreamClient, err := s.adminClient.StreamWorkflowReplicationMessages(outgoingContext)
	if err != nil {
		logger.Error("remoteAdminServiceClient.StreamWorkflowReplicationMessages encountered error", tag.Error(err))
		return err
	}

	shutdownChan := channel.NewShutdownOnce()

	// Downstream (targetStreamServer) recv loop
	go func() {
		defer func() {
			logger.Info("Shutdown targetStreamServer.Recv loop.")
			shutdownChan.Shutdown()

			err = sourceStreamClient.CloseSend()
			if err != nil {
				logger.Error("Failed to close sourceStreamClient", tag.Error(err))
			}
		}()

		for !shutdownChan.IsShutdown() {
			req, err := targetStreamServer.Recv()
			if err == io.EOF {
				logger.Info("targetStreamServer.Recv encountered EOF", tag.Error(err))
				return
			}

			if err != nil {
				logger.Error("targetStreamServer.Recv encountered error", tag.Error(err))
				return
			}

			streamTracker.UpdateStream(streamID)

			switch attr := req.GetAttributes().(type) {
			case *adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState:
				logger.Debug(fmt.Sprintf("forwarding SyncReplicationState: inclusive %v", attr.SyncReplicationState.InclusiveLowWatermark))
				if err = sourceStreamClient.Send(req); err != nil {
					if err != io.EOF {
						logger.Error("sourceStreamClient.Send encountered error", tag.Error(err))
					} else {
						logger.Info("sourceStreamClient.Send encountered EOF", tag.Error(err))
					}
					return
				}
			default:
				logger.Error("targetStreamServer.Recv encountered error", tag.Error(serviceerror.NewInternal(fmt.Sprintf(
					"StreamWorkflowReplicationMessages encountered unknown type: %T %v", attr, attr,
				))))
				return
			}
		}
	}()

	// Upstream (sourceStreamClient) recv loop
	// If Upstream recv loop failed (sourceStream server restart for example), StreamWorkflowReplicationMessages
	// returns without waiting for Downstream loop to stop. This is because Downstream loop can be blocked at
	// targetStreamServer.Recv, which prevent StreamWorkflowReplicationMessages from returning.
	// Once StreamWorkflowReplicationMessages returns, targetStreamServer.Recv will be unblocked
	// (see https://stackoverflow.com/questions/68218469/how-to-un-wedge-go-grpc-bidi-streaming-server-from-the-blocking-recv-call)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			logger.Info("Shutdown sourceStreamClient.Recv loop.")

			shutdownChan.Shutdown()
			wg.Done()
		}()

		for !shutdownChan.IsShutdown() {
			resp, err := sourceStreamClient.Recv()
			if err == io.EOF {
				logger.Info("sourceStreamClient.Recv encountered EOF", tag.Error(err))
				return
			}

			if err != nil {
				logger.Error("sourceStreamClient.Recv encountered error", tag.Error(err))
				return
			}

			streamTracker.UpdateStream(streamID)

			switch attr := resp.GetAttributes().(type) {
			case *adminservice.StreamWorkflowReplicationMessagesResponse_Messages:
				logger.Debug(fmt.Sprintf("forwarding ReplicationMessages: exclusive %v", attr.Messages.ExclusiveHighWatermark))
				if err = targetStreamServer.Send(resp); err != nil {
					if err != io.EOF {
						logger.Error("targetStreamServer.Send encountered error", tag.Error(err))
					} else {
						logger.Info("targetStreamServer.Send encountered EOF", tag.Error(err))
					}
					return
				}
			default:
				logger.Error("sourceStreamClient.Recv encountered error", tag.Error(serviceerror.NewInternal(fmt.Sprintf(
					"StreamWorkflowReplicationMessages encountered unknown type: %T %v", attr, attr,
				))))
				return
			}
		}
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
