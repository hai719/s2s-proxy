package proxy

import (
	"context"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	replicationv1 "go.temporal.io/server/api/replication/v1"
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
	// clientProvider := client.NewClientProvider(clientConfig, clientFactory, logger)
	return &adminServiceProxyServer{
		ps: ps,
		// adminClient:  adminclient.NewLazyClient(clientProvider),
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

// streamRouting handles stream routing for fixed shard count mode with forwarding enabled.
// This function manages both inbound and outbound stream connections to ensure proper
// shard ownership and paired stream lifecycle management.
func (s *adminServiceProxyServer) streamRouting(
	logger log.Logger,
	targetStreamServer adminservice.AdminService_StreamWorkflowReplicationMessagesServer,
	streamTracker *StreamTracker,
	streamID string,
	targetMetadata metadata.MD,
	clientShardID history.ClusterShardID,
	serverShardID history.ClusterShardID,
) error {
	logger.Info("streamRouting called for fixed shard count mode",
		tag.NewStringTag("isInbound", fmt.Sprintf("%t", s.IsInbound)))

	if s.IsInbound {
		// Inbound flow: Remote → Proxy → Local
		// When a remote cluster connects to this proxy, this proxy becomes the owner
		// of the target shard
		logger.Info("Handling inbound stream routing")
		return s.handleInboundStream(targetStreamServer, streamTracker, streamID, clientShardID, serverShardID, logger)
	} else {
		// Outbound flow: Local → Proxy → Remote
		// This is the paired stream for the inbound connection
		logger.Info("Handling outbound stream routing")
		return s.handleOutboundStream(targetStreamServer, streamTracker, streamID, targetMetadata, clientShardID, serverShardID, logger)
	}
}

func (s *adminServiceProxyServer) handleInboundStream(
	targetStreamServer adminservice.AdminService_StreamWorkflowReplicationMessagesServer,
	streamTracker *StreamTracker,
	streamID string,
	clientShardID history.ClusterShardID,
	serverShardID history.ClusterShardID,
	logger log.Logger,
) error {
	logger.Info("handleInboundStream: using streamWithLocalServer for inbound traffic")

	shutdownChan := channel.NewShutdownOnce()
	wg := sync.WaitGroup{}
	wg.Add(2)

	// send loop: send replication tasks to remote proxy
	sendChan := make(chan *adminservice.StreamWorkflowReplicationMessagesResponse, 100)
	s.ps.proxy.SetRemoteSendChan(clientShardID, sendChan)
	defer s.ps.proxy.RemoveRemoteSendChan(clientShardID) // Ensure cleanup on function exit

	go func() {
		defer func() {
			logger.Info("Shutdown targetStreamServer.Send loop.")
			shutdownChan.Shutdown()
			wg.Done()
		}()

		for !shutdownChan.IsShutdown() {
			select {
			case resp := <-sendChan:
				msg := make([]string, 0, len(resp.Attributes.(*adminservice.StreamWorkflowReplicationMessagesResponse_Messages).Messages.ReplicationTasks))
				for i, task := range resp.Attributes.(*adminservice.StreamWorkflowReplicationMessagesResponse_Messages).Messages.ReplicationTasks {
					msg = append(msg, fmt.Sprintf("[%d]: %v@%v", i, task.SourceTaskId, task.SourceShardId))
				}
				logger.Info("Sending replication tasks to remote proxy",
					tag.NewStringTag("streamID", streamID),
					tag.NewStringTag("priority", fmt.Sprintf("%v", resp.Attributes.(*adminservice.StreamWorkflowReplicationMessagesResponse_Messages).Messages.Priority)),
					tag.NewStringTag("sourceShardId", fmt.Sprintf("%v", resp.Attributes.(*adminservice.StreamWorkflowReplicationMessagesResponse_Messages).Messages.SourceShardId)),
					tag.NewStringTag("exclusiveHighWatermark", fmt.Sprintf("%v", resp.Attributes.(*adminservice.StreamWorkflowReplicationMessagesResponse_Messages).Messages.ExclusiveHighWatermark)),
					tag.NewStringTag("tasks", strings.Join(msg, ", ")),
				)
				streamTracker.UpdateStream(streamID)
				streamTracker.UpdateStreamReplicationMessages(streamID, resp.Attributes.(*adminservice.StreamWorkflowReplicationMessagesResponse_Messages).Messages.ExclusiveHighWatermark)
				if err := targetStreamServer.Send(resp); err != nil {
					logger.Error("targetStreamServer.Send encountered error", tag.Error(err))
					return
				}
			case <-shutdownChan.Channel():
				// Shutdown requested, exit the loop
				return
			}
		}
	}()

	// recv loop: ACK from remote proxy, forward to ackChan managed by startLocalReceiver
	go func() {
		defer func() {
			logger.Info("Shutdown targetStreamServer.Recv loop.")
			shutdownChan.Shutdown()
			wg.Done()
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

			// streamTracker.UpdateStream(streamID)

			switch attr := req.GetAttributes().(type) {
			case *adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState:
				logger.Info(fmt.Sprintf("forwarding SyncReplicationState: inclusive %v, attr: %v", attr.SyncReplicationState.InclusiveLowWatermark, attr))

				// Loop through each source shard state
				for sourceShardID := range attr.SyncReplicationState.SourceShardStates {
					logger.Info("Processing source shard state",
						tag.NewInt32("sourceShardID", sourceShardID),
						tag.NewStringTag("sourceShardState", fmt.Sprintf("%v", attr.SyncReplicationState.SourceShardStates[sourceShardID])),
					)

					// Get the appropriate ack channel for this shard
					ackChan, ok := s.ps.proxy.GetLocalAckChan(history.ClusterShardID{
						ClusterID: serverShardID.ClusterID,
						ShardID:   sourceShardID,
					})
					if !ok {
						logger.Error("No ack channel found for server shard", tag.NewStringTag("serverShard", ClusterShardIDtoString(serverShardID)))
						continue
					}
					// TODO: send only the ACK for the source shard.
					logger.Info("Sending ACK to source shard",
						tag.NewStringTag("source_shard", fmt.Sprintf("%v", sourceShardID)),
						tag.NewStringTag("ack", fmt.Sprintf("%v", req)),
					)
					ackChan <- req
				}
			default:
				logger.Error("targetStreamServer.Recv encountered error", tag.Error(serviceerror.NewInternal(fmt.Sprintf(
					"StreamWorkflowReplicationMessages encountered unknown type: %T %v", attr, attr,
				))))
				return
			}
		}
	}()

	wg.Wait()

	return nil
}

func (s *adminServiceProxyServer) handleOutboundStream(
	targetStreamServer adminservice.AdminService_StreamWorkflowReplicationMessagesServer,
	streamTracker *StreamTracker,
	streamID string,
	targetMetadata metadata.MD,
	clientShardID history.ClusterShardID,
	serverShardID history.ClusterShardID,
	logger log.Logger,
) error {
	logger.Info("handleOutboundStream: using streamForwarding for outbound traffic")

	// shutdownChan := channel.NewShutdownOnce()
	// 1. Use streamForwarding() for direct forwarding to remote proxy
	// This handles the bidirectional communication with the remote proxy
	errChan := make(chan error, 1)
	go func() {
		err := s.streamForwarding(logger, targetStreamServer, streamTracker, streamID, targetMetadata, nil)
		if err != nil {
			logger.Error("streamForwarding encountered error", tag.Error(err))
		}
		errChan <- err
	}()

	// 2. establish a stream to local server as a receiver. When receiving replication tasks from local server, we need to recalculate the target shard, and forward to the correct stream managed by handleInboundStream.
	go func() {
		err := s.startLocalReceiver(clientShardID, serverShardID, nil)
		if err != nil {
			logger.Error("localReceiver encountered error", tag.Error(err))
		}
		errChan <- err
	}()

	// 3. The streams in 1 and 2 should be closed together.
	return <-errChan
}

func (s *adminServiceProxyServer) startLocalReceiver(
	clientShardID history.ClusterShardID,
	serverShardID history.ClusterShardID,
	shutdownChan channel.ShutdownOnce,
) error {
	logger := log.With(s.logger,
		tag.NewStringTag("client", ClusterShardIDtoString(clientShardID)),
		tag.NewStringTag("server", ClusterShardIDtoString(serverShardID)),
	)

	// Check if there is a previous local receiver for this shard, and terminate that if needed
	s.ps.proxy.TerminatePreviousLocalReceiver(clientShardID)

	// Create shutdownChan if it's nil
	if shutdownChan == nil {
		shutdownChan = channel.NewShutdownOnce()
	}

	md := metadata.New(map[string]string{})
	md.Set(history.MetadataKeyClientClusterID, strconv.Itoa(int(serverShardID.ClusterID)))
	md.Set(history.MetadataKeyClientShardID, strconv.Itoa(int(serverShardID.ShardID)))
	md.Set(history.MetadataKeyServerClusterID, strconv.Itoa(int(clientShardID.ClusterID)))
	md.Set(history.MetadataKeyServerShardID, strconv.Itoa(int(clientShardID.ShardID)))

	outgoingContext := metadata.NewOutgoingContext(context.Background(), md)
	outgoingContext, cancel := context.WithCancel(outgoingContext)
	defer cancel() // Ensure context is cancelled to prevent leaks

	// stream receiver -> local server's stream sender, clientShardID
	sourceStreamClient, err := s.ps.proxy.inboundServer.GetAdminClient().StreamWorkflowReplicationMessages(outgoingContext)
	if err != nil {
		logger.Error("remoteAdminServiceClient.StreamWorkflowReplicationMessages encountered error", tag.Error(err))
		return err
	}

	ackByTargetShard := make(map[history.ClusterShardID]*replicationv1.SourceShardStates)
	ackChan := make(chan *adminservice.StreamWorkflowReplicationMessagesRequest, 100)
	s.ps.proxy.SetLocalAckChan(clientShardID, ackChan)

	// Register the cancel function for this local receiver so it can be terminated later if needed
	s.ps.proxy.SetLocalReceiverCancelFunc(clientShardID, cancel)

	defer func() {
		// Ensure cleanup on function exit
		s.ps.proxy.RemoveLocalAckChan(clientShardID)
		s.ps.proxy.RemoveLocalReceiverCancelFunc(clientShardID)
	}()

	// Register stream with tracker for debugging
	directionLabel := "receiver"
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

	// Process outgoing responses (upstream → downstream)
	var wg sync.WaitGroup
	wg.Add(2)
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
				logger.Info(fmt.Sprintf("processing ReplicationMessages: priority %v, exclusive %v, tasks: %v", attr.Messages.Priority, attr.Messages.ExclusiveHighWatermark, len(attr.Messages.ReplicationTasks)))

				// Update stream tracker with ReplicationMessages information
				streamTracker.UpdateStreamReplicationMessages(streamID, attr.Messages.ExclusiveHighWatermark)

				// Process each replication task to recalculate target shard

				// TODO: if replication tasks are empty, we should send the exclusive high watermark info to all target shards.
				if len(attr.Messages.ReplicationTasks) == 0 {
					logger.Info("Replication tasks are empty, sending exclusive high watermark info to all target shards")
					for targetShardID, sendChan := range s.ps.proxy.GetAllRemoteSendChans() {
						logger.Info("Sending high watermark to target shard", tag.NewStringTag("targetShard", ClusterShardIDtoString(targetShardID)))
						sendChan <- resp
					}
				} else {
					// TODO: use tasksByOwner instead of tasksByTargetShard when forwarding to another proxy
					tasksByTargetShard := make(map[history.ClusterShardID][]*replicationv1.ReplicationTask)

					for _, task := range attr.Messages.ReplicationTasks {
						if task.RawTaskInfo != nil && task.RawTaskInfo.NamespaceId != "" && task.RawTaskInfo.WorkflowId != "" {
							targetShardID := servercommon.WorkflowIDToHistoryShard(task.RawTaskInfo.NamespaceId, task.RawTaskInfo.WorkflowId, s.Config.ShardCountConfig.LocalShardCount)

							logger.Info("Recalculated shard for workflow task",
								tag.NewStringTag("namespaceId", task.RawTaskInfo.NamespaceId),
								tag.NewStringTag("workflowId", task.RawTaskInfo.WorkflowId),
								tag.NewStringTag("targetShard", fmt.Sprintf("%d", targetShardID)))

							targetClusterShardID := history.ClusterShardID{
								ClusterID: serverShardID.ClusterID,
								ShardID:   targetShardID,
							}
							tasksByTargetShard[targetClusterShardID] = append(tasksByTargetShard[targetClusterShardID], task)
						}
					}

					// TODO: Forward tasks to respective owner nodes
					// For now, just do in-proxy routing
					for targetShardID, tasks := range tasksByTargetShard {
						logger.Info("Tasks to forward to target shard",
							tag.NewStringTag("targetShard", ClusterShardIDtoString(targetShardID)),
							tag.NewStringTag("taskCount", fmt.Sprintf("%d", len(tasks))))
						sendChan, ok := s.ps.proxy.GetRemoteSendChan(targetShardID)
						if !ok {
							logger.Error("No send channel found for target shard", tag.NewStringTag("targetShard", ClusterShardIDtoString(targetShardID)))
							continue
						}
						sendChan <- &adminservice.StreamWorkflowReplicationMessagesResponse{
							Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{
								Messages: &replicationv1.WorkflowReplicationMessages{
									ReplicationTasks:       tasks,
									ExclusiveHighWatermark: tasks[len(tasks)-1].RawTaskInfo.TaskId + 1,
									Priority:               attr.Messages.Priority,
									SourceShardId:          attr.Messages.SourceShardId,
								},
							},
						}
					}
				}

			default:
				// For non-message responses, just forward as-is
				logger.Info("Forwarding non-message response",
					tag.NewStringTag("type", fmt.Sprintf("%T", attr)))
			}

		}
	}()

	go func() {
		defer func() {
			logger.Info("Shutdown sourceStreamClient.Recv loop.")
			shutdownChan.Shutdown()
			err := sourceStreamClient.CloseSend()
			if err != nil {
				logger.Error("Failed to close sourceStreamClient", tag.Error(err))
			}
			wg.Done()
		}()

		for !shutdownChan.IsShutdown() {
			select {
			case req := <-ackChan:
				switch attr := req.GetAttributes().(type) {
				case *adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState:
					logger.Info(fmt.Sprintf("forwarding SyncReplicationState: inclusive %v", attr.SyncReplicationState.InclusiveLowWatermark))

					// Initialize minimal ack with the original watermarks
					minimalAck := &replicationv1.SyncReplicationState{
						HighPriorityState: &replicationv1.ReplicationState{
							InclusiveLowWatermark: math.MaxInt64,
						},
						LowPriorityState: &replicationv1.ReplicationState{
							InclusiveLowWatermark: math.MaxInt64,
						},
					}
					found := false

					for sourceShardID, sourceShardState := range attr.SyncReplicationState.SourceShardStates {
						if sourceShardID != clientShardID.ShardID {
							continue
						}

						targetShardID := history.ClusterShardID{
							ClusterID: serverShardID.ClusterID,
							ShardID:   attr.SyncReplicationState.TargetShardId,
						}
						ackByTargetShard[targetShardID] = sourceShardState
						found = true

						logger.Info("Processing source shard state for aggregation",
							tag.NewInt32("sourceShardID", sourceShardID),
							tag.NewStringTag("targetShard", ClusterShardIDtoString(targetShardID)))
					}

					if !found {
						logger.Error("No source shard state found for client shard", tag.NewInt32("clientShardID", clientShardID.ShardID))
						return
					}

					// Aggregate minimal ack from all target shards
					for _, sourceShardState := range ackByTargetShard {
						// Compare high priority states - find minimum inclusive low watermark
						if sourceShardState.HighPriorityState != nil && minimalAck.HighPriorityState != nil {
							if sourceShardState.HighPriorityState.InclusiveLowWatermark < minimalAck.HighPriorityState.InclusiveLowWatermark {
								minimalAck.HighPriorityState.InclusiveLowWatermark = sourceShardState.HighPriorityState.InclusiveLowWatermark
							}
						}
						// Compare low priority states - find minimum inclusive low watermark
						if sourceShardState.LowPriorityState != nil && minimalAck.LowPriorityState != nil {
							if sourceShardState.LowPriorityState.InclusiveLowWatermark < minimalAck.LowPriorityState.InclusiveLowWatermark {
								minimalAck.LowPriorityState.InclusiveLowWatermark = sourceShardState.LowPriorityState.InclusiveLowWatermark
							}
						}
					}

					// Create a new request with the minimal ack to send back to source shard
					newReq := &adminservice.StreamWorkflowReplicationMessagesRequest{
						Attributes: &adminservice.StreamWorkflowReplicationMessagesRequest_SyncReplicationState{
							SyncReplicationState: &replicationv1.SyncReplicationState{
								// TODO: tiered replication processing is not supported yet.
								// HighPriorityState: minimalAck.HighPriorityState,
								// LowPriorityState:  minimalAck.LowPriorityState,
								InclusiveLowWatermark:     minimalAck.HighPriorityState.InclusiveLowWatermark,
								InclusiveLowWatermarkTime: minimalAck.HighPriorityState.InclusiveLowWatermarkTime,
								TargetShardId:             attr.SyncReplicationState.TargetShardId,
							},
						},
					}

					logger.Info("Sending aggregated ack with minimal watermarks",
						tag.NewInt32("clientShardID", clientShardID.ShardID),
						tag.NewInt64("highPriorityMinWatermark", minimalAck.HighPriorityState.InclusiveLowWatermark),
						tag.NewInt64("lowPriorityMinWatermark", minimalAck.LowPriorityState.InclusiveLowWatermark))

					if err = sourceStreamClient.Send(newReq); err != nil {
						if err != io.EOF {
							logger.Error("sourceStreamClient.Send encountered error", tag.Error(err))
						} else {
							logger.Info("sourceStreamClient.Send encountered EOF", tag.Error(err))
						}
						return
					}

					streamTracker.UpdateStream(streamID)
					// Update stream tracker with SyncReplicationState information
					var watermarkTime *time.Time
					if minimalAck.HighPriorityState.InclusiveLowWatermarkTime != nil {
						t := minimalAck.HighPriorityState.InclusiveLowWatermarkTime.AsTime()
						watermarkTime = &t
					}
					streamTracker.UpdateStreamSyncReplicationState(streamID, minimalAck.HighPriorityState.InclusiveLowWatermark, watermarkTime)

				default:
					logger.Error("targetStreamServer.Recv encountered error", tag.Error(serviceerror.NewInternal(fmt.Sprintf(
						"StreamWorkflowReplicationMessages encountered unknown type: %T %v", attr, attr,
					))))
					return
				}

			case <-shutdownChan.Channel():
				// Shutdown requested, exit the loop
				return
			}
		}
	}()

	wg.Wait()

	return nil
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

	if s.Config.ShardCountConfig.Mode == config.ShardCountFixed && s.Config.MemberlistConfig != nil && s.Config.MemberlistConfig.EnableForwarding {
		return s.streamRouting(logger, targetStreamServer, streamTracker, streamID, targetMetadata, clientShardID, serverShardID)
	}

	return s.streamForwarding(logger, targetStreamServer, streamTracker, streamID, targetMetadata, nil)
}

func (s *adminServiceProxyServer) streamForwarding(
	logger log.Logger,
	targetStreamServer adminservice.AdminService_StreamWorkflowReplicationMessagesServer,
	streamTracker *StreamTracker,
	streamID string,
	targetMetadata metadata.MD,
	shutdownChan channel.ShutdownOnce,
) error {
	logger.Info("stream forwarding started")

	outgoingContext := metadata.NewOutgoingContext(targetStreamServer.Context(), targetMetadata)
	outgoingContext, cancel := context.WithCancel(outgoingContext)
	defer cancel()

	sourceStreamClient, err := s.adminClient.StreamWorkflowReplicationMessages(outgoingContext)
	if err != nil {
		logger.Error("remoteAdminServiceClient.StreamWorkflowReplicationMessages encountered error", tag.Error(err))
		return err
	}

	if shutdownChan == nil {
		shutdownChan = channel.NewShutdownOnce()
	}

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
				logger.Info(fmt.Sprintf("forwarding SyncReplicationState: inclusive %v, attr: %v", attr.SyncReplicationState.InclusiveLowWatermark, attr))

				// Update stream tracker with SyncReplicationState information
				var watermarkTime *time.Time
				if attr.SyncReplicationState.InclusiveLowWatermarkTime != nil {
					t := attr.SyncReplicationState.InclusiveLowWatermarkTime.AsTime()
					watermarkTime = &t
				}
				streamTracker.UpdateStreamSyncReplicationState(streamID, attr.SyncReplicationState.InclusiveLowWatermark, watermarkTime)

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
				msg := make([]string, 0, len(attr.Messages.ReplicationTasks))
				for i, task := range attr.Messages.ReplicationTasks {
					msg = append(msg, fmt.Sprintf("[%d]: %v@%v", i, task.SourceTaskId, task.SourceShardId))
				}
				logger.Info(fmt.Sprintf("forwarding ReplicationMessages: exclusive %v, tasks: %v", attr.Messages.ExclusiveHighWatermark, strings.Join(msg, ", ")))

				// Update stream tracker with ReplicationMessages information
				streamTracker.UpdateStreamReplicationMessages(streamID, attr.Messages.ExclusiveHighWatermark)

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
