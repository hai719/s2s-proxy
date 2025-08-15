package proxy

import (
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/channel"
)

type StreamRequestOrResponse interface {
	adminservice.StreamWorkflowReplicationMessagesRequest | adminservice.StreamWorkflowReplicationMessagesResponse
}
type ValueWithError[T StreamRequestOrResponse] struct {
	val *T
	err error
}
type recvable[T StreamRequestOrResponse] interface {
	Recv() (*T, error)
}

// startListener creates a channel of Recv() from the provided source. It is the job of the caller to cancel the context
// that will stop Recv(), or the goroutine created by this will block forever
func startListener[T StreamRequestOrResponse](
	receiver recvable[T],
	shutdownChan channel.ShutdownOnce,
) chan ValueWithError[T] {
	targetStreamServerData := make(chan ValueWithError[T])
	go func() {
		defer close(targetStreamServerData)
		for !shutdownChan.IsShutdown() {
			req, err := receiver.Recv()
			select {
			case targetStreamServerData <- ValueWithError[T]{val: req, err: err}:
			case <-shutdownChan.Channel():
				return
			}
		}
	}()
	return targetStreamServerData
}
