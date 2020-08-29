package client

import (
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/turbo"
)

//ack Event
type AckEvent struct {
	RemoteClient *turbo.TClient
	Opaque       uint32
	event        interface{}
}

type AckHandler struct {
	turbo.BaseForwardHandler
	clientMangager *turbo.ClientManager
}

//------创建heartbeat
func NewAckHandler(name string, clientManager *turbo.ClientManager) *AckHandler {
	phandler := &AckHandler{}
	phandler.BaseForwardHandler = turbo.NewBaseForwardHandler(name, phandler)
	phandler.clientMangager = clientManager
	return phandler
}

func (self *AckHandler) TypeAssert(event turbo.IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *AckHandler) cast(event turbo.IEvent) (*AckEvent, bool) {
	v, ok := event.(*AckEvent)
	return v, ok
}

func (self *AckHandler) Process(ctx *turbo.DefaultPipelineContext, event turbo.IEvent) error {

	ack, ok := self.cast(event)
	if !ok {
		return turbo.ERROR_INVALID_EVENT_TYPE
	}
	if h, ok := ack.event.(*protocol.HeartBeat); ok {
		ack.RemoteClient.Attach(ack.Opaque, *h.Version)
	} else {
		ack.RemoteClient.Attach(ack.Opaque, ack.event)
	}

	return nil
}
