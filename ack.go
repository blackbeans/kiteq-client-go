package client

import (
	"github.com/blackbeans/kiteq-common/protocol"
	log "github.com/blackbeans/log4go"
	"github.com/blackbeans/turbo"

	"time"
)

//ack Event
type AckEvent struct {
	RemoteClient *turbo.TClient
	Opaque       uint32
	event        interface{}
}

type AckHandler struct {
	turbo.BaseForwardHandler
	clientMangager   *turbo.ClientManager
	heartbeatPeriod  time.Duration
	heartbeatTimeout time.Duration
}

//------创建heartbeat
func NewAckHandler(name string, heartbeatPeriod time.Duration,
	heartbeatTimeout time.Duration, clientManager *turbo.ClientManager) *AckHandler {
	phandler := &AckHandler{}
	phandler.BaseForwardHandler = turbo.NewBaseForwardHandler(name, phandler)
	phandler.clientMangager = clientManager
	phandler.heartbeatPeriod = heartbeatPeriod
	phandler.heartbeatTimeout = heartbeatTimeout
	return phandler
}

func (self *AckHandler) heartbeat() {

	for {
		select {
		case <-time.After(self.heartbeatPeriod):
			//心跳检测
			func() {

				clients := self.clientMangager.ClientsClone()

				for h, c := range clients {
					i := 0
					//关闭的时候发起重连
					if c.IsClosed() {
						i = 3
					} else {
						//如果是空闲的则发起心跳
						if c.Idle() {
							for ; i < 3; i++ {
								id := time.Now().Unix()
								p := protocol.MarshalHeartbeatPacket(id)
								hp := turbo.NewPacket(protocol.CMD_HEARTBEAT, p)
								err := c.Ping(hp, time.Duration(self.heartbeatTimeout))
								//如果有错误则需要记录
								if nil != err {
									log.WarnLog("kite", "AckHandler|KeepAlive|FAIL|%s|local:%s|remote:%s|%d\n", err, c.LocalAddr(), h, id)
									continue
								} else {
									log.InfoLog("kite", "AckHandler|KeepAlive|SUCC|local:%s|remote:%s|%d|%d ...\n", c.LocalAddr(), h, id, i)
									break
								}
							}
						}
					}
					if i >= 3 {
						//说明连接有问题需要重连
						c.Shutdown()
						self.clientMangager.SubmitReconnect(c)
						log.WarnLog("kite", "AckHandler|SubmitReconnect|%s", c.RemoteAddr())
					}
				}
			}()
		}
	}

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
