package client

import (
	"github.com/blackbeans/kiteq-common/protocol"
	log "github.com/blackbeans/log4go"
	"github.com/blackbeans/turbo"

	"time"
)

type HeartbeatHandler struct {
	turbo.BaseForwardHandler
	clientMangager   *turbo.ClientManager
	heartbeatPeriod  time.Duration
	heartbeatTimeout time.Duration
}

//------创建heartbeat
func NewHeartbeatHandler(name string, heartbeatPeriod time.Duration,
	heartbeatTimeout time.Duration, clientManager *turbo.ClientManager) *HeartbeatHandler {
	phandler := &HeartbeatHandler{}
	phandler.BaseForwardHandler = turbo.NewBaseForwardHandler(name, phandler)
	phandler.clientMangager = clientManager
	phandler.heartbeatPeriod = heartbeatPeriod
	phandler.heartbeatTimeout = heartbeatTimeout
	go phandler.keepAlive()
	return phandler
}

func (self *HeartbeatHandler) keepAlive() {

	for {
		select {
		case <-time.After(self.heartbeatPeriod):
			//心跳检测
			func() {
				id := time.Now().Unix()
				clients := self.clientMangager.ClientsClone()
				p := protocol.MarshalHeartbeatPacket(id)
				for h, c := range clients {
					i := 0
					//关闭的时候发起重连
					if c.IsClosed() {
						i = 3
					} else {
						//如果是空闲的则发起心跳
						if c.Idle() {
							for ; i < 3; i++ {
								hp := turbo.NewPacket(protocol.CMD_HEARTBEAT, p)
								err := c.Ping(hp, time.Duration(self.heartbeatTimeout))
								//如果有错误则需要记录
								if nil != err {
									log.WarnLog("kite_client_handler", "HeartbeatHandler|KeepAlive|FAIL|%s|local:%s|remote:%s|%d\n", err, c.LocalAddr(), h, id)
									continue
								} else {
									log.InfoLog("kite_client_handler", "HeartbeatHandler|KeepAlive|SUCC|local:%s|remote:%s|%d|%d ...\n", c.LocalAddr(), h, id, i)
									break
								}
							}
						}
					}
					if i >= 3 {
						//说明连接有问题需要重连
						c.Shutdown()
						self.clientMangager.SubmitReconnect(c)
						log.WarnLog("kite_client_handler", "HeartbeatHandler|SubmitReconnect|%s\n", c.RemoteAddr())
					}
				}
			}()
		}
	}

}

func (self *HeartbeatHandler) TypeAssert(event turbo.IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *HeartbeatHandler) cast(event turbo.IEvent) (val *turbo.HeartbeatEvent, ok bool) {
	val, ok = event.(*turbo.HeartbeatEvent)
	return
}

func (self *HeartbeatHandler) Process(ctx *turbo.DefaultPipelineContext, event turbo.IEvent) error {

	hevent, ok := self.cast(event)
	if !ok {
		return turbo.ERROR_INVALID_EVENT_TYPE
	}

	// log.DebugLog("kite_client_handler","HeartbeatHandler|%s|Process|Recieve|Pong|%s|%d\n", self.GetName(), hevent.RemoteClient.RemoteAddr(), hevent.Version)
	hevent.RemoteClient.Attach(hevent.Opaque, hevent.Version)
	return nil
}
