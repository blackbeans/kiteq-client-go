package client

import (
	"context"
	"errors"

	"github.com/blackbeans/kiteq-common/protocol"

	"github.com/blackbeans/turbo"
)

//远程操作的PacketHandler

type UnmarshalHandler struct {
	turbo.BaseForwardHandler
	//使用的gopool
	defaultPool *turbo.GPool
	pools       map[uint8]*turbo.GPool
}

func NewUnmarshalHandler(name string, pools map[uint8]*turbo.GPool, defaultPool *turbo.GPool) *UnmarshalHandler {
	packetHandler := &UnmarshalHandler{}
	packetHandler.BaseForwardHandler = turbo.NewBaseForwardHandler(name, packetHandler)
	packetHandler.pools = pools
	packetHandler.defaultPool = defaultPool
	return packetHandler

}

func (self *UnmarshalHandler) TypeAssert(event turbo.IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *UnmarshalHandler) cast(event turbo.IEvent) (val *turbo.PacketEvent, ok bool) {
	val, ok = event.(*turbo.PacketEvent)

	return
}

var INVALID_PACKET_ERROR = errors.New("INVALID PACKET ERROR")

func (self *UnmarshalHandler) Process(ctx *turbo.DefaultPipelineContext, event turbo.IEvent) error {

	// log.DebugLog("kite","UnmarshalHandler|Process|%s|%t", self.GetName(), event)

	pevent, ok := self.cast(event)
	if !ok {
		return turbo.ERROR_INVALID_EVENT_TYPE
	}

	cevent, err := self.handlePacket(pevent)
	if nil != err {
		return err
	}

	if p, ok := self.pools[pevent.Packet.Header.CmdType]; ok {
		p.Queue(context.TODO(), func(tx context.Context) (interface{}, error) {
			ctx.SendForward(cevent)
			return nil, nil
		})
	} else {
		self.defaultPool.Queue(context.TODO(), func(tx context.Context) (interface{}, error) {
			ctx.SendForward(cevent)
			return nil, nil
		})
	}

	return nil
}

var eventSunk = &turbo.SunkEvent{}

//对于请求事件
func (self *UnmarshalHandler) handlePacket(pevent *turbo.PacketEvent) (turbo.IEvent, error) {
	var err error
	var event turbo.IEvent
	packet := pevent.Packet
	//根据类型反解packet
	switch packet.Header.CmdType {
	//连接授权确认
	case protocol.CMD_CONN_AUTH:
		var auth protocol.ConnAuthAck
		err = protocol.UnmarshalPbMessage(packet.Data, &auth)
		if nil == err {
			event = &AckEvent{
				event:        &auth,
				RemoteClient: pevent.RemoteClient,
				Opaque:       packet.Header.Opaque,
			}
		}
	//心跳
	case protocol.CMD_HEARTBEAT:
		var hearbeat protocol.HeartBeat
		err = protocol.UnmarshalPbMessage(packet.Data, &hearbeat)
		if nil == err {
			event = &AckEvent{
				event:        &hearbeat,
				RemoteClient: pevent.RemoteClient,
				Opaque:       packet.Header.Opaque,
			}
		}
		//消息持久化
	case protocol.CMD_MESSAGE_STORE_ACK:
		var pesisteAck protocol.MessageStoreAck
		err = protocol.UnmarshalPbMessage(packet.Data, &pesisteAck)
		if nil == err {
			event = &AckEvent{
				event:        &pesisteAck,
				RemoteClient: pevent.RemoteClient,
				Opaque:       packet.Header.Opaque,
			}
		}

	case protocol.CMD_TX_ACK:
		var txAck protocol.TxACKPacket
		err = protocol.UnmarshalPbMessage(packet.Data, &txAck)
		if nil == err {
			event = newAcceptEvent(protocol.CMD_TX_ACK, &txAck, pevent.RemoteClient, packet.Header.Opaque)
		}
	//发送的是bytesmessage
	case protocol.CMD_BYTES_MESSAGE:
		var msg protocol.BytesMessage
		err = protocol.UnmarshalPbMessage(packet.Data, &msg)
		if nil == err {
			event = newAcceptEvent(protocol.CMD_BYTES_MESSAGE, &msg, pevent.RemoteClient, packet.Header.Opaque)
		}
	//发送的是StringMessage
	case protocol.CMD_STRING_MESSAGE:
		var msg protocol.StringMessage
		err = protocol.UnmarshalPbMessage(packet.Data, &msg)
		if nil == err {
			event = newAcceptEvent(protocol.CMD_STRING_MESSAGE, &msg, pevent.RemoteClient, packet.Header.Opaque)
		}
	}

	if nil == event {
		event = eventSunk
	}

	return event, err

}
