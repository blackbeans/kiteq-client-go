package client

import (
	"encoding/base64"
	"errors"
	"fmt"
	log "github.com/sirupsen/logrus"
	"runtime/debug"

	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/turbo"
)

//接受消息事件
type acceptEvent struct {
	turbo.IForwardEvent
	msgType      uint8
	msg          interface{} //attach的数据message
	remoteClient *turbo.TClient
	opaque       uint32
}

func newAcceptEvent(msgType uint8, msg interface{}, remoteClient *turbo.TClient, opaque uint32) *acceptEvent {
	return &acceptEvent{
		msgType:      msgType,
		msg:          msg,
		opaque:       opaque,
		remoteClient: remoteClient}
}

//--------------------如下为具体的处理Handler
type AcceptHandler struct {
	turbo.BaseForwardHandler
	listener IListener
}

func NewAcceptHandler(name string, listener IListener) *AcceptHandler {
	ahandler := &AcceptHandler{}
	ahandler.BaseForwardHandler = turbo.NewBaseForwardHandler(name, ahandler)
	ahandler.listener = listener
	return ahandler
}

func (self *AcceptHandler) TypeAssert(event turbo.IEvent) bool {
	_, ok := self.cast(event)
	return ok
}

func (self *AcceptHandler) cast(event turbo.IEvent) (val *acceptEvent, ok bool) {
	val, ok = event.(*acceptEvent)

	return
}

var INVALID_MSG_TYPE_ERROR = errors.New("INVALID MSG TYPE !")

func (self *AcceptHandler) Process(ctx *turbo.DefaultPipelineContext, event turbo.IEvent) error {
	// log.DebugLog("kite","AcceptHandler|Process|%s|%t", self.GetName(), event)

	acceptEvent, ok := self.cast(event)
	if !ok {
		return turbo.ERROR_INVALID_EVENT_TYPE
	}

	switch acceptEvent.msgType {
	case protocol.CMD_TX_ACK:

		//回调事务完成的监听器
		// log.DebugLog("kite","AcceptHandler|Check Message|%t", acceptEvent.Msg)
		txPacket := acceptEvent.msg.(*protocol.TxACKPacket)
		header := txPacket.GetHeader()
		tx := protocol.NewTxResponse(header)
		err := self.listener.OnMessageCheck(tx)
		if nil != err {
			tx.Unknown(err.Error())
		}
		//发起一个向后的处理时间发送出去
		//填充条件
		tx.ConvertTxAckPacket(txPacket)

		txData, _ := protocol.MarshalPbMessage(txPacket)

		txResp := turbo.NewRespPacket(acceptEvent.opaque, acceptEvent.msgType, txData)

		//发送提交结果确认的Packet
		remotingEvent := turbo.NewRemotingEvent(txResp, []string{acceptEvent.remoteClient.RemoteAddr()})
		ctx.SendForward(remotingEvent)
		// log.DebugLog("kite","AcceptHandler|Recieve TXMessage|%t", acceptEvent.Msg)

	case protocol.CMD_STRING_MESSAGE, protocol.CMD_BYTES_MESSAGE:
		//这里应该回调消息监听器然后发送处理结果
		//log.DebugLog("kite","AcceptHandler|Recieve Message|%t", acceptEvent.Msg)

		message := protocol.NewQMessage(acceptEvent.msg)

		//或者解压
		if message.GetHeader().GetSnappy() {
			switch message.GetMsgType() {
			case protocol.CMD_BYTES_MESSAGE:

				data, err := Decompress(message.GetBody().([]byte))
				if nil != err {
					log.Errorf("AcceptHandler|CMD_BYTES_MESSAGE|Body Decompress|FAIL|%s|%s", err, string(message.GetBody().([]byte)))
					//如果解压失败那么采用不解压
					data = message.GetBody().([]byte)
				}
				message.Body = data
			case protocol.CMD_STRING_MESSAGE:
				data, err := base64.StdEncoding.DecodeString(message.GetBody().(string))
				if nil != err {
					return fmt.Errorf("DecodeString Base64 String Fail %v", err)
				}
				data, err = Decompress(data)
				if nil != err {
					log.Errorf("AcceptHandler|CMD_STRING_MESSAGE|Body Decompress|FAIL|%s|%s", err, string(message.GetBody().([]byte)))
					//如果解压失败那么采用不解压
					data = message.GetBody().([]byte)
				}
				message.Body = string(data)
			}
		}

		var err error
		succ := false
		func() {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("%v", r)
					log.Errorf("AcceptHandler|Recover|%v|%v", r, string(debug.Stack()))
				}
			}()
			succ = self.listener.OnMessage(message)
		}()

		dpacket := protocol.MarshalDeliverAckPacket(message.GetHeader(), succ, err)

		respPacket := turbo.NewRespPacket(acceptEvent.opaque, protocol.CMD_DELIVER_ACK, dpacket)

		remotingEvent := turbo.NewRemotingEvent(respPacket, []string{acceptEvent.remoteClient.RemoteAddr()})

		ctx.SendForward(remotingEvent)

	default:
		return INVALID_MSG_TYPE_ERROR
	}

	return nil

}
