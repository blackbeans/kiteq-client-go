package client

import (
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/registry"
	log "github.com/blackbeans/log4go"
)

type IListener interface {
	//接受投递消息的回调
	OnMessage(msg *protocol.QMessage) bool
	//接收事务回调
	// 除非明确提交成功、其余都为不成功
	// 有异常或者返回值为false均为不提交
	OnMessageCheck(tx *protocol.TxResponse) error

	//注册下handler，使用bingding的形式来
	RegisteHandler(bind *registry.Binding) IListener
}

type MockListener struct {
}

func (self *MockListener) OnMessage(msg *protocol.QMessage) bool {
	log.DebugLog("kite", "MockListener|OnMessage", msg.GetHeader(), msg.GetBody())
	return true
}

func (self *MockListener) OnMessageCheck(tx *protocol.TxResponse) error {
	log.Debug("MockListener|OnMessageCheck|%s\n", tx.MessageId)
	v, _ := tx.GetProperty("tradeno")
	log.DebugLog("kite", "MockListener|OnMessageCheck|PROP|%s\n", v)
	tx.Commit()
	return nil
}

func (self *MockListener) RegisteHandler(bind *registry.Binding) IListener {
	return self
}
