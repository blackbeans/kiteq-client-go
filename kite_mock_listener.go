package client

import (
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/registry"
	log "github.com/blackbeans/log4go"
)

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

func (self *MockListener) AddMiddleWares(wares ...MiddleWare) IListener {
	return self
}
