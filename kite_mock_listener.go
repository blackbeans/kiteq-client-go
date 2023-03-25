package client

import (
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/registry"
	log "github.com/sirupsen/logrus"
)

type MockListener struct {
}

func (self *MockListener) OnMessage(msg *protocol.QMessage) bool {
	log.Debug("MockListener|OnMessage", msg.GetHeader(), msg.GetBody())
	return true
}

func (self *MockListener) OnMessageCheck(tx *protocol.TxResponse) error {
	log.Debugf("MockListener|OnMessageCheck|%s", tx.MessageId)
	v, _ := tx.GetProperty("tradeno")
	log.Debugf("MockListener|OnMessageCheck|PROP|%s", v)
	tx.Commit()
	return nil
}

func (self *MockListener) RegisteHandler(bind *registry.Binding) IListener {
	return self
}

func (self *MockListener) AddMiddleWares(wares ...MiddleWare) IListener {
	return self
}
