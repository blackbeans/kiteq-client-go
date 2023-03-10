package client

import (
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/registry"
)

type KiteQClient struct {
	mockModel bool //mock的模式
	k         *kite
}

func (self *KiteQClient) Start() {
	self.k.Start()
}

//设置listner
func (self *KiteQClient) SetListener(listener IListener) {
	self.k.SetListener(listener)
}

func (self *KiteQClient) GetListener() IListener {
	return self.k.GetListener()
}

func NewKiteQClient(zkAddr, groupId, secretKey string) *KiteQClient {
	return NewKiteQClientWithWarmup(zkAddr, groupId, secretKey, 0)
}

//mock的kiteQClient
func NewMockKiteQClient(zkAddr, groupId, secretKey string) *KiteQClient {
	return &KiteQClient{k: &kite{}, mockModel: true}
}

func NewKiteQClientWithWarmup(zkAddr, groupId, secretKey string, warmingupSec int) *KiteQClient {
	return &KiteQClient{
		k: newKite(zkAddr, groupId, secretKey, warmingupSec, NewKiteQListener())}
}

func (self *KiteQClient) SetPreEnv(isPre bool) {
	self.k.isPreEnv = isPre
}

func (self *KiteQClient) SetTopics(topics []string) {
	if !self.mockModel {
		self.k.SetPublishTopics(topics)
	}
}

func (self *KiteQClient) SetBindings(bindings []*registry.Binding) {
	if self.mockModel {
		return
	}
	self.k.SetBindings(bindings)
}

func (self *KiteQClient) SendTxStringMessage(msg *protocol.StringMessage, transcation DoTransaction) error {
	if self.mockModel {
		return nil
	}
	message := protocol.NewQMessage(msg)
	return self.k.SendTxMessage(message, transcation)
}

func (self *KiteQClient) SendTxBytesMessage(msg *protocol.BytesMessage, transcation DoTransaction) error {
	if self.mockModel {
		return nil
	}
	message := protocol.NewQMessage(msg)
	return self.k.SendTxMessage(message, transcation)
}

func (self *KiteQClient) SendStringMessage(msg *protocol.StringMessage) error {
	if self.mockModel {
		return nil
	}
	message := protocol.NewQMessage(msg)
	return self.k.SendMessage(message)
}

func (self *KiteQClient) SendBytesMessage(msg *protocol.BytesMessage) error {
	if self.mockModel {
		return nil
	}
	message := protocol.NewQMessage(msg)
	return self.k.SendMessage(message)
}

func (self *KiteQClient) Destroy() {
	if self.mockModel {
		return
	}
	self.k.Destroy()
}
