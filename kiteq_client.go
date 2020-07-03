package client

import (
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/registry"
)

type KiteQClient struct {
	k *kite
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

func NewKiteQClientWithWarmup(zkAddr, groupId, secretKey string, warmingupSec int) *KiteQClient {
	return &KiteQClient{
		k: newKite(zkAddr, groupId, secretKey, warmingupSec, NewKiteQListener())}
}

func (self *KiteQClient) SetTopics(topics []string) {
	self.k.SetPublishTopics(topics)
}

func (self *KiteQClient) SetBindings(bindings []*registry.Binding) {
	self.k.SetBindings(bindings)

}

func (self *KiteQClient) SendTxStringMessage(msg *protocol.StringMessage, transcation DoTransaction) error {
	message := protocol.NewQMessage(msg)
	return self.k.SendTxMessage(message, transcation)
}

func (self *KiteQClient) SendTxBytesMessage(msg *protocol.BytesMessage, transcation DoTransaction) error {
	message := protocol.NewQMessage(msg)
	return self.k.SendTxMessage(message, transcation)
}

func (self *KiteQClient) SendStringMessage(msg *protocol.StringMessage) error {
	message := protocol.NewQMessage(msg)
	return self.k.SendMessage(message)
}

func (self *KiteQClient) SendBytesMessage(msg *protocol.BytesMessage) error {
	message := protocol.NewQMessage(msg)
	return self.k.SendMessage(message)
}

func (self *KiteQClient) Destory() {
	self.k.Destroy()
}
