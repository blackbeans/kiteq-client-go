package client

import (
	"context"
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/registry"
	"github.com/blackbeans/logx"
)

var log = logx.GetLogger("kiteq_client")

type KiteQClient struct {
	mockModel bool //mock的模式
	k         *kite
}

func (kq *KiteQClient) Start() {
	kq.k.Start()
}

//设置listner
func (kq *KiteQClient) SetListener(listener IListener) {
	kq.k.SetListener(listener)
}

func (kq *KiteQClient) GetListener() IListener {
	return kq.k.GetListener()
}

func NewKiteQClient(parentCtx context.Context, zkAddr, groupId, secretKey string) *KiteQClient {
	return NewKiteQClientWithWarmup(parentCtx, zkAddr, groupId, secretKey, 0)
}

//mock的kiteQClient
func NewMockKiteQClient(registryAddr, groupId, secretKey string) *KiteQClient {
	return &KiteQClient{k: &kite{}, mockModel: true}
}

func NewKiteQClientWithWarmup(parentCtx context.Context, registryAddr, groupId, secretKey string, warmingupSec int) *KiteQClient {
	return &KiteQClient{
		k: newKite(parentCtx, registryAddr, groupId, secretKey, warmingupSec, NewKiteQListener())}
}

func (kq *KiteQClient) SetPreEnv(isPre bool) {
	kq.k.isPreEnv = isPre
}

func (kq *KiteQClient) SetTopics(topics []string) {
	if !kq.mockModel {
		kq.k.SetPublishTopics(topics)
	}
}

func (kq *KiteQClient) SetBindings(bindings []*registry.Binding) {
	if kq.mockModel {
		return
	}
	kq.k.SetBindings(bindings)
}

func (kq *KiteQClient) SendTxStringMessage(msg *protocol.StringMessage, transcation DoTransaction) error {
	if kq.mockModel {
		return nil
	}
	message := protocol.NewQMessage(msg)
	return kq.k.SendTxMessage(message, transcation)
}

func (kq *KiteQClient) SendTxBytesMessage(msg *protocol.BytesMessage, transcation DoTransaction) error {
	if kq.mockModel {
		return nil
	}
	message := protocol.NewQMessage(msg)
	return kq.k.SendTxMessage(message, transcation)
}

func (kq *KiteQClient) SendStringMessage(msg *protocol.StringMessage) error {
	if kq.mockModel {
		return nil
	}
	message := protocol.NewQMessage(msg)
	return kq.k.SendMessage(message)
}

func (kq *KiteQClient) SendBytesMessage(msg *protocol.BytesMessage) error {
	if kq.mockModel {
		return nil
	}
	message := protocol.NewQMessage(msg)
	return kq.k.SendMessage(message)
}

func (kq *KiteQClient) Destroy() {
	if kq.mockModel {
		return
	}
	kq.k.Destroy()
}
