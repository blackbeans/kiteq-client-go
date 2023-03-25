package client

import (
	"context"
	"fmt"
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/registry"
	"github.com/golang/protobuf/proto"
	"testing"
	"time"
)

func buildStringMessage(commit bool) *protocol.StringMessage {
	//创建消息
	entity := &protocol.StringMessage{}
	entity.Header = &protocol.Header{
		Snappy:       proto.Bool(true),
		MessageId:    proto.String(MessageId()),
		Topic:        proto.String("user-profile"),
		MessageType:  proto.String("profile-update"),
		ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
		DeliverLimit: proto.Int32(-1),
		GroupId:      proto.String("s-user-profile"),
		Commit:       proto.Bool(commit),
		Fly:          proto.Bool(false)}
	entity.Body = proto.String("hello go-kite")

	return entity
}

func buildBytesMessage(commit bool) *protocol.BytesMessage {
	//创建消息
	entity := &protocol.BytesMessage{}
	entity.Header = &protocol.Header{
		Snappy:       proto.Bool(true),
		MessageId:    proto.String(MessageId()),
		Topic:        proto.String("user-profile"),
		MessageType:  proto.String("profile-update"),
		ExpiredTime:  proto.Int64(time.Now().Add(10 * time.Minute).Unix()),
		DeliverLimit: proto.Int32(-1),
		GroupId:      proto.String("s-user-profile"),
		Commit:       proto.Bool(commit),
		Fly:          proto.Bool(false)}
	entity.Body = []byte("helloworld")

	return entity
}

type MockTestListener struct {
	rc  chan string
	txc chan string
}

func (self *MockTestListener) RegisteHandler(bind *registry.Binding) IListener {
	//TODO implement me
	//panic("implement me")
	return self
}

func (self *MockTestListener) AddMiddleWares(wares ...MiddleWare) IListener {
	//TODO implement me
	//panic("implement me")
	return self
}

func (self *MockTestListener) OnMessage(msg *protocol.QMessage) bool {
	fmt.Printf("MockTestListener|OnMessage|%+v|%s", msg.GetHeader(), msg.GetBody())
	self.rc <- msg.GetHeader().GetMessageId()

	return true
}

func (self *MockTestListener) OnMessageCheck(tx *protocol.TxResponse) error {
	fmt.Printf("MockTestListener|OnMessageCheck|%s", tx.MessageId)
	self.txc <- tx.MessageId
	tx.Commit()
	return nil
}

var rc = make(chan string, 1)
var txc = make(chan string, 1)
var manager *kite

func init() {

	// 创建客户端
	manager = newKite(context.TODO(), "file://./registry_test.yaml", "s-user-profile", "123456", 5, &MockTestListener{rc: rc, txc: txc})
	manager.SetPublishTopics([]string{"user-profile"})
	// 设置接收类型
	manager.SetBindings(
		[]*registry.Binding{
			registry.Bind_Direct("s-user-profile", "user-profile", "profile-update", 1000, true),
		},
	)

	time.Sleep(10 * time.Second)
	manager.Start()
}

func TestStringMessage(t *testing.T) {

	m := buildStringMessage(true)
	// 发送数据
	err := manager.SendMessage(protocol.NewQMessage(m))
	if nil != err {
		t.Logf("SEND StringMESSAGE |FAIL|%v", err)
	} else {
		t.Log("SEND StringMESSAGE |SUCCESS")
	}

	select {
	case mid := <-rc:
		t.Logf("RECEIVE StringMESSAGE |SUCCESS|%s", mid)
	case <-time.After(10 * time.Second):
		t.Logf("WAIT StringMESSAGE |TIMEOUT|%v", err)
		t.Fail()

	}

}

func TestBytesMessage(t *testing.T) {

	bm := buildBytesMessage(true)
	// 发送数据
	err := manager.SendMessage(protocol.NewQMessage(bm))
	if nil != err {
		t.Logf("SEND BytesMESSAGE |FAIL|%v", err)
	} else {
		t.Log("SEND BytesMESSAGE |SUCCESS")
	}

	select {
	case mid := <-rc:
		if mid != bm.GetHeader().GetMessageId() {
			t.Fail()
		}
		t.Log("RECEIVE BytesMESSAGE |SUCCESS")
	case <-time.After(10 * time.Second):
		t.Log("WAIT BytesMESSAGE |TIMEOUT|", err)
		t.Fail()

	}

}

func TestTxBytesMessage(t *testing.T) {

	bm := buildBytesMessage(false)

	// 发送数据
	err := manager.SendTxMessage(protocol.NewQMessage(bm),
		func(message *protocol.QMessage) (bool, error) {
			return true, nil
		})
	if nil != err {
		t.Log("SEND TxBytesMESSAGE |FAIL|", err)
	} else {
		t.Log("SEND TxBytesMESSAGE |SUCCESS")
	}

	select {
	case mid := <-rc:
		t.Log("RECEIVE TxBytesMESSAGE |SUCCESS", mid)
	case txid := <-txc:
		if txid != bm.GetHeader().GetMessageId() {
			t.Fail()
			t.Log("SEND TxBytesMESSAGE |RECEIVE TXACK SUCC")
		}

	case <-time.After(10 * time.Second):
		t.Log("WAIT TxBytesMESSAGE |TIMEOUT")
		t.Fail()

	}
}
