package client

import (
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/log4go"
	"github.com/golang/protobuf/proto"
	"time"
)

//
//构建BytesMessage
//
func BuildByteMessage(topic, messageType string, body []byte, commit bool) *protocol.BytesMessage {
	//创建消息
	entity := &protocol.BytesMessage{}
	entity.Header = &protocol.Header{
		//默认开启了压缩
		Snappy:       proto.Bool(true),
		MessageId:    proto.String(MessageId()),
		Topic:        proto.String(topic),
		MessageType:  proto.String(messageType),
		ExpiredTime:  proto.Int64(-1),
		DeliverLimit: proto.Int32(100),
		GroupId:      proto.String(""),
		Commit:       proto.Bool(commit),
		Fly:          proto.Bool(false),
		CreateTime:   proto.Int64(time.Now().Unix())}

	entity.Body = body
	return entity
}

//发送的信息
func SendBytesMessage(kiteqClient *KiteQClient, body []byte, topic string, messageType string) bool {
	//向kiteQ 发送用户基础信息
	err := kiteqClient.SendBytesMessage(BuildByteMessage(topic,
		messageType, body, true))
	if nil != err {
		log4go.ErrorLog("kite", "SendBytesMessage|SendBytesMessage|FAIL|%v|msgType:%s", err, messageType)
		return false
	}
	return true
}
