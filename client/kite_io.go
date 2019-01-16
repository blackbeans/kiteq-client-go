package client

import (
	"encoding/base64"
	"errors"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"

	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/turbo"
)

type kiteIO struct {
	client *turbo.TClient
}

func newKiteIO(c *turbo.TClient) *kiteIO {

	client := &kiteIO{
		client: c}

	return client
}

//发送事务的确认,无需等待服务器反馈
func (self *kiteIO) sendTxAck(message *protocol.QMessage,
	txstatus protocol.TxStatus, feedback string) error {
	//写入时间
	if message.GetHeader().GetCreateTime() <= 0 {
		message.GetHeader().CreateTime = protocol.MarshalInt64(time.Now().Unix())
	}
	txpacket := protocol.MarshalTxACKPacket(message.GetHeader(), txstatus, feedback)
	return self.innerSendMessage(protocol.CMD_TX_ACK, txpacket, 0)
}

func (self *kiteIO) sendMessage(message *protocol.QMessage) error {
	//写入时间
	if message.GetHeader().GetCreateTime() <= 0 {
		message.GetHeader().CreateTime = protocol.MarshalInt64(time.Now().Unix())
	}

	//snappy
	if message.GetHeader().GetSnappy() {

		switch message.GetMsgType() {
		case protocol.CMD_BYTES_MESSAGE:
			compress, err := Compress(message.GetBody().([]byte))
			if nil != err {
				return err
			}
			bytesMessage := message.GetPbMessage().(*protocol.BytesMessage)
			bytesMessage.Body = compress
			message = protocol.NewQMessage(bytesMessage)

		case protocol.CMD_STRING_MESSAGE:
			compress, err := Compress([]byte(message.GetBody().(string)))
			if nil != err {
				return err
			}
			stringMessage := message.GetPbMessage().(*protocol.StringMessage)
			stringMessage.Body = proto.String(base64.StdEncoding.EncodeToString(compress))
			message = protocol.NewQMessage(stringMessage)
		}
	}

	data, err := protocol.MarshalPbMessage(message.GetPbMessage())
	if nil != err {
		return err
	}
	timeout := 3 * time.Second
	return self.innerSendMessage(message.GetMsgType(), data, timeout)
}

var TIMEOUT_ERROR = errors.New("WAIT RESPONSE TIMEOUT ")

func (self *kiteIO) innerSendMessage(cmdType uint8, p []byte, timeout time.Duration) error {

	msgpacket := turbo.NewPacket(cmdType, p)

	//如果是需要等待结果的则等待
	if timeout <= 0 {
		_, err := self.client.Write(*msgpacket)
		return err
	} else {
		resp, err := self.client.WriteAndGet(*msgpacket, timeout)
		if nil != err {
			return err
		} else {
			storeAck, ok := resp.(*protocol.MessageStoreAck)
			if !ok || !storeAck.GetStatus() {
				return errors.New(fmt.Sprintf("kiteIO|SendMessage|FAIL|%s\n", resp))
			} else {
				//log.DebugLog("kite_client","kiteIO|SendMessage|SUCC|%s|%s\n", storeAck.GetMessageId(), storeAck.GetFeedback())
				return nil
			}
		}
	}
}
