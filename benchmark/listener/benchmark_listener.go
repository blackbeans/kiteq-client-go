package listener

import (
	"fmt"
	client "github.com/blackbeans/kiteq-client-go"
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/registry"
	"log"
	"sync/atomic"
	"time"
)

type DefaultListener struct {
	client.IListener
	count int32
	lc    int32
}

func (self *DefaultListener) Monitor() {
	for {
		tmp := self.count
		ftmp := self.lc

		time.Sleep(1 * time.Second)
		fmt.Printf("tps:%d\n", (tmp - ftmp))
		self.lc = tmp
	}
}

func (self *DefaultListener) OnMessage(msg *protocol.QMessage) bool {
	//	log.Info("DefaultListener|OnMessage|%s", msg.GetHeader().GetMessageId())
	atomic.AddInt32(&self.count, 1)
	return true
}

func (self *DefaultListener) OnMessageCheck(tx *protocol.TxResponse) error {
	log.Printf("DefaultListener|OnMessageCheck", tx.MessageId)
	tx.Commit()
	return nil
}

//注册下handler，使用bingding的形式来
func (self *DefaultListener) RegisteHandler(bind *registry.Binding) client.IListener {
	return self
}
