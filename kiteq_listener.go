package client

import (
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/registry"
	"github.com/blackbeans/log4go"
)

//
type IListener interface {
	//接受投递消息的回调
	OnMessage(msg *protocol.QMessage) bool
	//接收事务回调
	// 除非明确提交成功、其余都为不成功
	// 有异常或者返回值为false均为不提交
	OnMessageCheck(tx *protocol.TxResponse) error

	//注册下handler，使用bingding的形式来
	RegisteHandler(bind *registry.Binding) IListener

	//增加消息处理的中间件
	AddMiddleWares(wares ...MiddleWare) IListener
}

//消息处理上下文
type Context struct {
	Msg *protocol.QMessage
	//处理的error
	Err error
	//接收成功
	Success bool
}

//处理链
type Chain func(ctx *Context) error

type MiddleWare func(next Chain) Chain

//处理器
type Handler func(body interface{}) (bool, error)

//kiteqListener
type KiteQListener struct {
	IListener
	wares    []MiddleWare
	handlers []*registry.Binding
}

//创建KiteQListener
func NewKiteQListener() *KiteQListener {
	return &KiteQListener{handlers: make([]*registry.Binding, 0, 5)}
}

//增加中间件
func (self *KiteQListener) AddMiddleWares(wares ...MiddleWare) {
	self.wares = wares
}

func (self *KiteQListener) RegisteHandler(bind *registry.Binding) IListener {
	if nil != bind.Handler {
		self.handlers = append(self.handlers, bind)
	}
	return self
}

func (self *KiteQListener) onFire(ctx *Context) error {

	next := func(ctx *Context) error {
		msg := ctx.Msg
		topic := msg.GetHeader().GetTopic()
		messageType := msg.GetHeader().GetMessageType()
		for _, bind := range self.handlers {
			if bind.Matches(topic, messageType) {
				ctx.Success, ctx.Err = bind.Handler(msg.GetBody())
				if nil != ctx.Err || !ctx.Success {
					log4go.ErrorLog("kite", "OnMessage|Handle|FAIL|%s|%s|%s|%s|%v",
						msg.GetHeader().GetMessageId(),
						msg.GetHeader().GetTopic(),
						msg.GetHeader().GetMessageType(),
						msg.GetHeader().GetProperties(), ctx.Err)
					return nil
				}
				return nil
			}
		}
		//不存在这样的消息处理器
		log4go.WarnLog("kite", "OnMessage|Handle|FAIL|NO Handler|%s|%s|%s|%s",
			msg.GetHeader().GetMessageId(),
			msg.GetHeader().GetTopic(),
			msg.GetHeader().GetMessageType(),
			msg.GetHeader().GetProperties())
		return nil
	}

	//开始进行处理链处理
	if nil != self.wares {
		for _, c := range self.wares {
			next = c(next)
		}
	}
	return next(ctx)
}

//接受投递消息的回调
func (self *KiteQListener) OnMessage(msg *protocol.QMessage) bool {

	ctx := &Context{
		Msg: msg,
	}
	err := self.onFire(ctx)
	if nil != err {
		return false
	}

	if nil != ctx.Err || !ctx.Success {
		log4go.ErrorLog("kite", "OnMessage|Handle|FAIL|%s|%s|%s|%s|%v",
			msg.GetHeader().GetMessageId(),
			msg.GetHeader().GetTopic(),
			msg.GetHeader().GetMessageType(),
			msg.GetHeader().GetProperties(), err)
		return false
	}

	return ctx.Success

}

//接收事务回调
// 除非明确提交成功、其余都为不成功
// 有异常或者返回值为false均为不提交
func (self *KiteQListener) OnMessageCheck(tx *protocol.TxResponse) error {
	//TODO 暂时没有2PC的消息
	tx.Commit()
	return nil
}
