package client

import (
	"strings"

	"github.com/blackbeans/kiteq-common/registry"

	"github.com/blackbeans/kiteq-common/protocol"
	log "github.com/blackbeans/log4go"
	"github.com/blackbeans/turbo"
)

func (self *KiteClientManager) NodeChange(path string, eventType registry.RegistryEvent, children []string) {

	//如果是订阅关系变更则处理
	if strings.HasPrefix(path, registry.KITEQ_SERVER) {
		//获取topic
		split := strings.Split(path, "/")
		if len(split) < 4 {
			//不合法的订阅璐姐
			log.WarnLog("kite_client", "KiteClientManager|ChildWatcher|INVALID SERVER PATH |%s|%t", path, children)
			return
		}
		//获取topic
		topic := split[3]
		log.WarnLog("kite_client", "KiteClientManager|ChildWatcher|Change|%s|%v|%+v", path, children, eventType)
		//search topic
		for _, t := range self.topics {
			if t == topic {
				self.onQServerChanged(topic, children)
				break
			}
		}
	}
}

//当触发QServer地址发生变更
func (self *KiteClientManager) onQServerChanged(topic string, hosts []string) {
	self.lock.Lock()
	defer self.lock.Unlock()
	//重建一下topic下的kiteclient
	clients := make([]*kiteClient, 0, 10)
	for _, host := range hosts {
		//如果能查到remoteClient 则直接复用
		remoteClient := self.clientManager.FindTClient(host)
		if nil == remoteClient {
			//这里就新建一个remote客户端连接
			conn, err := dial(host)
			if nil != err {
				log.ErrorLog("kite_client", "KiteClientManager|onQServerChanged|Create REMOTE CLIENT|FAIL|%s|%s", err, host)
				continue
			}
			remoteClient = turbo.NewTClient(conn, func() turbo.ICodec {
				return protocol.KiteQBytesCodec{
					MaxFrameLength: turbo.MAX_PACKET_BYTES}
			},
				func(ctx *turbo.TContext) error {
					p := ctx.Message
					c := ctx.Client
					event := turbo.NewPacketEvent(c, p)
					err := self.pipeline.FireWork(event)
					if nil != err {
						log.ErrorLog("kite_client", "KiteClientManager|onPacketRecieve|FAIL|%s|%t", err, p)
						return err
					}
					return nil
				}, self.config)
			remoteClient.Start()
			auth, err := handshake(self.ga, remoteClient)
			if !auth || nil != err {
				remoteClient.Shutdown()
				log.ErrorLog("kite_client", "KiteClientManager|onQServerChanged|HANDSHAKE|FAIL|%s|%s", err, auth)
				continue
			}
			self.clientManager.Auth(self.ga, remoteClient)
		} else if remoteClient.IsClosed() {
			//如果当前是关闭的状态，那么就会自动重连，不需要创建新的连接
			log.InfoLog("kite_client", "KiteClientManager|onQServerChanged|Closed|Wait Reconnect|%s|%s", topic, hosts)
		}

		//创建kiteClient
		kiteClient := newKitClient(remoteClient)
		clients = append(clients, kiteClient)
	}

	log.InfoLog("kite_client", "KiteClientManager|onQServerChanged|SUCC|%s|%s", topic, hosts)

	//替换掉线的server
	old, ok := self.kiteClients[topic]
	self.kiteClients[topic] = clients
	if ok {
		del := make([]string, 0, 2)
	outter:
		for _, o := range old {
			//决定删除的时候必须把所有的当前对应的client遍历一遍不然会删除掉
			for _, clients := range self.kiteClients {
				for _, c := range clients {
					if c.client.RemoteAddr() == o.client.RemoteAddr() {
						continue outter
					}
				}
			}
			del = append(del, o.client.RemoteAddr())
		}
		//需要删掉已经废弃的连接
		if len(del) > 0 {
			self.clientManager.DeleteClients(del...)
		}
	}
}

func (self *KiteClientManager) DataChange(path string, binds []*registry.Binding) {
	//IGNORE
	log.InfoLog("kite_client", "KiteClientManager|DataChange|%s|%s", path, binds)
}

func (self *KiteClientManager) OnSessionExpired() {
	//推送订阅关系和topics
	self.Start()

	log.InfoLog("kite_client", "KiteClientManager|OnSessionExpired|Restart...")
}
