package client

import (
	"context"
	"errors"
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/registry"
	"github.com/blackbeans/turbo"
	log "github.com/sirupsen/logrus"
	"strings"
)

func (self *kite) NodeChange(path string, eventType registry.RegistryEvent, children []string) {

	//如果是订阅关系变更则处理
	if strings.HasPrefix(path, registry.KITEQ_SERVER) {
		//获取topic
		split := strings.Split(path, "/")
		if len(split) < 4 {
			//不合法的订阅璐姐
			log.Warnf("kite|ChildWatcher|INVALID SERVER PATH |%s|%v", path, children)
			return
		}
		//获取topic
		topic := split[3]
		log.Warnf("kite|ChildWatcher|Change|%s|%v|%v", path, children, eventType)
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
func (self *kite) onQServerChanged(topic string, hosts []string) {
	//重建一下topic下的kiteclient
	addresses := make([]string, 0, 10)
	for _, host := range hosts {
		//如果能查到remoteClient 则直接复用
		newHost := host
		newFutureTask := turbo.NewFutureTask(self.ctx, func(ctx context.Context) (interface{}, error) {
			return self.onTClientInit(newHost)
		})
		_, loaded := self.addressToTClient.LoadOrStore(host, newFutureTask)

		//不存在这个任务，那么使用的是创建的这个任务
		if !loaded {
			//执行运行一下
			newFutureTask.Run()
		}
		addresses = append(addresses, host)
	}

	log.Infof("kite|onQServerChanged|SUCC|%s|%s", topic, hosts)

	//替换掉线的server
	_, loaded := self.topicToAddress.LoadOrStore(topic, addresses)
	if loaded {
		//放入新的地址列表
		self.topicToAddress.Store(topic, addresses)
	} else {
		//说明没有旧的
	}

	//目前使用的链接地址
	usingAddr := make(map[string]interface{}, 10)
	self.topicToAddress.Range(func(key, value interface{}) bool {
		for _, addr := range value.([]string) {
			usingAddr[addr] = nil
		}
		return true
	})

	dels := make([]string, 0, 2)
	self.addressToTClient.Range(func(key, value interface{}) bool {
		//如果所有的topic都不再使用这个kiteio地址，那么则进行移除
		if _, ok := usingAddr[key.(string)]; !ok {
			dels = append(dels, key.(string))
		}
		return true
	})

	//需要删掉已经废弃的连接
	if len(dels) > 0 {
		for _, del := range dels {
			self.addressToTClient.Delete(del)
			self.clientManager.DeleteClients(del)
		}

		log.Infof("kite|onQServerChanged.RemoveUnusedAddr|%s|%s", topic, dels)
	}
}

//创建kiteio
func (self *kite) onTClientInit(host string) (*turbo.TClient, error) {

	//优先从clientmanager中获取，不存在则创建开启
	remoteClient := self.clientManager.FindTClient(host)
	if nil == remoteClient {
		//这里就新建一个remote客户端连接
		conn, err := dial(host)
		if nil != err {
			log.Errorf("kite|onTClientInit|Create REMOTE CLIENT|FAIL|%s|%s", err, host)
			return nil, err
		}
		remoteClient = turbo.NewTClient(self.ctx, conn, func() turbo.ICodec {
			return protocol.KiteQBytesCodec{
				MaxFrameLength: turbo.MAX_PACKET_BYTES}
		}, self.fire, self.config)
		remoteClient.Start()
		auth, err := handshake(self.ga, remoteClient)
		if !auth || nil != err {
			remoteClient.Shutdown()
			log.Errorf("kite|onTClientInit|HANDSHAKE|FAIL|%s|%s", err, auth)
			return nil, errors.New("onTClientInit FAIL ")
		}
		//授权
		self.clientManager.Auth(self.ga, remoteClient)
	}
	return remoteClient, nil
}

func (self *kite) DataChange(path string, binds []*registry.Binding) {
	//IGNORE
	log.Infof("kite|DataChange|%s|%s", path, binds)
}

func (self *kite) OnSessionExpired() {
	//推送订阅关系和topics
	self.Start()

	log.Infof("kite|OnSessionExpired|Restart...")
}
