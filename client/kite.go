package client

import (
	"context"
	"errors"

	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/registry"

	"github.com/blackbeans/kiteq-common/stat"
	log "github.com/blackbeans/log4go"
	"github.com/blackbeans/turbo"
)

const (
	PATH_KITEQ_SERVER = "/kiteq/server"
)

//本地事务的方法
type DoTransaction func(message *protocol.QMessage) (bool, error)

type kite struct {
	ga             *turbo.GroupAuth
	registryUri    string
	topics         []string
	binds          []*registry.Binding //订阅的关系
	clientManager  *turbo.ClientManager
	listener       IListener
	kiteClients    map[string] /*topic*/ []*kiteIO //topic对应的kiteclient
	registryCenter *registry.RegistryCenter
	pipeline       *turbo.DefaultPipeline
	lock           sync.RWMutex
	config         *turbo.TConfig
	flowstat       *stat.FlowStat
	ctx            context.Context
	closed         context.CancelFunc
}

func newKite(registryUri, groupId, secretKey string, warmingupSec int) *kite {

	flowstat := stat.NewFlowStat()
	config := turbo.NewTConfig(
		"remoting-"+groupId,
		50, 16*1024,
		16*1024, 10000, 10000,
		10*time.Second,
		50*10000)

	registryCenter := registry.NewRegistryCenter(registryUri)
	ga := turbo.NewGroupAuth(groupId, secretKey)
	ga.WarmingupSec = warmingupSec

	ctx, closed := context.WithCancel(context.Background())
	manager := &kite{
		ga:             ga,
		kiteClients:    make(map[string][]*kiteIO, 10),
		topics:         make([]string, 0, 10),
		config:         config,
		flowstat:       flowstat,
		registryUri:    registryUri,
		registryCenter: registryCenter,
		ctx:            ctx,
		closed:         closed,
	}
	return manager
}

func (self *kite) remointflow() {
	go func() {
		t := time.NewTicker(1 * time.Second)
		for {
			ns := self.config.FlowStat.Stat()
			log.InfoLog("kite_client", "Remoting read:%d/%d\twrite:%d/%d\tdispatcher_go:%d/%d\tconnetions:%d", ns.ReadBytes, ns.ReadCount,
				ns.WriteBytes, ns.WriteCount, ns.DisPoolSize, ns.DisPoolCap, self.clientManager.ConnNum())
			<-t.C
		}
	}()
}

//设置listner
func (self *kite) SetListener(listener IListener) {
	self.listener = listener
}

//启动
func (self *kite) Start() {

	//没有listenr的直接启动报错
	if nil == self.listener {
		panic("KiteClient Listener Not Set !")
	}

	//重连管理器
	reconnManager := turbo.NewReconnectManager(true, 30*time.Second,
		100, handshake)

	//构造pipeline的结构
	pipeline := turbo.NewDefaultPipeline()
	self.clientManager = turbo.NewClientManager(reconnManager)
	pipeline.RegisteHandler("kiteclient-packet", NewPacketHandler("kiteclient-packet"))
	pipeline.RegisteHandler("kiteclient-heartbeat", NewHeartbeatHandler("kiteclient-heartbeat", 10*time.Second, 5*time.Second, self.clientManager))
	pipeline.RegisteHandler("kiteclient-accept", NewAcceptHandler("kiteclient-accept", self.listener))
	pipeline.RegisteHandler("kiteclient-remoting", turbo.NewRemotingHandler("kiteclient-remoting", self.clientManager))
	self.pipeline = pipeline
	//注册kiteqserver的变更
	self.registryCenter.RegisteWatcher(PATH_KITEQ_SERVER, self)
	hostname, _ := os.Hostname()
	//推送本机到
	err := self.registryCenter.PublishTopics(self.topics, self.ga.GroupId, hostname)
	if nil != err {
		log.Crashf("kite|PublishTopics|FAIL|%s|%s\n", err, self.topics)
	} else {
		log.InfoLog("kite_client", "kite|PublishTopics|SUCC|%s\n", self.topics)
	}

outter:
	for _, b := range self.binds {
		for _, t := range self.topics {
			if t == b.Topic {
				continue outter
			}
		}
		self.topics = append(self.topics, b.Topic)
	}

	for _, topic := range self.topics {

		hosts, err := self.registryCenter.GetQServerAndWatch(topic)
		if nil != err {
			log.Crashf("kite|GetQServerAndWatch|FAIL|%s|%s\n", err, topic)
		} else {
			log.InfoLog("kite_client", "kite|GetQServerAndWatch|SUCC|%s|%s\n", topic, hosts)
		}
		self.onQServerChanged(topic, hosts)
	}

	if len(self.kiteClients) <= 0 {
		log.Crashf("kite|Start|NO VALID KITESERVER|%s\n", self.topics)
	}

	if len(self.binds) > 0 {
		//订阅关系推送，并拉取QServer
		err = self.registryCenter.PublishBindings(self.ga.GroupId, self.binds)
		if nil != err {
			log.Crashf("kite|PublishBindings|FAIL|%s|%s\n", err, self.binds)
		}
	}

	//开启流量统计
	self.remointflow()
}

//创建物理连接
func dial(hostport string) (*net.TCPConn, error) {
	//连接
	remoteAddr, err_r := net.ResolveTCPAddr("tcp4", hostport)
	if nil != err_r {
		log.ErrorLog("kite_client", "kite|RECONNECT|RESOLVE ADDR |FAIL|remote:%s\n", err_r)
		return nil, err_r
	}
	conn, err := net.DialTCP("tcp4", nil, remoteAddr)
	if nil != err {
		log.ErrorLog("kite_client", "kite|RECONNECT|%s|FAIL|%s\n", hostport, err)
		return nil, err
	}

	return conn, nil
}

//握手包
func handshake(ga *turbo.GroupAuth, remoteClient *turbo.TClient) (bool, error) {

	for i := 0; i < 3; i++ {
		p := protocol.MarshalConnMeta(ga.GroupId, ga.SecretKey, int32(ga.WarmingupSec))
		rpacket := turbo.NewPacket(protocol.CMD_CONN_META, p)
		resp, err := remoteClient.WriteAndGet(*rpacket, 5*time.Second)
		if nil != err {
			//两秒后重试
			time.Sleep(2 * time.Second)
			log.WarnLog("kite_client", "kiteIO|handShake|FAIL|%s|%s\n", ga.GroupId, err)
		} else {
			authAck, ok := resp.(*protocol.ConnAuthAck)
			if !ok {
				return false, errors.New("Unmatches Handshake Ack Type! ")
			} else {
				if authAck.GetStatus() {
					log.InfoLog("kite_client", "kiteIO|handShake|SUCC|%s|%s\n", ga.GroupId, authAck.GetFeedback())
					return true, nil
				} else {
					log.WarnLog("kite_client", "kiteIO|handShake|FAIL|%s|%s\n", ga.GroupId, authAck.GetFeedback())
					return false, errors.New("Auth FAIL![" + authAck.GetFeedback() + "]")
				}
			}
		}
	}

	return false, errors.New("handshake fail! [" + remoteClient.RemoteAddr() + "]")
}

func (self *kite) SetPublishTopics(topics []string) {
	self.topics = append(self.topics, topics...)
}

func (self *kite) SetBindings(bindings []*registry.Binding) {
	for _, b := range bindings {
		b.GroupId = self.ga.GroupId
	}
	self.binds = bindings

}

//发送事务消息
func (self *kite) SendTxMessage(msg *protocol.QMessage, doTranscation DoTransaction) (err error) {

	msg.GetHeader().GroupId = protocol.MarshalPbString(self.ga.GroupId)

	//路由选择策略
	c, err := self.selectKiteClient(msg.GetHeader())
	if nil != err {
		return err
	}

	//先发送消息
	err = c.sendMessage(msg)
	if nil != err {
		return err
	}

	//执行本地事务返回succ为成功则提交、其余条件包括错误、失败都属于回滚
	feedback := ""
	succ := false
	txstatus := protocol.TX_UNKNOWN
	//执行本地事务
	succ, err = doTranscation(msg)
	if nil == err && succ {
		txstatus = protocol.TX_COMMIT
	} else {
		txstatus = protocol.TX_ROLLBACK
		if nil != err {
			feedback = err.Error()
		}
	}
	//发送txack到服务端
	c.sendTxAck(msg, txstatus, feedback)
	return err
}

//发送消息
func (self *kite) SendMessage(msg *protocol.QMessage) error {
	//fix header groupId
	msg.GetHeader().GroupId = protocol.MarshalPbString(self.ga.GroupId)
	//select client
	c, err := self.selectKiteClient(msg.GetHeader())
	if nil != err {
		return err
	}
	return c.sendMessage(msg)
}

//kiteclient路由选择策略
func (self *kite) selectKiteClient(header *protocol.Header) (*kiteIO, error) {

	self.lock.RLock()
	defer self.lock.RUnlock()

	clients, ok := self.kiteClients[header.GetTopic()]
	if !ok || len(clients) <= 0 {
		// 	log.WarnLog("kite_client","kite|selectKiteClient|FAIL|NO Remote Client|%s\n", header.GetTopic())
		return nil, errors.New("NO KITE CLIENT ! [" + header.GetTopic() + "]")
	}
	for i := 0; i < 3; i++ {
		c := clients[rand.Intn(len(clients))]
		if !c.client.IsClosed() {
			return c, nil
		}
	}
	return nil, errors.New("NO Alive KITE CLIENT ! [" + header.GetTopic() + "]")
}

func (self *kite) Destroy() {
	self.registryCenter.Close()
	self.closed()
}
