package client

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"

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
	ga          *turbo.GroupAuth
	registryUri string
	topics      []string
	binds       []*registry.Binding //订阅的关系
	//bindWithHandlers map[string/*topic_messagetype*/]*registry.Binding //
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
	pools          map[uint8]*turbo.GPool
	defaultPool    *turbo.GPool
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
		listener:       NewKiteQListener(),
	}
	return manager
}

func (self *kite) remointflow() {
	go func() {
		t := time.NewTicker(1 * time.Second)
		for {
			ns := self.config.FlowStat.Stat()
			log.InfoLog("kite", "Remoting read:%d/%d\twrite:%d/%d\tdispatcher_go:%d/%d\tconnetions:%d", ns.ReadBytes, ns.ReadCount,
				ns.WriteBytes, ns.WriteCount, ns.DisPoolSize, ns.DisPoolCap, self.clientManager.ConnNum())
			<-t.C
		}
	}()
}

//废弃了设置listner
//会自动创建默认的Listener,只需要在订阅期间Binding设置处理器即可
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
	self.clientManager = turbo.NewClientManager(reconnManager)

	//构造pipeline的结构
	pipeline := turbo.NewDefaultPipeline()
	ackHandler := NewAckHandler("ack", 10*time.Second, 5*time.Second, self.clientManager)
	accept := NewAcceptHandler("accept", self.listener)
	remoting := turbo.NewRemotingHandler("remoting", self.clientManager)

	//对于ack和acceptevent使用不同的线程池，优先级不同
	msgPool := turbo.NewLimitPool(self.ctx, self.config.TW, 50)
	ackPool := turbo.NewLimitPool(self.ctx, self.config.TW, 5)
	storeAckPool := turbo.NewLimitPool(self.ctx, self.config.TW, 5)
	defaultPool := turbo.NewLimitPool(self.ctx, self.config.TW, 5)

	//pools
	pools := make(map[uint8]*turbo.GPool)
	pools[protocol.CMD_CONN_AUTH] = ackPool
	pools[protocol.CMD_HEARTBEAT] = ackPool
	pools[protocol.CMD_MESSAGE_STORE_ACK] = storeAckPool
	pools[protocol.CMD_TX_ACK] = msgPool
	pools[protocol.CMD_BYTES_MESSAGE] = msgPool
	pools[protocol.CMD_STRING_MESSAGE] = msgPool

	self.pools = pools
	self.defaultPool = defaultPool

	unmarshal := NewUnmarshalHandler("unmarshal",
		pools,
		defaultPool)
	pipeline.RegisteHandler("unmarshal", unmarshal)
	pipeline.RegisteHandler("ack", ackHandler)
	pipeline.RegisteHandler("accept", accept)
	pipeline.RegisteHandler("remoting", remoting)
	self.pipeline = pipeline
	//注册kiteqserver的变更
	self.registryCenter.RegisteWatcher(PATH_KITEQ_SERVER, self)
	hostname, _ := os.Hostname()
	//推送本机到
	err := self.registryCenter.PublishTopics(self.topics, self.ga.GroupId, hostname)
	if nil != err {
		log.Crashf("kite|PublishTopics|FAIL|%s|%s\n", err, self.topics)
	} else {
		log.InfoLog("kite", "kite|PublishTopics|SUCC|%s\n", self.topics)
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
			log.InfoLog("kite", "kite|GetQServerAndWatch|SUCC|%s|%s\n", topic, hosts)
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
	go ackHandler.heartbeat()
	go self.poolMonitor()

}

//poolMonitor
func (self *kite) poolMonitor() {
	for {
		select {
		case <-self.ctx.Done():
			break
		default:

		}

		keys := make([]int, 0, len(self.pools))
		for cmdType := range self.pools {
			keys = append(keys, int(cmdType))
		}
		sort.Ints(keys)
		str := fmt.Sprintf("Cmd-Pool\tGoroutines:%d\t", runtime.NumGoroutine())
		for _, cmdType := range keys {
			p := self.pools[uint8(cmdType)]
			used, capsize := p.Monitor()
			str += fmt.Sprintf("%s:%d/%d\t", protocol.NameOfCmd(uint8(cmdType)), used, capsize)
		}

		used, capsize := self.defaultPool.Monitor()
		str += fmt.Sprintf("default:%d/%d\t", used, capsize)
		log.InfoLog("kite", str)

		time.Sleep(1 * time.Second)
	}
}

//kiteQClient的处理器
func (self *kite) fire(ctx *turbo.TContext) error {
	p := ctx.Message
	c := ctx.Client
	event := turbo.NewPacketEvent(c, p)
	err := self.pipeline.FireWork(event)
	if nil != err {
		log.ErrorLog("kite", "kite|onPacketReceive|FAIL|%s|%t", err, p)
		return err
	}
	return nil
}

//创建物理连接
func dial(hostport string) (*net.TCPConn, error) {
	//连接
	remoteAddr, err_r := net.ResolveTCPAddr("tcp4", hostport)
	if nil != err_r {
		log.ErrorLog("kite", "kite|RECONNECT|RESOLVE ADDR |FAIL|remote:%s\n", err_r)
		return nil, err_r
	}
	conn, err := net.DialTCP("tcp4", nil, remoteAddr)
	if nil != err {
		log.ErrorLog("kite", "kite|RECONNECT|%s|FAIL|%s\n", hostport, err)
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
			log.WarnLog("kite", "kiteIO|handShake|FAIL|%s|%s\n", ga.GroupId, err)
		} else {
			authAck, ok := resp.(*protocol.ConnAuthAck)
			if !ok {
				return false, errors.New("Unmatches Handshake Ack Type! ")
			} else {
				if authAck.GetStatus() {
					log.InfoLog("kite", "kiteIO|handShake|SUCC|%s|%s\n", ga.GroupId, authAck.GetFeedback())
					return true, nil
				} else {
					log.WarnLog("kite", "kiteIO|handShake|FAIL|%s|%s\n", ga.GroupId, authAck.GetFeedback())
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
		if nil != b.Handler {
			self.listener.RegisteHandler(b)
		}
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
		// 	log.WarnLog("kite","kite|selectKiteClient|FAIL|NO Remote Client|%s\n", header.GetTopic())
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
