package client

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sort"

	"net"
	"os"
	"sync"
	"time"

	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/kiteq-common/registry"

	"github.com/blackbeans/kiteq-common/stat"
	"github.com/blackbeans/turbo"
)

var addressPool = &sync.Pool{}

func init() {
	addressPool.New = func() interface{} {
		return make([]*turbo.TClient, 0, 10)
	}
}

//本地事务的方法
type DoTransaction func(message *protocol.QMessage) (bool, error)

type kite struct {
	isPreEnv      bool //是否为预发部环境的client
	ga            *turbo.GroupAuth
	registryUri   string
	topics        []string
	binds         []*registry.Binding //订阅的关系
	clientManager *turbo.ClientManager
	listener      IListener

	topicToAddress   *sync.Map //topic对应的address
	addressToTClient *sync.Map //远程地址对应的物理连接

	registryCenter *registry.RegistryCenter
	pipeline       *turbo.DefaultPipeline
	config         *turbo.TConfig
	flowstat       *stat.FlowStat
	ctx            context.Context
	closed         context.CancelFunc
	pools          map[uint8]*turbo.GPool
	defaultPool    *turbo.GPool

	//心跳时间
	heartbeatPeriod  time.Duration
	heartbeatTimeout time.Duration

	cliSelector Strategy
}

func newKite(parent context.Context, registryUri, groupId, secretKey string, warmingupSec int, listener IListener) *kite {

	flowstat := stat.NewFlowStat()
	config := turbo.NewTConfig(
		"remoting-"+groupId,
		50, 16*1024,
		16*1024, 10000, 10000,
		10*time.Second,
		50*10000)

	ctx, closed := context.WithCancel(parent)
	registryCenter := registry.NewRegistryCenter(ctx, registryUri)
	ga := turbo.NewGroupAuth(groupId, secretKey)
	ga.WarmingupSec = warmingupSec

	manager := &kite{
		ga:               ga,
		topicToAddress:   &sync.Map{},
		addressToTClient: &sync.Map{},
		topics:           make([]string, 0, 10),
		config:           config,
		flowstat:         flowstat,
		registryUri:      registryUri,
		registryCenter:   registryCenter,
		ctx:              ctx,
		closed:           closed,
		listener:         listener,
		heartbeatPeriod:  10 * time.Second,
		heartbeatTimeout: 5 * time.Second,
		cliSelector:      NewRandomSelector(),
	}
	registryCenter.RegisterWatcher(manager)
	return manager
}

func (k *kite) remointflow() {
	go func() {
		t := time.NewTicker(1 * time.Second)
		for {
			ns := k.config.FlowStat.Stat()
			log.Infof("Remoting read:%d/%d\twrite:%d/%d\tdispatcher_go:%d/%d\tconnetions:%d", ns.ReadBytes, ns.ReadCount,
				ns.WriteBytes, ns.WriteCount, ns.DisPoolSize, ns.DisPoolCap, k.clientManager.ConnNum())
			<-t.C
		}
	}()
}

//废弃了设置listner
//会自动创建默认的Listener,只需要在订阅期间Binding设置处理器即可
func (k *kite) SetListener(listener IListener) {
	k.listener = listener
}

func (k *kite) GetListener() IListener {
	return k.listener
}

//启动
func (k *kite) Start() {

	//没有listenr的直接启动报错
	if nil == k.listener {
		panic("KiteClient Listener Not Set !")
	}

	//如果是预发环境，则加入预发环境的group后缀
	if k.isPreEnv {
		k.ga.GroupId = fmt.Sprintf("%s-pre", k.ga.GroupId)
	}

	//重连管理器
	reconnManager := turbo.NewReconnectManager(true, 30*time.Second,
		100, handshake)
	k.clientManager = turbo.NewClientManager(reconnManager)

	//构造pipeline的结构
	pipeline := turbo.NewDefaultPipeline()
	ackHandler := NewAckHandler("ack", k.clientManager)
	accept := NewAcceptHandler("accept", k.listener)
	remoting := turbo.NewRemotingHandler("remoting", k.clientManager)

	//对于ack和acceptevent使用不同的线程池，优先级不同
	msgPool := turbo.NewLimitPool(k.ctx, 50)
	ackPool := turbo.NewLimitPool(k.ctx, 5)
	storeAckPool := turbo.NewLimitPool(k.ctx, 5)
	defaultPool := turbo.NewLimitPool(k.ctx, 5)

	//pools
	pools := make(map[uint8]*turbo.GPool)
	pools[protocol.CMD_CONN_AUTH] = ackPool
	pools[protocol.CMD_HEARTBEAT] = ackPool
	pools[protocol.CMD_MESSAGE_STORE_ACK] = storeAckPool
	pools[protocol.CMD_TX_ACK] = msgPool
	pools[protocol.CMD_BYTES_MESSAGE] = msgPool
	pools[protocol.CMD_STRING_MESSAGE] = msgPool

	k.pools = pools
	k.defaultPool = defaultPool

	unmarshal := NewUnmarshalHandler("unmarshal",
		pools,
		defaultPool)
	pipeline.RegisteHandler("unmarshal", unmarshal)
	pipeline.RegisteHandler("ack", ackHandler)
	pipeline.RegisteHandler("accept", accept)
	pipeline.RegisteHandler("remoting", remoting)
	k.pipeline = pipeline
	//注册kiteqserver的变更
	k.registryCenter.RegisterWatcher(k)
	hostname, _ := os.Hostname()
	//推送本机到
	err := k.registryCenter.PublishTopics(k.topics, k.ga.GroupId, hostname)
	if nil != err {
		log.Errorf("kite|PublishTopics|FAIL|%s|%s", err, k.topics)
	} else {
		log.Infof("kite|PublishTopics|SUCC|%s", k.topics)
	}

outter:
	for _, b := range k.binds {
		for _, t := range k.topics {
			if t == b.Topic {
				continue outter
			}
		}
		k.topics = append(k.topics, b.Topic)
	}

	for _, topic := range k.topics {

		hosts, err := k.registryCenter.GetQServerAndWatch(topic)
		if nil != err {
			log.Errorf("kite|GetQServerAndWatch|FAIL|%s|%s", err, topic)
		} else {
			log.Infof("kite|GetQServerAndWatch|SUCC|%s|%s", topic, hosts)
		}
		k.OnQServerChanged(topic, hosts)
	}

	length := 0
	k.topicToAddress.Range(func(key, value interface{}) bool {
		length++
		return true
	})

	if length <= 0 {
		log.Errorf("kite|Start|NO VALID KITESERVER|%s", k.topics)
	}

	if !k.isPreEnv && len(k.binds) > 0 {
		//订阅关系推送，并拉取QServer
		err = k.registryCenter.PublishBindings(k.ga.GroupId, k.binds)
		if nil != err {
			log.Errorf("kite|PublishBindings|FAIL|%s|%v", err, k.binds)
		}
	}

	if k.isPreEnv {
		rawBinds, _ := json.Marshal(k.binds)
		log.Infof("kite|PublishBindings|Ignored|[preEnv:%v]|%s...", k.isPreEnv, string(rawBinds))
	}

	//开启流量统计
	k.remointflow()
	go k.heartbeat()
	go k.poolMonitor()

}

//poolMonitor
func (k *kite) poolMonitor() {
	for {
		select {
		case <-k.ctx.Done():
			break
		default:

		}

		keys := make([]int, 0, len(k.pools))
		for cmdType := range k.pools {
			keys = append(keys, int(cmdType))
		}
		sort.Ints(keys)
		str := fmt.Sprintf("Cmd-Pool\tGoroutines:%d\t", runtime.NumGoroutine())
		for _, cmdType := range keys {
			p := k.pools[uint8(cmdType)]
			used, capsize := p.Monitor()
			str += fmt.Sprintf("%s:%d/%d\t", protocol.NameOfCmd(uint8(cmdType)), used, capsize)
		}

		used, capsize := k.defaultPool.Monitor()
		str += fmt.Sprintf("default:%d/%d\t", used, capsize)
		log.Infof(str)

		time.Sleep(1 * time.Second)
	}
}

//kiteQClient的处理器
func (k *kite) fire(ctx *turbo.TContext) error {
	p := ctx.Message
	c := ctx.Client
	event := turbo.NewPacketEvent(c, p)
	err := k.pipeline.FireWork(event)
	if nil != err {
		log.Errorf("kite|onPacketReceive|FAIL|%s|%v", err, p)
		return err
	}
	return nil
}

//创建物理连接
func dial(hostport string) (*net.TCPConn, error) {
	//连接
	remoteAddr, err_r := net.ResolveTCPAddr("tcp4", hostport)
	if nil != err_r {
		log.Errorf("kite|RECONNECT|RESOLVE ADDR |FAIL|remote:%s", err_r)
		return nil, err_r
	}
	conn, err := net.DialTCP("tcp4", nil, remoteAddr)
	if nil != err {
		log.Errorf("kite|RECONNECT|%s|FAIL|%s", hostport, err)
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
			log.Warnf("kiteIO|handShake|FAIL|%s|%s", ga.GroupId, err)
		} else {
			authAck, ok := resp.(*protocol.ConnAuthAck)
			if !ok {
				return false, errors.New("Unmatches Handshake Ack Type! ")
			} else {
				if authAck.GetStatus() {
					log.Infof("kiteIO|handShake|SUCC|%s|%s", ga.GroupId, authAck.GetFeedback())
					return true, nil
				} else {
					log.Warnf("kiteIO|handShake|FAIL|%s|%s", ga.GroupId, authAck.GetFeedback())
					return false, errors.New("Auth FAIL![" + authAck.GetFeedback() + "]")
				}
			}
		}
	}

	return false, errors.New("handshake fail! [" + remoteClient.RemoteAddr() + "]")
}

func (k *kite) SetPublishTopics(topics []string) {
	k.topics = append(k.topics, topics...)
}

func (k *kite) SetBindings(bindings []*registry.Binding) {
	for _, b := range bindings {
		b.GroupId = k.ga.GroupId
		if nil != b.Handler {
			k.listener.RegisteHandler(b)
		}
	}
	k.binds = bindings
}

//发送事务消息
func (k *kite) SendTxMessage(msg *protocol.QMessage, doTransaction DoTransaction) (err error) {

	msg.GetHeader().GroupId = protocol.MarshalPbString(k.ga.GroupId)

	//路由选择策略
	c, err := k.selectKiteClient(msg.GetHeader())
	if nil != err {
		return err
	}

	//先发送消息
	err = sendMessage(c, msg)
	if nil != err {
		return err
	}

	//执行本地事务返回succ为成功则提交、其余条件包括错误、失败都属于回滚
	feedback := ""
	succ := false
	txstatus := protocol.TX_UNKNOWN
	//执行本地事务
	succ, err = doTransaction(msg)
	if nil == err && succ {
		txstatus = protocol.TX_COMMIT
	} else {
		txstatus = protocol.TX_ROLLBACK
		if nil != err {
			feedback = err.Error()
		}
	}
	//发送txack到服务端
	sendTxAck(c, msg, txstatus, feedback)
	return err
}

//发送消息
func (k *kite) SendMessage(msg *protocol.QMessage) error {
	//fix header groupId
	msg.GetHeader().GroupId = protocol.MarshalPbString(k.ga.GroupId)
	//select client
	c, err := k.selectKiteClient(msg.GetHeader())
	if nil != err {
		return err
	}
	return sendMessage(c, msg)
}

//kiteclient路由选择策略
func (k *kite) selectKiteClient(header *protocol.Header) (*turbo.TClient, error) {
	return k.cliSelector.Select(header, k.topicToAddress, k.addressToTClient, func(tc *turbo.TClient) bool {
		//只接收
		//remoteAddr := tc.RemoteAddr()

		return false
	})
}

func (k *kite) heartbeat() {

	for {
		select {
		case <-time.After(k.heartbeatPeriod):
			//心跳检测
			k.addressToTClient.Range(func(key, value interface{}) bool {
				i := 0
				future := value.(*turbo.FutureTask)
				if v, err := future.Get(); nil == err && nil != v {
					c := v.(*turbo.TClient)
					//关闭的时候发起重连
					if c.IsClosed() {
						i = 3
					} else {
						//如果是空闲的则发起心跳
						if c.Idle() {
							for ; i < 3; i++ {
								id := time.Now().Unix()
								p := protocol.MarshalHeartbeatPacket(id)
								hp := turbo.NewPacket(protocol.CMD_HEARTBEAT, p)
								err := c.Ping(hp, time.Duration(k.heartbeatTimeout))
								//如果有错误则需要记录
								if nil != err {
									log.Warnf("AckHandler|KeepAlive|FAIL|%s|local:%s|remote:%s|%d", err, c.LocalAddr(), key, id)
									continue
								} else {
									log.Infof("AckHandler|KeepAlive|SUCC|local:%s|remote:%s|%d|%d ...", c.LocalAddr(), key, id, i)
									break
								}
							}
						}
					}
					if i >= 3 {
						//说明连接有问题需要重连
						c.Shutdown()
						k.clientManager.SubmitReconnect(c)
						log.Warnf("AckHandler|SubmitReconnect|%s", c.RemoteAddr())
					}
				}
				return true
			})
		case <-k.ctx.Done():
			return
		}
	}
}

func (k *kite) OnBindChanged(topic, groupId string, newbinds []*registry.Binding) {}

func (k *kite) Destroy() {
	k.registryCenter.Close()
	k.closed()
}
