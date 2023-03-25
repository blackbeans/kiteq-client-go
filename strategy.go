package client

import (
	"errors"
	"github.com/blackbeans/kiteq-common/protocol"
	"github.com/blackbeans/turbo"
	"math/rand"
	"sync"
	"time"
)

type Strategy interface {
	Select(header *protocol.Header, topicToAddress, addressToTClient *sync.Map,
		filter func(tc *turbo.TClient) bool) (*turbo.TClient, error)
}

type RandomSelector struct {
}

func NewRandomSelector() *RandomSelector {
	return &RandomSelector{}
}

// 选择
func (r *RandomSelector) Select(header *protocol.Header, topicToAddress, addressToTClient *sync.Map, filter func(tc *turbo.TClient) bool) (*turbo.TClient, error) {
	v, ok := topicToAddress.Load(header.GetTopic())
	if !ok || nil == v {
		// 	log.WarnLog("kite","kite|selectKiteClient|FAIL|NO Remote Client|%s", header.GetTopic())
		return nil, errors.New("NO KITE CLIENT ! [" + header.GetTopic() + "]")
	}

	addresses, ok := v.([]string)
	if !ok || len(addresses) <= 0 {
		return nil, errors.New("NO KITE CLIENT ! [" + header.GetTopic() + "]")
	}

	aliveTClients := addressPool.Get()
	for _, addr := range addresses {
		if v, ok := addressToTClient.Load(addr); ok && nil != v {
			if futureTask, ok := v.(*turbo.FutureTask); ok {
				if v, err := futureTask.Get(); nil == err && nil != v {
					if c, ok := v.(*turbo.TClient); ok && !c.IsClosed() && !filter(c) {
						aliveTClients = append(aliveTClients.([]*turbo.TClient), c)
					}
				}
			}
		}
	}
	//随机选取节点
	randomTClients := aliveTClients.([]*turbo.TClient)
	if len(randomTClients) > 0 {
		source := rand.NewSource(time.Now().UnixNano())
		random := rand.New(source)
		c := randomTClients[random.Intn(len(randomTClients))]
		addressPool.Put(randomTClients[:0])
		return c, nil
	} else {
		addressPool.Put(randomTClients[:0])
		return nil, errors.New("NO Alive KITE CLIENT ! [" + header.GetTopic() + "]")
	}
}
