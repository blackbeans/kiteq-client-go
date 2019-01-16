# kiteq-client-go
kiteq-client-go is kiteq's go client

#### 工程结构
    kiteq/
    ├── README.md
    ├── log               log4go的配置
    ├── benchmark         KiteQ的Benchmark程序
    ├── client            KiteQ的客户端
  
##### 启动客户端：
```
        对于KiteQClient需要实现消息监听器，我们定义了如下的接口：
        type IListener interface {
            //接受投递消息的回调
            OnMessage(msg *protocol.StringMessage) bool
            //接收事务回调
            // 除非明确提交成功、其余都为不成功
            // 有异常或者返回值为false均为不提交
            OnMessageCheck(tx *protocol.TxResponse) error
        }

    启动Producer :
        producer := client.NewKiteQClient(${zkhost}, ${groupId}, ${password}, &defualtListener{})
        producer.SetTopics([]string{"trade"})
        producer.Start()
        //构建消息
        msg := &protocol.StringMessage{}
        msg.Header = &protocol.Header{
            MessageId:     proto.String(client.MessageId()),
            Topic:         proto.String("trade"),
            MessageType:   proto.String("pay-succ"),
            ExpiredTime:   proto.Int64(time.Now().Unix()),
            DeliveryLimit: proto.Int32(-1),
            GroupId:       proto.String("go-kite-test"),
            Commit:        proto.Bool(true)}
        msg.Body = proto.String("echo")
        //发送消息
        producer.SendStringMessage(msg)

    启动Consumer:
        consumer:= client.NewKiteQClient(${zkhost}, ${groupId}, ${password}, &defualtListener{})
        consumer.SetBindings([]*binding.Binding{
            binding.Bind_Direct("s-mts-test", "trade", "pay-succ", 1000, true),
        })
        consumer.Start()
  ```
  Bingo  完成发布和订阅消息的功能了.....
  
  - 可以参考benchmark中使用
   
