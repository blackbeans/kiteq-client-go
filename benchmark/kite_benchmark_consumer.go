package main

import (
	"flag"
	"fmt"
	"github.com/blackbeans/kiteq-common/registry"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	log "github.com/blackbeans/log4go"
	"kiteq-client-go/benchmark/listener"
	"kiteq-client-go/client"
)

func main() {
	logxml := flag.String("logxml", "../log/log_consumer.xml", "-logxml=../log/log_consumer.xml")
	zkhost := flag.String("registryUri", "etcd://http://localhost:2379", "-registryUri=etcd://http://localhost:2379")
	flag.Parse()
	runtime.GOMAXPROCS(8)

	log.LoadConfiguration(*logxml)
	go func() {

		log.Info(http.ListenAndServe(":38000", nil))
	}()

	lis := &listener.DefaultListener{}
	go lis.Monitor()

	kite := client.NewKiteQClient(*zkhost, "s-mts-test1", "123456")
	kite.SetBindings(
		[]*registry.Binding{
			registry.Bind_Direct("s-mts-test1", "user-profile", "pay-succ", 8000, true)})
	kite.SetListener(lis)
	kite.Start()

	var s = make(chan os.Signal, 1)
	signal.Notify(s, syscall.SIGKILL, syscall.SIGUSR1)
	//是否收到kill的命令
	for {
		cmd := <-s
		if cmd == syscall.SIGKILL {
			break
		} else if cmd == syscall.SIGUSR1 {
			//如果为siguser1则进行dump内存
			unixtime := time.Now().Unix()
			path := "./heapdump-consumer" + fmt.Sprintf("%d", unixtime)
			f, err := os.Create(path)
			if nil != err {
				continue
			} else {
				debug.WriteHeapDump(f.Fd())
			}
		}
	}
	kite.Destory()
}
