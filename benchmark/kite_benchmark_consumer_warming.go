package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/blackbeans/kiteq-client-go"
	"github.com/blackbeans/kiteq-common/registry"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	"github.com/blackbeans/kiteq-client-go/benchmark/listener"
)

func main() {
	zkhost := flag.String("registryUri", "etcd://http://localhost:2379", "-registryUri=etcd://http://localhost:2379")
	warmingUp := flag.Int("warmingUp", 10, "-warmingUp=10")
	flag.Parse()
	runtime.GOMAXPROCS(8)

	go func() {

		http.ListenAndServe(":38000", nil)
	}()

	lis := &listener.DefaultListener{}
	go lis.Monitor()

	kite := client.NewKiteQClientWithWarmup(context.TODO(), *zkhost, "s-mts-test1", "123456", *warmingUp)
	kite.SetBindings([]*registry.Binding{
		registry.Bind_Direct("s-mts-test1", "user-profile", "profile-update", 8000, false),
	})
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
	kite.Destroy()
}
