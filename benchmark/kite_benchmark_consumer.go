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
	log "github.com/sirupsen/logrus"
)

func main() {
	registryUrl := flag.String("registryUri", "zk://localhost:2181", "-registryUri=file://./registry_demo.yaml")
	flag.Parse()
	runtime.GOMAXPROCS(8)

	go func() {

		log.Info(http.ListenAndServe(":38000", nil))
	}()

	lis := &listener.DefaultListener{}
	go lis.Monitor()

	kite := client.NewKiteQClient(context.TODO(), *registryUrl, "s-mts-test1", "123456")
	kite.SetBindings(
		[]*registry.Binding{
			registry.Bind_Direct("s-mts-test1", "user-profile", "profile-update", 8000, true)})
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
