package main

import (
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/AgilaNews/bfserver/bloom"
	g "github.com/AgilaNews/bfserver/g"
	"github.com/AgilaNews/bfserver/service"
	"github.com/alecthomas/log4go"
	"net/http"
	_ "net/http/pprof"
)

func main() {
	var wg sync.WaitGroup
	done := make(chan bool)
	defer log4go.Global.Close()

	bloom.UseGzip = g.Config.Persist.UseGzip
	log4go.Info("current cpu: %d", runtime.NumCPU())
	rand.Seed(time.Now().UTC().UnixNano())

	persister, err := bloom.NewLocalFileFilterPersister(g.Config.Persist.Path)
	if err != nil {
		panic("open persister erorr")
	}

	manager, err := bloom.NewFilterManager(persister)
	if err != nil {
		panic("new filter manager error")
	}

	if err := manager.RecoverFilters(); err != nil {
		panic("recover filter error")
	}

	c, err := service.NewBloomFilterServer(g.Config.Rpc.BF.Addr, manager)
	if err != nil {
		panic("create filter server error")
	}

	wg.Add(2)

	go func() {
		defer wg.Done()
		c.Work()

		log4go.Info("rpc server graceful exits")
	}()

	go func() {
		defer wg.Done()

		manager.Work()
		log4go.Info("manager gracefully quit")
	}()

	go func() {
		wg.Wait()
		done <- true
	}()

	if g.Config.Gprof.Enabled {
		go func() {
			log4go.Info("gprof listen on %s", g.Config.Gprof.Addr)
			http.ListenAndServe(g.Config.Gprof.Addr, nil)
		}()
	}
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
OUTFOR:
	for {
		select {
		case <-sigs:
			log4go.Info("get interrupt, gracefull stop")
			c.Stop()
			manager.Stop()
		case <-done:
			log4go.Info("all routine done, exit")
			break OUTFOR
		}
	}
}
