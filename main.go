package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path"
	"sync"
	"time"
)

const (
	chanSize = 16
)

var (
	cfgFile = flag.String("config", `/etc/gobwmon`, "Configuration file")
)

type dataUpdate struct {
	data  BWSample
	index int
}

type ifstore struct {
	iface *Iface
	db    *bwdb
}

func init() {
	flag.Parse()
	if *cfgFile == "" {
		log.Fatal("Configuration file must be specified")
	}
}

func main() {
	var ifaces []ifstore
	cfg, err := NewConfig(*cfgFile)
	if err != nil {
		log.Fatal(err)
	}
	lst, err := net.Listen(`tcp`, cfg.Web_Server_Bind_Address)
	if err != nil {
		log.Fatal("Failed to bind to ", cfg.Web_Server_Bind_Address, err)
	}
	defer lst.Close()

	if len(cfg.Interface) == 0 {
		fmt.Printf("No interfaces specified")
		return
	}
	for i := range cfg.Interface {
		iface, err := NewIfmon(cfg.Interface[i])
		if err != nil {
			fmt.Printf("Failed to open %v: %v\n", cfg.Interface[i], err)
			return
		}
		defer iface.Close()
		dbpath := path.Join(cfg.Storage_Location, cfg.Interface[i]+".db")
		db, err := NewBwDb(dbpath, cfg.Live_Size, NewBwSample)
		if err != nil {
			fmt.Printf("Failed to open db %v: %v\n", dbpath, err)
			return
		}
		defer db.Close()
		if err := db.Rebase(time.Now()); err != nil {
			fmt.Printf("Failed to rebase DB: %v\n", err)
			return
		}
		is := ifstore{
			iface: iface,
			db:    db,
		}
		ifaces = append(ifaces, is)
	}
	lf, err := NewLiveFeeder()
	if err != nil {
		fmt.Printf("Failed to create live feeder: %v\n", err)
		return
	}
	ch := make(chan dataUpdate, chanSize)
	closer := make(chan bool, 1)
	wg := sync.WaitGroup{}
	wg.Add(2)

	ws, err := NewWebserver(lst, cfg.Web_Root, lf, ifaces)
	if err != nil {
		fmt.Printf("Failed to initialize webserver: %v\n", err)
		return
	}
	if err := ws.Run(); err != nil {
		fmt.Printf("Failed to start the webserver: %v\n", err)
		return
	}

	//kick off the consumer
	go updateConsumer(ch, ifaces, &wg)

	//kick off the producer
	interval := time.Duration(cfg.Update_Interval_Seconds) * time.Second
	go updateProducer(ch, interval, ifaces, &wg, closer, lf)

	//register for signals and wait
	sch := make(chan os.Signal)
	signal.Notify(sch, os.Interrupt, os.Kill)
	<-sch

	//close things down and wait for everyone to exit
	close(closer)
	fmt.Printf("Closed closer, waiting\n")
	wg.Wait()

}

func updateProducer(ch chan dataUpdate, interval time.Duration, is []ifstore, wg *sync.WaitGroup, cl chan bool, lf *LiveFeeder) {
	defer wg.Done()
	defer close(ch)
	//build a ticker
	tkr := time.NewTicker(interval)
	defer tkr.Stop()
opLoop:
	for {
		select {
		case _ = <-cl:
			break opLoop
		case _ = <-tkr.C:
			for j := range is {
				s, r, err := is[j].iface.GetStats()
				if err != nil {
					fmt.Printf("GetStats failed: %v\n", err)
					return
				}
				sample := BWSample{
					Ts:        time.Now(),
					BytesUp:   s,
					BytesDown: r,
				}
				ch <- dataUpdate{
					data:  sample,
					index: j,
				}
				if err := lf.ServiceLiveFeeders(is[j].iface.Name(), &sample); err != nil {
					fmt.Printf("Failed to service feeders: %v\n", err)
					break
				}
			}
		}
	}
}

func updateConsumer(ch chan dataUpdate, is []ifstore, wg *sync.WaitGroup) {
	defer wg.Done()

	for v := range ch {
		//check that things are kosher
		if v.index >= len(is) {
			fmt.Printf("invalid index on data update: %d >= %d\n", v.index, len(is))
			continue
		}
		//check the data to the database
		if err := is[v.index].db.Add(&v.data); err != nil {
			fmt.Printf("Failed to update DB: %v\n", err)
			continue
		}
	}
}
