package main

import (
	"encoding/json"
	"errors"
	"github.com/gorilla/websocket"
	"net"
	"net/http"
	"sort"
	"sync"
)

const (
	apiMins   = `/api/minutes`
	apiHours  = `/api/hours`
	apiDays   = `/api/days`
	apiMonths = `/api/months`
	apiLive   = `/api/live`
	apiIface  = `/api/interfaces`
	home      = `/`

	chanBufferSize = 8

	minId   setId = iota
	hourId  setId = iota
	dayId   setId = iota
	monthId setId = iota
)

var (
	errInvalidState = errors.New("Invalid state")
	errInvalidType  = errors.New("Invalid type")
)

type setId int

type webserver struct {
	lst     net.Listener
	ifaces  []ifstore
	lf      *LiveFeeder
	root    string
	wg      *sync.WaitGroup
	mtx     *sync.Mutex
	running bool
	err     error
}

func NewWebserver(lst net.Listener, root string, lf *LiveFeeder, ifaces []ifstore) (*webserver, error) {
	if lst == nil {
		return nil, errors.New("invalid listener")
	}
	return &webserver{
		lst:    lst,
		lf:     lf,
		ifaces: ifaces,
		root:   root,
		wg:     &sync.WaitGroup{},
		mtx:    &sync.Mutex{},
	}, nil
}

func (w *webserver) Close() error {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	if w.lst == nil || w.wg == nil || !w.running {
		return errInvalidState
	}
	if err := w.lst.Close(); err != nil {
		return err
	}
	w.wg.Wait()
	return w.err
}

func (w *webserver) Run() error {
	w.mtx.Lock()
	defer w.mtx.Unlock()
	if w.lst == nil || w.wg == nil || w.running {
		return errInvalidState
	}
	w.wg.Add(1)
	w.running = true
	go w.routine()
	return nil
}

func (w *webserver) routine() {
	defer w.wg.Done()
	mux := http.NewServeMux()
	mux.HandleFunc(apiMins, w.minutes)
	mux.HandleFunc(apiHours, w.hours)
	mux.HandleFunc(apiDays, w.days)
	mux.HandleFunc(apiMonths, w.months)
	mux.HandleFunc(apiIface, w.interfaces)
	mux.HandleFunc(apiLive, w.live)
	mux.Handle(home, http.FileServer(http.Dir(w.root)))

	w.err = http.Serve(w.lst, mux)
	w.running = false
}

type namedBwSample struct {
	Name string
	Data Sample
}

type liveWSFeeder struct {
	ch chan namedBwSample
}

func (wsf *liveWSFeeder) Write(name string, s Sample) error {
	bws, ok := s.(*BWSample)
	if !ok {
		//skip ip
		return errInvalidType
	}
	//we don't want to ever block the DB, so if a write fails, bail
	select {
	case wsf.ch <- namedBwSample{name, bws}:
	default:
		return nil
	}
	return nil
}

func (wsf *liveWSFeeder) Close() error {
	close(wsf.ch)
	return nil
}

func (w *webserver) interfaces(resp http.ResponseWriter, req *http.Request) {
	var ifaces []string
	for i := range w.ifaces {
		ifaces = append(ifaces, w.ifaces[i].iface.Name())
	}
	resp.Header().Set("Content-Type", "application/json")
	jenc := json.NewEncoder(resp)
	if err := jenc.Encode(ifaces); err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
	}
}

func (w *webserver) live(resp http.ResponseWriter, req *http.Request) {
	//get our feeder registered
	wsf := &liveWSFeeder{
		ch: make(chan namedBwSample, chanBufferSize),
	}
	id, err := w.lf.RegisterLiveFeeder(wsf)
	if err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer w.lf.DeregisterLiveFeeder(id)

	//upgrade to a websocket
	var upgrader = websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}
	conn, err := upgrader.Upgrade(resp, req, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	//start feeding and relaying
	for s := range wsf.ch {
		if err := websocket.WriteJSON(conn, s); err != nil {
			break
		}
	}
}

type sample struct {
	Name    string
	Samples []BWSample
}

func (w *webserver) sendSamples(req setId, resp http.ResponseWriter) error {
	var smps []sample
	for i := range w.ifaces {
		var smp sample
		var err error
		var bws []BWSample
		var s []Sample
		smp.Name = w.ifaces[i].iface.Name()
		switch req {
		case minId:
			s, err = w.ifaces[i].db.Minutes()
		case hourId:
			s, err = w.ifaces[i].db.Hours()
		case dayId:
			s, err = w.ifaces[i].db.Days()
		case monthId:
			s, err = w.ifaces[i].db.Months()
		default:
			err = errors.New("Invalid set")
		}
		if err != nil {
			return err
		}
		for j := range s {
			bw, ok := s[j].(*BWSample)
			if !ok {
				continue
			}
			bws = append(bws, *bw)
		}
		sort.Sort(sortSet(bws))
		smp.Samples = bws
		smps = append(smps, smp)
	}
	resp.Header().Set("Content-Type", "application/json")
	jenc := json.NewEncoder(resp)
	if err := jenc.Encode(smps); err != nil {
		return err
	}
	return nil
}

func (w *webserver) minutes(resp http.ResponseWriter, req *http.Request) {
	if err := w.sendSamples(minId, resp); err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
	}
}

func (w *webserver) hours(resp http.ResponseWriter, req *http.Request) {
	if err := w.sendSamples(hourId, resp); err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
	}
}

func (w *webserver) days(resp http.ResponseWriter, req *http.Request) {
	if err := w.sendSamples(dayId, resp); err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
	}
}

func (w *webserver) months(resp http.ResponseWriter, req *http.Request) {
	if err := w.sendSamples(monthId, resp); err != nil {
		resp.WriteHeader(http.StatusInternalServerError)
	}
}

type sortSet []BWSample

func (ss sortSet) Len() int           { return len(ss) }
func (ss sortSet) Less(i, j int) bool { return ss[i].TS().Before(ss[j].TS()) }
func (ss sortSet) Swap(i, j int)      { ss[i], ss[j] = ss[j], ss[i] }
