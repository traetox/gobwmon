package main

import (
	"sync"
)

type LiveFeeder struct {
	mtx           *sync.Mutex
	liveIds       int
	liveConsumers map[int]LiveConsumer
}

type LiveConsumer interface {
	Write(string, Sample) error
	Close() error
}

func NewLiveFeeder() (*LiveFeeder, error) {
	return &LiveFeeder{
		mtx:           &sync.Mutex{},
		liveConsumers: make(map[int]LiveConsumer, 4),
	}, nil
}

func (lf *LiveFeeder) RegisterLiveFeeder(lc LiveConsumer) (int, error) {
	lf.mtx.Lock()
	defer lf.mtx.Unlock()
	lf.liveIds++
	lf.liveConsumers[lf.liveIds] = lc
	return lf.liveIds, nil
}

func (lf *LiveFeeder) DeregisterLiveFeeder(id int) error {
	lf.mtx.Lock()
	defer lf.mtx.Unlock()
	lc, ok := lf.liveConsumers[id]
	if !ok {
		return nil
	}
	delete(lf.liveConsumers, id)
	if err := lc.Close(); err != nil {
		return err
	}
	return nil
}

func (lf *LiveFeeder) ServiceLiveFeeders(name string, s Sample) error {
	lf.mtx.Lock()
	defer lf.mtx.Unlock()
	for k, v := range lf.liveConsumers {
		if err := v.Write(name, s); err != nil {
			delete(lf.liveConsumers, k) //if a write fails delete it and close it
			v.Close()
		}
	}
	return nil
}
