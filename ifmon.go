package main

import (
	"errors"
	"os"
	"path"
	"strconv"
	"sync"
)

const (
	sysClassPath   = `/sys/class/net/`
	sysClassRxPath = `/statistics/rx_bytes`
	sysClassTxPath = `/statistics/tx_bytes`
)

var (
	ErrInvalidInterface = errors.New("Interface is invalid")
	ErrClosed           = errors.New("Interface Closed")
	ErrFailedSeek       = errors.New("Failed to seek stat file")
	ErrInvalidData      = errors.New("Invalid data")
)

type Iface struct {
	name     string
	alias    string
	fioSend  *os.File
	fioRecv  *os.File
	mtx      *sync.Mutex
	lastSend uint64
	lastRecv uint64
	open     bool
}

func NewIfmon(name, alias string) (*Iface, error) {
	//open up both the file descriptors
	fioRx, err := os.Open(path.Join(sysClassPath, name, sysClassRxPath))
	if err != nil {
		return nil, ErrInvalidInterface
	}
	fioTx, err := os.Open(path.Join(sysClassPath, name, sysClassTxPath))
	if err != nil {
		fioRx.Close()
		return nil, ErrInvalidInterface
	}

	return &Iface{
		name:    name,
		alias:   alias,
		fioSend: fioTx,
		fioRecv: fioRx,
		mtx:     &sync.Mutex{},
		open:    true,
	}, nil
}

func (iface *Iface) Close() error {
	iface.mtx.Lock()
	defer iface.mtx.Unlock()
	if !iface.open {
		return ErrClosed
	}
	if err := iface.fioSend.Close(); err != nil {
		return err
	}
	if err := iface.fioRecv.Close(); err != nil {
		return err
	}
	iface.open = false
	iface.fioSend = nil
	iface.fioRecv = nil
	return nil
}

func (iface *Iface) getFioInt(fio *os.File) (uint64, error) {
	bt := make([]byte, 64)
	n, err := fio.Seek(0, 0)
	if err != nil {
		return 0, err
	}
	if n != 0 {
		return 0, ErrFailedSeek
	}
	rn, err := fio.Read(bt)
	if err != nil {
		return 0, err
	}
	if bt[rn-1] != '\n' || rn < 2 {
		return 0, ErrInvalidData
	}
	v := string(bt[0 : rn-1])
	return strconv.ParseUint(v, 10, 64)
}

//getStats returns send bytes, recv bytes, and error
//returned data is the quantity of bytes sent/recv since last query
func (iface *Iface) GetStats() (uint64, uint64, error) {
	iface.mtx.Lock()
	defer iface.mtx.Unlock()
	rx, err := iface.getFioInt(iface.fioRecv)
	if err != nil {
		return 0, 0, err
	}
	tx, err := iface.getFioInt(iface.fioSend)
	if err != nil {
		return 0, 0, err
	}
	sendInt := tx - iface.lastSend
	recvInt := rx - iface.lastRecv
	if iface.lastSend == 0 {
		sendInt = 0
	}
	if iface.lastRecv == 0 {
		recvInt = 0
	}
	iface.lastSend = tx
	iface.lastRecv = rx

	return sendInt, recvInt, nil
}

func (iface Iface) Name() string {
	if iface.alias == "" {
		return iface.name
	}
	return iface.alias
}
