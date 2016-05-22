package main

import (
	"errors"
	"log"
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
	ErrInterfaceOpen    = errors.New("Interface is already open")
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
	iface := &Iface{
		name:  name,
		alias: alias,
		mtx:   &sync.Mutex{},
		open:  true,
	}
	if err := iface.reopenInterfaces(); err != nil {
		log.Printf("Failed to open %s, will keep trying: %v\n", name, err)
	}
	return iface, nil
}

//reopeninterfaces is NOT protected by the mutex, caller must hold it
func (iface *Iface) reopenInterfaces() error {
	if iface.fioSend != nil || iface.fioRecv != nil {
		return ErrInterfaceOpen
	}
	//open up both the file descriptors
	fioRx, err := os.Open(path.Join(sysClassPath, iface.name, sysClassRxPath))
	if err != nil {
		return ErrInvalidInterface
	}
	fioTx, err := os.Open(path.Join(sysClassPath, iface.name, sysClassTxPath))
	if err != nil {
		fioRx.Close()
		return ErrInvalidInterface
	}
	iface.fioSend = fioTx
	iface.fioRecv = fioRx
	return nil
}

//closeInterfaces tries to do a little cleanup, but is mainly for when an interface disapears
func (iface *Iface) closeInterfaces() {
	//shutdown send
	iface.fioSend.Close()
	iface.fioSend = nil
	iface.lastSend = 0

	//shutdown recv
	iface.fioRecv.Close()
	iface.fioRecv = nil
	iface.lastRecv = 0
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
	//check if interfaces are closed, if so try to reopen them
	if iface.fioSend == nil || iface.fioRecv == nil {
		if err := iface.reopenInterfaces(); err != nil {
			//failed, return 0
			return 0, 0, nil
		}
	}

	rx, err := iface.getFioInt(iface.fioRecv)
	if err != nil {
		iface.closeInterfaces()
		return 0, 0, nil
	}
	tx, err := iface.getFioInt(iface.fioSend)
	if err != nil {
		iface.closeInterfaces()
		return 0, 0, nil
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
