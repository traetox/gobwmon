package main

import (
	"errors"
	"time"
	"unsafe"
)

const (
	bwSampleSize = 8 * 3 //3 64bit integers
)

var (
	errBWTypeConversion  = errors.New("type is not a BWSample")
	errInvalidBufferSize = errors.New("Invalid buffer size")
)

type BWSample struct {
	Ts        time.Time //timestamp
	BytesUp   uint64    //bytes
	BytesDown uint64    //bytes
}

func NewBwSample() Sample {
	return &BWSample{}
}

func (s *BWSample) After(ts time.Time) bool {
	return !s.Ts.Before(ts)
}

func (s *BWSample) Add(sn Sample) error {
	x, ok := sn.(*BWSample)
	if !ok {
		return errBWTypeConversion
	}
	s.BytesUp += x.BytesUp
	s.BytesDown += x.BytesDown
	return nil
}

func (s *BWSample) Decode(b []byte) error {
	if len(b) != bwSampleSize {
		return errInvalidBufferSize
	}
	s.Ts = time.Unix(0, *(*int64)(unsafe.Pointer(&b[0])))
	s.BytesUp = *(*uint64)(unsafe.Pointer(&b[8]))
	s.BytesDown = *(*uint64)(unsafe.Pointer(&b[16]))
	return nil
}

func (s *BWSample) Encode() []byte {
	buff := make([]byte, bwSampleSize)
	*(*int64)(unsafe.Pointer(&buff[0])) = s.Ts.UnixNano()
	*(*uint64)(unsafe.Pointer(&buff[8])) = s.BytesUp
	*(*uint64)(unsafe.Pointer(&buff[16])) = s.BytesDown
	return buff
}

func (s *BWSample) TimeLabel(fmt string) []byte {
	return []byte(s.Ts.Format(fmt))
}

func (s *BWSample) TS() time.Time {
	return s.Ts
}

func (s *BWSample) SetTS(t time.Time) {
	s.Ts = t
}

func (s *BWSample) Less(x Sample) bool {
	sn, ok := x.(*BWSample)
	if !ok {
		return false
	}
	return s.Ts.Before(sn.Ts)
}
