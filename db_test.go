package main

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

const (
	p           = `/dev/shm/test.db`
	liveSetSize = 20 //always less than addCount
)

var (
	db *bwdb
)

func TestCreate(t *testing.T) {
	if db != nil {
		t.Fatal("db is not nil")
	}
	d, err := NewBwDb(p, liveSetSize, NewBwSample)
	if err != nil {
		t.Fatal(err)
	}
	db = d
}

func TestDbAddSeconds(t *testing.T) {
	if db == nil {
		t.Fatal("nil db")
	}
	//fill it
	ts := time.Unix(0, 0)
	for i := 0; i < 119; i++ {
		ts = ts.Add(time.Second)
		if err := db.Add(makeBWSample(ts, uint64(ts.Minute()), uint64(ts.Minute()))); err != nil {
			t.Fatal(err)
		}
		//fmt.Printf("%s %d %d\n", ts, uint64(ts.Unix()), uint64(ts.Unix()+1000))
	}
	//pull back and check values
	set, err := db.Minutes()
	if err != nil {
		t.Fatal(err)
	}
	if len(set) != 2 {
		//printSet(set)
		t.Fatal(fmt.Errorf("Invalid size: %d != 2", len(set)))
	}
	if err := testSet(set); err != nil {
		t.Fatal(err)
	}
}

func TestDbAddRand(t *testing.T) {
	if db == nil {
		t.Fatal("nil db")
	}
	ts := time.Now()
	for i := 0; i < 90; i++ {
		ts = ts.Add(time.Second * time.Duration(-100))
		if err := db.AddRand(makeBWSample(ts, uint64((i+1)*999), uint64((i+1)*777))); err != nil {
			t.Fatal(err)
		}
	}
}

func TestLive(t *testing.T) {
	if db == nil {
		t.Fatal("nil db")
	}
	set, err := db.LiveSet()
	if err != nil {
		t.Fatal(err)
	}
	if len(set) != liveSetSize {
		t.Fatal("Set is too small")
	}
}

//purge the DB
func TestPurge(t *testing.T) {
	if db == nil {
		t.Fatal("nil db")
	}
	if err := db.purge(); err != nil {
		t.Fatal(err)
	}
	v, err := db.LiveSet()
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 0 {
		t.Fatal("non empty set after purge")
	}
	v, err = db.Minutes()
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 0 {
		t.Fatal("non empty set after purge")
	}
	v, err = db.Hours()
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 0 {
		t.Fatal("non empty set after purge")
	}
	v, err = db.Days()
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 0 {
		t.Fatal("non empty set after purge")
	}
	v, err = db.Months()
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 0 {
		t.Fatal("non empty set after purge")
	}
}

//testing filling in hours and ensure it rolls over into days
func TestRollover(t *testing.T) {
	ts, err := time.Parse("01-02-2006 15:04:05", "01-01-2016 01:00:00")
	if err != nil {
		t.Fatal(err)
	}
	firstHour := ts.UTC().Hour()
	//fill in 60 minutes and check that they are actually there
	for i := 0; i < 60; i++ {
		if err := db.Add(makeBWSample(ts, uint64(1), uint64(1))); err != nil {
			t.Fatal(err)
		}
		ts = ts.Add(time.Minute)
	}
	v, err := db.Minutes()
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 60 {
		t.Fatal(fmt.Sprintf("Failed to retrieve 60 minutes after adding 60.  %d != 60", len(v)))
	}

	//force the rollover and check results
	if err := db.Add(makeBWSample(ts, uint64(1000), uint64(1000))); err != nil {
		t.Fatal(err)
	}
	//ensure there is only 1 minute
	v, err = db.Minutes()
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 1 {
		t.Fatal(fmt.Sprintf("Failed to retrieve 1 minute after forcing rollover %d != 1", len(v)))
	}

	//check that there is an hour with the appropriate hour
	v, err = db.Hours()
	if err != nil {
		t.Fatal(err)
	}
	if len(v) != 2 {
		t.Fatal(fmt.Sprintf("Failed to retrieve 2 hours after forcing rollover %d != 2", len(v)))
	}

	//check contents of the two hours, should be current hour
	if v[1].TS().UTC().Hour() != ts.UTC().Hour() {
		t.Fatal(fmt.Sprintf("Invalid current after rollover: %d != %d.  %v\n", v[1].TS().UTC().Hour(), ts.UTC().Hour(), v[1].TS()))
	}
	//current hour should have 1000 for upstream and downstream
	bs, ok := v[1].(*BWSample)
	if !ok {
		t.Fatal("Failed to type to BWSample\n")
	}
	if bs.BytesUp != 1000 {
		t.Fatal(fmt.Sprintf("Invalid upstream bytes: %d != 1000", bs.BytesUp))
	}
	if bs.BytesDown != 1000 {
		t.Fatal(fmt.Sprintf("Invalid downstream bytes: %d != 1000", bs.BytesDown))
	}

	//first should be previous hour
	if v[0].TS().UTC().Hour() != firstHour {
		t.Fatal(fmt.Sprintf("Invalid previous hour after rollover: %d != %d.  %v\n", v[0].TS().UTC().Hour(), firstHour, v[0].TS()))
	}
}

func TestClose(t *testing.T) {
	if db == nil {
		t.Fatal("nil db")
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = nil
}

func TestClean(t *testing.T) {
	if err := os.Remove(p); err != nil {
		t.Fatal(err)
	}
}

func makeBWSample(ts time.Time, up, down uint64) *BWSample {
	return &BWSample{
		Ts:        ts,
		BytesUp:   up,
		BytesDown: down,
	}
}

func testSet(s []Sample) error {
	for i := range s {
		bw, ok := s[i].(*BWSample)
		if !ok {
			return errors.New("Invalid type conversion")
		}
		if bw.BytesUp != uint64(bw.Ts.Minute()*60) {
			return fmt.Errorf("Invalid up value %s %d != %d", bw.Ts, bw.BytesUp, uint64(bw.Ts.Minute()*60))
		}
		if bw.BytesDown != uint64(bw.Ts.Minute()*60) {
			return fmt.Errorf("Invalid down value %s %d != %d", bw.Ts, bw.BytesDown, uint64(bw.Ts.Minute()*60))
		}
	}
	return nil
}

func printSet(s []Sample) {
	for i := range s {
		bw, ok := s[i].(*BWSample)
		if !ok {
			fmt.Printf("invalid type conversion")
			return
		}
		fmt.Printf("%s %d %d\n", bw.Ts, bw.BytesUp, bw.BytesDown)
	}
}
