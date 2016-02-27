package main

import (
	"container/list"
	"errors"
	"fmt"
	"github.com/boltdb/bolt"
	"sync"
	"time"
)

const (
	defaultHistSize = 60
	minFmt          = `010220061504`
	hourFmt         = `0102200615`
	dayFmt          = `01022006`
	monFmt          = `012006`

	dataSize = 8 * 3
)

var (
	errNotOpen          = errors.New("Database not open")
	errInvalidTimestamp = errors.New("Invalid timestamp")
	errCorruptValue     = errors.New("Corrupt value in DB")

	errNoBucket = errors.New("Bucket does not exist")

	bktMin  = []byte(`min`)
	bktHour = []byte(`hour`)
	bktDay  = []byte(`day`)
	bktMon  = []byte(`mon`)

	zeroTime time.Time
)

type newVarInit func() Sample

type bwdb struct {
	open     bool
	mtx      *sync.Mutex
	db       *bolt.DB
	hist     *list.List
	histSize int
	last     time.Time
	newVar   newVarInit
}

type Sample interface {
	After(time.Time) bool
	Add(Sample) error
	Decode([]byte) error
	Encode() []byte
	TimeLabel(string) []byte
	TS() time.Time
	SetTS(time.Time)
}

//we hand in a temporary variable that represents the type
//used in storing to the DB, this is so we can use an interface here
func NewBwDb(path string, liveSize int, nv newVarInit) (*bwdb, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, err
	}
	if liveSize <= 0 {
		liveSize = defaultHistSize
	}
	r := &bwdb{
		mtx:      &sync.Mutex{},
		db:       db,
		open:     true,
		hist:     list.New(),
		histSize: liveSize,
		newVar:   nv,
	}
	return r, nil
}

func (db *bwdb) Close() error {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	if !db.open {
		return errNotOpen
	}
	if err := db.db.Close(); err != nil {
		return err
	}
	db.hist.Init()
	db.open = false
	return nil
}

//Add adds a timestamp to the DB with the number of bytes it represents
func (db *bwdb) Add(s Sample) error {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	if !db.open {
		return errNotOpen
	}
	//check if this isn't a regular sequential update
	if !s.After(db.last) {
		return db.addOutOfOrder(s)
	}
	//add to our live list
	db.hist.PushFront(s)

	//trim
	for db.hist.Len() > db.histSize {
		db.hist.Remove(db.hist.Back())
	}
	if db.last == zeroTime {
		db.last = s.TS()
	}

	//add value to each bucket and shift if needed
	err := db.db.Batch(func(tx *bolt.Tx) error {
		//get all our lables
		minLbl := s.TimeLabel(minFmt)
		hourLbl := s.TimeLabel(hourFmt)
		dayLbl := s.TimeLabel(dayFmt)
		monLbl := s.TimeLabel(monFmt)

		//get all our buckets
		minBkt, err := tx.CreateBucketIfNotExists(bktMin)
		if err != nil {
			return err
		}
		hourBkt, err := tx.CreateBucketIfNotExists(bktHour)
		if err != nil {
			return err
		}
		dayBkt, err := tx.CreateBucketIfNotExists(bktDay)
		if err != nil {
			return err
		}
		monBkt, err := tx.CreateBucketIfNotExists(bktMon)
		if err != nil {
			return err
		}
		//perform any required shifts
		//new hour
		if string(hourLbl) != db.last.Format(hourFmt) {
			lastHourLbl := []byte(db.last.Format(hourFmt))
			if err := db.sumAndShift(minBkt, hourBkt, lastHourLbl); err != nil {
				return err
			}
		}
		//new day
		if string(dayLbl) != db.last.Format(dayFmt) {
			lastDayLbl := []byte(db.last.Format(dayFmt))
			if err := db.sumAndShift(hourBkt, dayBkt, lastDayLbl); err != nil {
				return err
			}
		}
		//new month
		if string(monLbl) != db.last.Format(monFmt) {
			lastMonLabel := []byte(db.last.Format(monFmt))
			if err := db.sumAndShift(dayBkt, monBkt, lastMonLabel); err != nil {
				return err
			}
		}

		//minutes
		if err := db.updateVal(minBkt, minLbl, s); err != nil {
			return err
		}

		//hour
		if err := db.updateVal(hourBkt, hourLbl, s); err != nil {
			return err
		}

		//days
		if err := db.updateVal(dayBkt, dayLbl, s); err != nil {
			return err
		}

		//months
		if err := db.updateVal(monBkt, monLbl, s); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return err
	}
	db.last = s.TS()
	return nil
}

func (db *bwdb) AddRand(s Sample) error {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	return db.addOutOfOrder(s)
}

//addOutOfOrder does not go into the live set, as its assumed to come out of order and does not update the last variable
func (db *bwdb) addOutOfOrder(s Sample) error {
	if !db.open {
		return errNotOpen
	}
	return db.db.Batch(func(tx *bolt.Tx) error {
		//figure out where it should be
		if s.TS().Minute() == db.last.Minute() {
			minBkt, err := tx.CreateBucketIfNotExists(bktMin)
			if err != nil {
				return err
			}
			minLbl := s.TimeLabel(minFmt)
			if err := db.updateVal(minBkt, minLbl, s); err != nil {
				return err
			}
		} else if s.TS().Hour() == db.last.Hour() {
			hourBkt, err := tx.CreateBucketIfNotExists(bktHour)
			if err != nil {
				return err
			}
			hourLbl := s.TimeLabel(hourFmt)
			if err := db.updateVal(hourBkt, hourLbl, s); err != nil {
				return err
			}

		} else if s.TS().Day() == db.last.Day() {
			dayBkt, err := tx.CreateBucketIfNotExists(bktDay)
			if err != nil {
				return err
			}
			dayLbl := s.TimeLabel(dayFmt)
			if err := db.updateVal(dayBkt, dayLbl, s); err != nil {
				return err
			}
		} else {
			monBkt, err := tx.CreateBucketIfNotExists(bktMon)
			if err != nil {
				return err
			}
			monLbl := s.TimeLabel(monFmt)
			if err := db.updateVal(monBkt, monLbl, s); err != nil {
				return err
			}
		}
		return nil
	})
}

//Rebase will swep through our time buckets and ensure that they only contain
//the appropriate entires.  Should be called each time the DB is opened
func (db *bwdb) Rebase(ts time.Time) error {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	if !db.open {
		return errNotOpen
	}

	if err := db.shiftIfOlder(bktMin, bktHour, prevHour(ts), hourFmt); err != nil {
		return err
	}
	if err := db.shiftIfOlder(bktHour, bktDay, prevDay(ts), dayFmt); err != nil {
		return err
	}
	if err := db.shiftIfOlder(bktDay, bktMon, prevMon(ts), monFmt); err != nil {
		return err
	}
	return nil
}

func (db *bwdb) shiftIfOlder(srcBktKey, dstBktKey []byte, tcheck time.Time, keyFmt string) error {
	s := db.newVar()
	return db.db.Batch(func(tx *bolt.Tx) error {
		srcBkt, err := tx.CreateBucketIfNotExists(srcBktKey)
		if err != nil {
			return err
		}
		dstBkt, err := tx.CreateBucketIfNotExists(dstBktKey)
		if err != nil {
			return err
		}
		return srcBkt.ForEach(func(k, v []byte) error {
			if err := s.Decode(v); err != nil {
				return err
			}
			//if its outside our window shift up
			if s.TS().Before(tcheck) {
				//delete from the old one
				if err := srcBkt.Delete(k); err != nil {
					return err
				}
				//push into the new one
				if err := db.updateVal(dstBkt, []byte(s.TS().Format(keyFmt)), s); err != nil {
					return err
				}
			}
			return nil
		})
	})
}

func (db *bwdb) sumAndShift(pullBkt, putBkt *bolt.Bucket, putKey []byte) error {
	//pull all the values out of the pullBkt and sum them
	s := db.newVar()
	err := pullBkt.ForEach(func(k, v []byte) error {
		sx := db.newVar()
		if err := sx.Decode(v); err != nil {
			return err
		}
		if err := s.Add(sx); err != nil {
			return err
		}
		s.SetTS(sx.TS())
		return pullBkt.Delete(k)
	})
	if err != nil {
		return err
	}
	return db.updateVal(putBkt, putKey, s) //if for some reason the key already exists, add it
}

func (db *bwdb) pullSet(bktName []byte) ([]Sample, error) {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	if !db.open {
		return nil, errNotOpen
	}
	var ss []Sample
	err := db.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bktName)
		if bkt == nil {
			return errNoBucket
		}
		return bkt.ForEach(func(k, v []byte) error {
			s := db.newVar()
			if err := s.Decode(v); err != nil {
				return err
			}
			ss = append(ss, s)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return ss, nil
}

func (db *bwdb) LiveSet() ([]Sample, error) {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	if !db.open {
		return nil, errNotOpen
	}
	var set []Sample
	for e := db.hist.Front(); e != nil; e = e.Next() {
		set = append(set, e.Value.(Sample))
	}
	return set, nil
}

//purge removes all entries from the bolt DB and live set
func (db *bwdb) purge() error {
	db.mtx.Lock()
	defer db.mtx.Unlock()
	if !db.open {
		return errNotOpen
	}
	//purge last update
	db.last = zeroTime

	//purge live
	db.hist = db.hist.Init()
	if db.hist.Len() != 0 {
		return errors.New("Failed to clear live set")
	}

	//roll through each bucket and delete its contents
	return db.db.Batch(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			return b.ForEach(func(k, _ []byte) error {
				return b.Delete(k)
			})
		})
	})
}

func (db *bwdb) Minutes() ([]Sample, error) {
	return db.pullSet(bktMin)
}

func (db *bwdb) Hours() ([]Sample, error) {
	return db.pullSet(bktHour)
}

func (db *bwdb) Days() ([]Sample, error) {
	return db.pullSet(bktDay)
}

func (db *bwdb) Months() ([]Sample, error) {
	return db.pullSet(bktMon)
}

func (db *bwdb) updateVal(bkt *bolt.Bucket, key []byte, s Sample) error {
	//attempt to get what is there
	v := bkt.Get(key)
	if v == nil {
		e := s.Encode()
		return bkt.Put(key, e)
	}
	sold := db.newVar()
	if err := sold.Decode(v); err != nil {
		return err
	}
	//WARNING: because s is an interface it is naturally a pointer, you MUST add to sold and NOT s
	sold.Add(s)
	e := sold.Encode()
	return bkt.Put(key, e)
}

/*
func printBucket(bkt *bolt.Bucket) {
	bkt.ForEach(func(k, v []byte) error {
		sx := &BWSample{}
		if err := sx.Decode(v); err != nil {
			return err
		}
		fmt.Printf("%s %d %d\n", sx.ts, sx.BytesUp, sx.BytesDown)
		return nil
	})
}
*/

//these functions are ghetto as shit, but I don't want to do the math...
func prevHour(ts time.Time) time.Time {
	tn, _ := time.Parse(hourFmt, ts.UTC().Format(hourFmt))
	return tn.Add(time.Hour)
}

func prevDay(ts time.Time) time.Time {
	tn, _ := time.Parse(dayFmt, ts.UTC().Format(dayFmt))
	return tn.Add(24*time.Hour)
}

func prevMon(ts time.Time) time.Time {
	year := ts.UTC().Year()
	month := ts.UTC().Month()
	if month == time.January {
		year -= 1
		month = time.December
	} else {
		month -= 1
	}
	tn, _ := time.Parse(`2006-01`, fmt.Sprintf("%04d-%02d", year, month))
	return tn
}
