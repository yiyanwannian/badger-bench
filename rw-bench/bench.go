package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/dgraph-io/badger-bench/rdb"
	"github.com/dgraph-io/badger-bench/store"
	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/y"
)

var (
	numKeys    = flag.Int("keys_mil", 1, "How many million keys to write.")
	valueSize  = flag.Int("valsz", 0, "Value size in bytes.")
	mil        = 1000000
	cpuprofile = flag.String("cpuprofile", "", "write cpu profile `file`")
	memprofile = flag.String("memprofile", "", "write memory profile to `file`")
)

type entry struct {
	Key   []byte
	Value []byte
	Meta  byte
}

func fillEntryWithIndex(e *entry, index int) {
	k := rand.Intn(*numKeys * mil * 10)
	key := fmt.Sprintf("vsz=%036d-k=%010d-%010d", *valueSize, k, index) // 64 bytes.
	if cap(e.Key) < len(key) {
		e.Key = make([]byte, 2*len(key))
	}
	e.Key = e.Key[:len(key)]
	copy(e.Key, key)

	rCnt := *valueSize
	p := make([]byte, rCnt)
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for i := 0; i < rCnt; i++ {
		p[i] = ' ' + byte(r.Intn('~'-' '+1))
	}
	e.Value = p[:*valueSize]

	//rCnt := 100
	//p := make([]byte, rCnt)
	//r := rand.New(rand.NewSource(time.Now().Unix()))
	//for i := 0; i < rCnt; i++ {
	//	p[i] = ' ' + byte(r.Intn('~'-' '+1))
	//}
	//
	//valueCap := *valueSize + (rCnt - *valueSize % rCnt)
	//b := make([]byte, 0, valueCap)
	//for len(b) < valueCap {
	//	b = append(b, p...)
	//}
	//e.Value = b[:*valueSize]
	e.Meta = 0
}

func fillEntry(e *entry) {
	k := rand.Intn(*numKeys * mil * 10)
	key := fmt.Sprintf("vsz=%047d-k=%010d", *valueSize, k) // 64 bytes.
	if cap(e.Key) < len(key) {
		e.Key = make([]byte, 2*len(key))
	}
	e.Key = e.Key[:len(key)]
	copy(e.Key, key)

	rand.Read(e.Value)
	e.Meta = 0
}

var bdg *badger.DB
var rocks *store.Store

func createEntries(entries []*entry) *rdb.WriteBatch {
	rb := rocks.NewWriteBatch()
	for _, e := range entries {
		fillEntry(e)
		rb.Put(e.Key, e.Value)
	}
	return rb
}

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	rand.Seed(time.Now().Unix())
	opt := badger.DefaultOptions("tmp/badger")
	// opt.MapTablesTo = table.Nothing
	opt.SyncWrites = false

	var err error
	y.Check(os.RemoveAll("tmp/badger"))
	os.MkdirAll("tmp/badger", 0777)
	bdg, err = badger.Open(opt)
	y.Check(err)

	y.Check(os.RemoveAll("tmp/rocks"))
	os.MkdirAll("tmp/rocks", 0777)
	rocks, err = store.NewStore("tmp/rocks")
	y.Check(err)

	unit := 10000
	total := *numKeys*unit

	keystart := time.Now()
	for i:= 0; i < 1000; i ++ {
		e := new(entry)
		fillEntryWithIndex(e, 1000 + i)
	}
	keytime := time.Since(keystart)
	keyus := keytime.Microseconds() * int64(total / 1000)

	fmt.Println("Num unique keys: ", total)
	fmt.Println("gen keys time: ", keyus)
	fmt.Println("Key size: ", 64)
	fmt.Println("Value size: ", *valueSize)

	fmt.Println("RocksDB:")
	rstart := time.Now()
	for i := 0; i < total; i ++ {
		e := new(entry)
		fillEntryWithIndex(e, i)
		rb := rocks.NewWriteBatch()
		rb.Put(e.Key, e.Value)
		y.Check(rocks.WriteBatch(rb))
		rb.Destroy()
		if i % unit == 0 {
			fmt.Println(fmt.Sprintf("rocksdb write %d st data", i))
		}
	}

	rstime := time.Since(rstart)
	fmt.Println("Total time: ", rstime)
	rocks.Close()

	fmt.Println("Badger:")
	bstart := time.Now()
	for i := 0; i < total; i ++ {
		txn := bdg.NewTransaction(true)
		e := new(entry)
		fillEntryWithIndex(e, i)
		y.Check(txn.Set(e.Key, e.Value))
		y.Check(txn.Commit())
		if i % unit == 0 {
			fmt.Println(fmt.Sprintf("badger write %d st data", i))
		}
	}
	bdg.Close()

	bstime := time.Since(bstart)
	fmt.Println("Total time: ",bstime)

	fmt.Println("\nTotal:", total)
	fmt.Println("Key size:", 64)
	fmt.Println("Value size:", *valueSize)
	fmt.Println("Rocksdb: ", rstime)
	fmt.Println("Badgerdb: ", bstime)

	fmt.Println(fmt.Sprintf("Rocksdb: %d s", (rstime.Microseconds() - keyus) / 1000000))
	fmt.Println(fmt.Sprintf("Badgerdb: %d s", (bstime.Microseconds() - keyus) / 1000000))
}


func main1() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	rand.Seed(time.Now().Unix())
	opt := badger.DefaultOptions("tmp/badger")
	// opt.MapTablesTo = table.Nothing
	opt.SyncWrites = false

	var err error
	y.Check(os.RemoveAll("tmp/badger"))
	os.MkdirAll("tmp/badger", 0777)
	bdg, err = badger.Open(opt)
	y.Check(err)

	y.Check(os.RemoveAll("tmp/rocks"))
	os.MkdirAll("tmp/rocks", 0777)
	rocks, err = store.NewStore("tmp/rocks")
	y.Check(err)

	tmp := 10000
	entries := make([]*entry, *numKeys*tmp)
	for i := 0; i < len(entries); i++ {
		e := new(entry)
		e.Key = make([]byte, 64)
		e.Value = make([]byte, *valueSize)
		entries[i] = e
	}
	rb := createEntries(entries)
	txn := bdg.NewTransaction(true)
	for _, e := range entries {
		y.Check(txn.Set(e.Key, e.Value))
	}

	fmt.Println("Key size:", len(entries[0].Key))
	fmt.Println("Value size:", *valueSize)
	fmt.Println("RocksDB:")
	rstart := time.Now()
	y.Check(rocks.WriteBatch(rb))
	count := *numKeys * tmp
	//var count int
	//ritr := rocks.NewIterator()
	//ristart := time.Now()
	//for ritr.SeekToFirst(); ritr.Valid(); ritr.Next() {
	//	_ = ritr.Key()
	//	count++
	//}
	fmt.Println("Num unique keys:", count)
	//fmt.Println("Iteration time: ", time.Since(ristart))
	fmt.Println("Total time: ", time.Since(rstart))
	rb.Destroy()
	rocks.Close()

	fmt.Println("Badger:")
	bstart := time.Now()
	y.Check(txn.Commit())
	//iopt := badger.IteratorOptions{}
	////bistart := time.Now()
	//iopt.PrefetchValues = false
	//iopt.PrefetchSize = 1000
	//txn = bdg.NewTransaction(false)
	//bitr := txn.NewIterator(iopt)
	//count = 0
	//for bitr.Rewind(); bitr.Valid(); bitr.Next() {
	//	_ = bitr.Item().Key()
	//	count++
	//}
	fmt.Println("Num unique keys:", count)
	//fmt.Println("Iteration time: ", time.Since(bistart))
	fmt.Println("Total time: ", time.Since(bstart))
	if *memprofile != "" {
		f, err := os.Create(*memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}
	bdg.Close()
}
