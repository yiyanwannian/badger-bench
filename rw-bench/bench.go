package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/dgraph-io/badger-bench/rdb"
	"github.com/dgraph-io/badger-bench/store"
	"github.com/dgraph-io/badger/v3"
	"github.com/dgraph-io/badger/v3/y"
)

var (
	numKeys = flag.Int("keys_mil", 1, "How many million keys to write.")
	// valueSize = flag.Int("valsz", 0, "Value size in bytes.")
	mil = 1000000
	// cpuprofile = flag.String("cpuprofile", "", "write cpu profile `file`")
	// memprofile = flag.String("memprofile", "", "write memory profile to `file`")

	valueSize = flag.Int("valsz", 0, "Value size in bytes.")
	start     = flag.Int("start", 1, "data write count range start.")
	end       = flag.Int("end", 1, "data write count range end.")
	sp        = flag.Int("skip", 1, "How many million keys grow skip.")
	bsize     = flag.Int("batchSize", 1, "How many keys each batch write.")
)

type entry struct {
	Key   []byte
	Value []byte
	Meta  byte
}

func fillEntryWithIndex(e *entry, valueSz, index int) {
	k := rand.Intn(*numKeys * mil * 10)
	key := fmt.Sprintf("vsz=%036d-k=%010d-%010d", *valueSize, k, index) // 64 bytes.
	if cap(e.Key) < len(key) {
		e.Key = make([]byte, 2*len(key))
	}
	e.Key = e.Key[:len(key)]
	copy(e.Key, key)

	rCnt := valueSz
	p := make([]byte, rCnt)
	r := rand.New(rand.NewSource(time.Now().Unix()))
	for i := 0; i < rCnt; i++ {
		p[i] = ' ' + byte(r.Intn('~'-' '+1))
	}
	e.Value = p[:valueSz]

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
	// valueSz := 1024
	// dataCntRange := 10
	// skip := 1
	// batchSize := 10000

	flag.Parse()
	valueSz := *valueSize
	dataRangeStart := *start
	dataRangeEnd := *end
	skip := *sp
	batchSize := *bsize

	fmt.Printf("valueSz: %d\n", valueSz)
	fmt.Printf("dataRangeStart: %d\n", dataRangeStart)
	fmt.Printf("dataRangeEnd: %d\n", dataRangeEnd)
	fmt.Printf("skip: %d\n", skip)
	fmt.Printf("batchSize: %d\n", batchSize)

	if dataRangeStart < 1 {
		dataRangeStart = 1
	}

	if dataRangeEnd < dataRangeStart {
		dataRangeEnd = dataRangeStart
	}

	dataRangeCnt := dataRangeEnd - dataRangeStart
	badgerTimes := make([]float64, 0, dataRangeCnt)
	rocksdbTimes := make([]float64, 0, dataRangeCnt)
	for i := dataRangeStart; i <= dataRangeEnd; i++ {
		rt, bt := bench_test(i*skip, valueSz, batchSize)
		rocksdbTimes = append(rocksdbTimes, rt)
		badgerTimes = append(badgerTimes, bt)
	}

	for i := 0; i < len(badgerTimes); i++ {
		fmt.Printf("total: %d, badgerTime: %f μs/op, rocksdbTime: %f μs/op\n",
			(i+dataRangeStart)*batchSize*skip, badgerTimes[i], rocksdbTimes[i])
	}
}

func bench_test(dataCnt, valuesz, batchSize int) (rocksdbTime, badgerTime float64) {
	total := dataCnt * batchSize

	rand.Seed(time.Now().Unix())
	bpath := fmt.Sprintf("tmp/badger-%dw", dataCnt)
	opt := badger.DefaultOptions(bpath)
	// opt.MapTablesTo = table.Nothing
	opt.SyncWrites = false

	var err error

	//y.Check(os.RemoveAll("tmp/badger"))
	os.MkdirAll(bpath, 0777)
	bdg, err = badger.Open(opt)
	y.Check(err)

	//y.Check(os.RemoveAll("tmp/rocks"))
	rpath := fmt.Sprintf("tmp/rocks-%dw", dataCnt)
	os.MkdirAll(rpath, 0777)
	rocks, err = store.NewStore(rpath)
	y.Check(err)

	fmt.Println("Num unique keys: ", total)
	fmt.Println("each batch: ", batchSize)
	fmt.Println("Key size: ", 64)
	fmt.Println("Value size: ", valuesz)

	fmt.Println("RocksDB:")
	rtotalWriteTime := float64(0)
	rstart := time.Now()
	for i := 1; i <= dataCnt; i++ {
		entries := make([]*entry, 0, batchSize)
		for k := 0; k < batchSize; k++ {
			e := new(entry)
			fillEntryWithIndex(e, valuesz, k)
			entries = append(entries, e)
		}

		wstart := time.Now()
		rb := rocks.NewWriteBatch()
		for j := 0; j < batchSize; j++ {
			rb.Put(entries[j].Key, entries[j].Value)
		}

		y.Check(rocks.WriteBatch(rb))
		rb.Destroy()
		wend := time.Since(wstart)
		fmt.Printf("rocksdb write %d st data\n", i)
		rtotalWriteTime = rtotalWriteTime + float64(wend.Microseconds())
	}

	fmt.Printf("Total rocksdb write time: %f ms\n", rtotalWriteTime/1000)
	rtotalWriteTime = rtotalWriteTime / float64(total)
	fmt.Printf("Each rocksdb write time: %f μs/op\n", rtotalWriteTime)
	fmt.Println("Total rocksdb time: ", time.Since(rstart))
	rocks.Close()

	fmt.Println("Badger:")
	bstart := time.Now()
	btotalWriteTime := float64(0)
	for i := 0; i < dataCnt; i++ {
		entries := make([]*entry, 0, batchSize)
		for k := 0; k < batchSize; k++ {
			e := new(entry)
			fillEntryWithIndex(e, valuesz, k)
			entries = append(entries, e)
		}

		wstart := time.Now()
		wb := bdg.NewWriteBatch()
		//txn := bdg.NewTransaction(true)
		for j := 0; j < batchSize; j++ {
			y.Check(wb.Set(entries[j].Key, entries[j].Value))
			//y.Check(txn.Set(e.Key, e.Value))
		}
		y.Check(wb.Flush())
		//y.Check(txn.Commit())
		wend := time.Since(wstart)
		fmt.Printf("badger write %d st data\n", i)
		btotalWriteTime = btotalWriteTime + float64(wend.Microseconds())
	}
	fmt.Printf("Total badger write time: %f ms\n", btotalWriteTime/1000)
	btotalWriteTime = btotalWriteTime / float64(total)
	fmt.Printf("Each badger write time: %f μs/op\n", btotalWriteTime)
	fmt.Println("Total badger time: ", time.Since(bstart))
	bdg.Close()

	fmt.Println("\nTotal:", total)
	fmt.Println("Key size:", 64)
	fmt.Println("Value size:", valuesz)
	fmt.Printf("Cgorocksdb write: %f μs/op\n", rtotalWriteTime)
	fmt.Printf("Badgerdb write: %f μs/op\n", btotalWriteTime)

	return rtotalWriteTime, btotalWriteTime
}

//func main_put() {
//	flag.Parse()
//	if *cpuprofile != "" {
//		f, err := os.Create(*cpuprofile)
//		if err != nil {
//			log.Fatal("could not create CPU profile: ", err)
//		}
//		if err := pprof.StartCPUProfile(f); err != nil {
//			log.Fatal("could not start CPU profile: ", err)
//		}
//		defer pprof.StopCPUProfile()
//	}
//
//	rand.Seed(time.Now().Unix())
//	opt := badger.DefaultOptions("tmp/badger")
//	// opt.MapTablesTo = table.Nothing
//	opt.SyncWrites = false
//
//	var err error
//	y.Check(os.RemoveAll("tmp/badger"))
//	os.MkdirAll("tmp/badger", 0777)
//	bdg, err = badger.Open(opt)
//	y.Check(err)
//
//	y.Check(os.RemoveAll("tmp/rocks"))
//	os.MkdirAll("tmp/rocks", 0777)
//	rocks, err = store.NewStore("tmp/rocks")
//	y.Check(err)
//
//	batchCnt := 10000
//	total := *numKeys* batchCnt
//
//	fmt.Println("Num unique keys: ", total)
//	fmt.Println("each batch: ", batchCnt)
//	fmt.Println("Key size: ", 64)
//	fmt.Println("Value size: ", *valueSize)
//
//	fmt.Println("RocksDB:")
//	rtotalWriteTime := int64(0)
//	rstart := time.Now()
//	var wstart time.Time
//	var wend time.Duration
//	for i := 0; i < *numKeys ; i ++ {
//		//rb := rocks.NewWriteBatch()
//		for j := 0; j < batchCnt; j ++ {
//			e := new(entry)
//			fillEntryWithIndex(e, i)
//			wstart = time.Now()
//			y.Check(rocks.SetOne(e.Key, e.Value))
//			wend = time.Since(wstart)
//			//rb.Put(e.Key, e.Value)
//			rtotalWriteTime = rtotalWriteTime + wend.Microseconds()
//		}
//		//wstart := time.Now()
//		//y.Check(rocks.WriteBatch(rb))
//		//wend := time.Since(wstart)
//		//rb.Destroy()
//		fmt.Println(fmt.Sprintf("rocksdb write %d st data", i))
//		//rtotalWriteTime = rtotalWriteTime + wend.Microseconds()
//	}
//	rtotalWriteTime = rtotalWriteTime / 1000.0
//	fmt.Println( fmt.Sprintf("Total write time: %d s", rtotalWriteTime))
//	fmt.Println("Total time: ", time.Since(rstart))
//	rocks.Close()
//
//	fmt.Println("Badger:")
//	bstart := time.Now()
//	btotalWriteTime := int64(0)
//	for i := 0; i < *numKeys ; i ++ {
//		//	wb := bdg.NewWriteBatch()
//		//	txn := bdg.NewTransaction(true)
//		for j := 0; j < batchCnt; j ++ {
//			e := new(entry)
//			fillEntryWithIndex(e, i)
//			//y.Check(wb.Set(e.Key, e.Value))
//			wstart = time.Now()
//			txn := bdg.NewTransaction(true)
//			y.Check(txn.Set(e.Key, e.Value))
//			y.Check(txn.Commit())
//			wend = time.Since(wstart)
//			btotalWriteTime = btotalWriteTime + wend.Microseconds()
//		}
//		//wstart := time.Now()
//		//y.Check(wb.Flush())
//		//y.Check(txn.Commit())
//		//wend := time.Since(wstart)
//		fmt.Println(fmt.Sprintf("badger write %d st data", i))
//		//btotalWriteTime = btotalWriteTime + wend.Microseconds()
//	}
//	btotalWriteTime = btotalWriteTime / 1000.0
//	fmt.Println(fmt.Sprintf("Total write time: %d s", btotalWriteTime))
//	fmt.Println("Total time: ",time.Since(bstart))
//	bdg.Close()
//
//	fmt.Println("\nTotal:", total)
//	fmt.Println("Key size:", 64)
//	fmt.Println("Value size:", *valueSize)
//	fmt.Println(fmt.Sprintf("Cgorocksdb write: %d s", rtotalWriteTime))
//	fmt.Println(fmt.Sprintf("Badgerdb write: %d s", btotalWriteTime))
//}

//func main1() {
//	flag.Parse()
//	if *cpuprofile != "" {
//		f, err := os.Create(*cpuprofile)
//		if err != nil {
//			log.Fatal("could not create CPU profile: ", err)
//		}
//		if err := pprof.StartCPUProfile(f); err != nil {
//			log.Fatal("could not start CPU profile: ", err)
//		}
//		defer pprof.StopCPUProfile()
//	}
//
//	rand.Seed(time.Now().Unix())
//	opt := badger.DefaultOptions("tmp/badger")
//	// opt.MapTablesTo = table.Nothing
//	opt.SyncWrites = false
//
//	var err error
//	y.Check(os.RemoveAll("tmp/badger"))
//	os.MkdirAll("tmp/badger", 0777)
//	bdg, err = badger.Open(opt)
//	y.Check(err)
//
//	y.Check(os.RemoveAll("tmp/rocks"))
//	os.MkdirAll("tmp/rocks", 0777)
//	rocks, err = store.NewStore("tmp/rocks")
//	y.Check(err)
//
//	tmp := 10000
//	entries := make([]*entry, *numKeys*tmp)
//	for i := 0; i < len(entries); i++ {
//		e := new(entry)
//		e.Key = make([]byte, 64)
//		e.Value = make([]byte, *valueSize)
//		entries[i] = e
//	}
//	rb := createEntries(entries)
//	txn := bdg.NewTransaction(true)
//	for _, e := range entries {
//		y.Check(txn.Set(e.Key, e.Value))
//	}
//
//	fmt.Println("Key size:", len(entries[0].Key))
//	fmt.Println("Value size:", *valueSize)
//	fmt.Println("RocksDB:")
//	rstart := time.Now()
//	y.Check(rocks.WriteBatch(rb))
//	count := *numKeys * tmp
//	//var count int
//	//ritr := rocks.NewIterator()
//	//ristart := time.Now()
//	//for ritr.SeekToFirst(); ritr.Valid(); ritr.Next() {
//	//	_ = ritr.Key()
//	//	count++
//	//}
//	fmt.Println("Num unique keys:", count)
//	//fmt.Println("Iteration time: ", time.Since(ristart))
//	fmt.Println("Total time: ", time.Since(rstart))
//	rb.Destroy()
//	rocks.Close()
//
//	fmt.Println("Badger:")
//	bstart := time.Now()
//	y.Check(txn.Commit())
//	//iopt := badger.IteratorOptions{}
//	////bistart := time.Now()
//	//iopt.PrefetchValues = false
//	//iopt.PrefetchSize = 1000
//	//txn = bdg.NewTransaction(false)
//	//bitr := txn.NewIterator(iopt)
//	//count = 0
//	//for bitr.Rewind(); bitr.Valid(); bitr.Next() {
//	//	_ = bitr.Item().Key()
//	//	count++
//	//}
//	fmt.Println("Num unique keys:", count)
//	//fmt.Println("Iteration time: ", time.Since(bistart))
//	fmt.Println("Total time: ", time.Since(bstart))
//	if *memprofile != "" {
//		f, err := os.Create(*memprofile)
//		if err != nil {
//			log.Fatal("could not create memory profile: ", err)
//		}
//		runtime.GC() // get up-to-date statistics
//		if err := pprof.WriteHeapProfile(f); err != nil {
//			log.Fatal("could not write memory profile: ", err)
//		}
//		f.Close()
//	}
//	bdg.Close()
//}
