module github.com/dgraph-io/badger-bench

go 1.16

require (
	github.com/HdrHistogram/hdrhistogram-go v1.1.0
	github.com/bmatsuo/lmdb-go v1.8.0
	github.com/boltdb/bolt v1.3.1
	github.com/cockroachdb/c-jemalloc v0.0.0-20170411174633-bc4025230132 // indirect
	github.com/cockroachdb/c-lz4 v0.0.0-20160606191938-834d3303c9e8
	github.com/cockroachdb/c-rocksdb v0.0.0-20170329231916-0dd42399d1f0
	github.com/cockroachdb/c-snappy v0.0.0-20161124051749-c0cd3c9ce92f
	github.com/dgraph-io/badger/v3 v3.0.0-00010101000000-000000000000
	//github.com/dgraph-io/badger v1.6.2
	github.com/paulbellamy/ratecounter v0.2.0
	github.com/pkg/profile v1.6.0
	github.com/stretchr/testify v1.7.0
	github.com/syndtr/goleveldb v1.0.0
	github.com/traetox/goaio v0.0.0-20171005222435-46641abceb17
	golang.org/x/net v0.0.0-20210614182718-04defd469f4e
)

replace github.com/dgraph-io/badger/v3 => ../../../github.com/dgraph-io/badger
