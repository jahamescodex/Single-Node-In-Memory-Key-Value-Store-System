package main

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

var globalSink []int
var mapTypes = []struct {
	name string
	init func(n int) Store
}{
	{
		name: "No-lock-stripping",
		init: func(n int) Store { return NewContactBookMap(n) },
	},
	{
		name: "With-lock-stripping",
		init: func(n int) Store { return NewShardedMap(16, n) },
	},
}

var loadCases = []int{100, 1000, 10000, 100000, 1000000}

var commandCases = []string{"Get"}

func BenchmarkColdStartAllocations(b *testing.B) {
	for _, mC := range mapTypes {
		for _, lC := range loadCases {
			benchMarkName := fmt.Sprintf("Cold-Start/%s/Load-%d", mC.name, lC)
			b.Run(benchMarkName, func(childB *testing.B) {

				key := make([][]byte, lC)
				val := make([][]byte, lC)

				for i := 0; i < lC; i++ {
					key[i] = []byte(fmt.Sprintf("Input-%v", i))
					val[i] = []byte(fmt.Sprintf("Val-%v", i))
				}

				workers := runtime.GOMAXPROCS(0)
				pairRange := lC / workers

				runtime.GC()
				childB.ReportAllocs() // cleans up physical ram (keeps the key and val but removes the string objects [ or ig structs ._. ])
				childB.ResetTimer()   // does not clean up physical ram, just the metrics (the results)

				for i := 0; i < childB.N; i++ { // simulates .RunParallel()
					mapToTest := mC.init(lC)
					var wg sync.WaitGroup
					wg.Add(workers)

					for w := 0; w < workers; w++ {
						start := w * pairRange
						end := start + pairRange
						if w == workers-1 {
							end = lC
						}

						go func(low, high int) {
							defer wg.Done()
							for k := low; k < high; k++ {
								mapToTest.Set(key[k], val[k])
							}
						}(start, end)
					}
					wg.Wait()
				}
			})
		}
	}
}

func BenchmarkMapCommands(b *testing.B) {
	for _, mT := range mapTypes {
		for _, lC := range loadCases {
			for _, cName := range commandCases {
				benchMarkName := fmt.Sprintf("%s/%s/Load-%d", mT.name, cName, lC)
				b.Run(benchMarkName, func(childB *testing.B) {
					mapToTest := mT.init(lC)

					key := make([][]byte, lC)
					val := make([][]byte, lC)

					for i := 0; i < lC; i++ {
						key[i] = []byte(fmt.Sprintf("Input-%v", i))
						val[i] = []byte(fmt.Sprintf("Val-%v", i))
					}

					for k := 0; k < lC; k++ {
						mapToTest.Set(key[k], val[k])
					}

					var safeIndex atomic.Uint32
					var globalIdx atomic.Uint32
					workers := runtime.GOMAXPROCS(0)
					globalSink = make([]int, workers)

					var bufferPool = sync.Pool{
						New: func() any {
							bufferPtr := make([]byte, 1024)
							return &bufferPtr
						},
					}

					runtime.GC()
					childB.ReportAllocs()
					childB.ResetTimer()

					b.RunParallel(func(pb *testing.PB) {
						localIdx := int(globalIdx.Add(1) - 1)
						var accumulator int
						for pb.Next() {
							idx := int(safeIndex.Add(1)-1) % lC

							buff := bufferPool.Get().(*[]byte)
							buffHeader := (*buff)

							localFaucet, found := mapToTest.Get(key[idx], buffHeader)
							if found {
								accumulator += len(localFaucet)
							}
							*buff = (*buff)[:cap(*buff)]
							clear(*buff)
							bufferPool.Put(buff)
						}
						globalSink[localIdx] = accumulator
					})
				})
			}

		}
	}
}
