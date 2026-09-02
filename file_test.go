package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

var globalSink []byte

// var bruh sync.Once

//	func init() {
//		log.Println("Set Test")
//	}

func BenchmarkTests(b *testing.B) {
	// mTestBookMap := NewContactBookMap()

	// mKey := make([][]byte, 1000)
	// mValue := make([][]byte, 1000)
	// for i := 0; i < 1000; i++ {
	// 	key[i] = []byte(fmt.Sprintf("User-%v", i))
	// 	value[i] = []byte(fmt.Sprintf("User-%v", i))
	// }

	// bruh.Do(func() {
	// 	b.Log("Set Test") // do this or we can do func init()
	// })

	b.Run("Set Test", func(childB *testing.B) {
		testBookMap := NewContactBookMap(childB.N)

		key := make([][]byte, childB.N)
		value := make([][]byte, childB.N)

		for i := 0; i < childB.N; i++ {
			key[i] = []byte(fmt.Sprintf("Client-%v", i))
			value[i] = []byte(fmt.Sprintf("Customer-%v", i))
		}

		var iteration atomic.Uint64

		childB.ResetTimer()
		childB.ReportAllocs()

		childB.RunParallel(func(setPb *testing.PB) {
			for setPb.Next() {
				idx := iteration.Add(1) - 1
				testBookMap.Set(key[int(idx)], value[int(idx)])
			}
		})
	})

	b.Run("Get Test", func(childB *testing.B) {
		testBookMap := NewContactBookMap(childB.N)
		mutex := sync.Mutex{}

		key := make([][]byte, childB.N)
		value := make([][]byte, childB.N)

		for i := 0; i < childB.N; i++ {
			key[i] = []byte(fmt.Sprintf("User-%v", i))
			value[i] = []byte(fmt.Sprintf("User-%v", i))
			testBookMap.Set(key[i], value[i])
		}

		var iteration atomic.Uint64

		childB.ResetTimer()
		childB.ReportAllocs()

		childB.RunParallel(func(getPb *testing.PB) {
			var localFaucet []byte
			localDst := make([]byte, 1024)

			// still gets allocated | 10 * (1024) B / b.N (# of operations) rounded to integers

			for getPb.Next() {
				faucet, status := testBookMap.Get(key[int(iteration.Add(1)-1)], localDst)
				if status {
					localFaucet = faucet
				}
			}
			mutex.Lock()
			globalSink = localFaucet
			mutex.Unlock()
		})
	})

	b.Run("Delete Test", func(childB *testing.B) {
		testBookMap := NewContactBookMap(childB.N)

		keys := make([][]byte, childB.N)
		values := make([][]byte, childB.N)

		for i := 0; i < childB.N; i++ {
			keys[i] = []byte(fmt.Sprintf("User-%v", i))
			values[i] = []byte(fmt.Sprintf("User-%v", i))
			testBookMap.Set(keys[i], values[i])
		}

		var iteration atomic.Uint64

		childB.ReportAllocs()
		childB.ResetTimer()

		childB.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				testBookMap.Delete(keys[int(iteration.Add(1)-1)])
			}
		})
	})
}
