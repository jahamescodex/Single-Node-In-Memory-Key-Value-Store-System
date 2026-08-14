package main

import (
	"fmt"
	"sync"
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
		testBookMap := NewContactBookMap()

		key := make([][]byte, 1000)
		value := make([][]byte, 1000)

		for i := 0; i < 1000; i++ {
			key[i] = []byte(fmt.Sprintf("User-%v", i))
			value[i] = []byte(fmt.Sprintf("User-%v", i))
		}

		childB.ResetTimer()
		childB.ReportAllocs()

		childB.RunParallel(func(setPb *testing.PB) {
			iteration := 0
			for setPb.Next() {
				iteration = iteration % len(key)
				testBookMap.Set(key[iteration], value[iteration])
				iteration++
			}
		})
	})

	b.Run("Get Test", func(childB *testing.B) {
		testBookMap := NewContactBookMap()
		mutex := sync.Mutex{}

		key := make([][]byte, 1000)
		value := make([][]byte, 1000)

		for i := 0; i < 1000; i++ {
			key[i] = []byte(fmt.Sprintf("User-%v", i))
			value[i] = []byte(fmt.Sprintf("User-%v", i))
			testBookMap.Set(key[i], value[i])
		}

		childB.ResetTimer()
		childB.ReportAllocs()

		childB.RunParallel(func(getPb *testing.PB) {
			var localFaucet []byte
			localDst := make([]byte, 1024)
			iteration := 0

			// still gets allocated | 10 * (1024) B / b.N (# of operations) rounded to integers

			for getPb.Next() {
				iteration = iteration % len(key)
				faucet, status := testBookMap.Get(key[iteration], localDst)
				if status {
					localFaucet = faucet
				}
				iteration++
			}
			mutex.Lock()
			globalSink = localFaucet
			mutex.Unlock()

		})
	})
}
