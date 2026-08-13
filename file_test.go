package main

import (
	"fmt"
	"testing"
)

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

		for i := 0; i < b.N; i++ {
			key[i] = []byte(fmt.Sprintf("User-%v", i))
			value[i] = []byte(fmt.Sprintf("User-%v", i))
		}

		childB.ResetTimer()
		childB.ReportAllocs()

		childB.RunParallel(func(setPb *testing.PB) {
			iteration := 0
			for setPb.Next() {
				iteration := iteration % len(key)
				testBookMap.Set(key[iteration], value[iteration])
				iteration++
			}
		})

	})
}
