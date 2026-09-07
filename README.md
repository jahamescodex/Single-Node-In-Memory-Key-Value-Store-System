An Ultra-low Allocation In-memory KV store (In active development)

**CURRENT PROJECT STATUS:** I am actively refactoring and building the Single-Node storage core (data engine). So expect lots of bugs. To take it one step further, I am attempting to shift the Single-Node to a distributed model architecture.

**CURRENTLY WORKING ON:**
- [x] Take out worker pools (bottlknecks for I/O bound architecture); utilized goroutine per connection
- [x] Protect internal map in store.go with 'sync.RWMutex'; heavy read/write isolation
- [x] Isolate write-paths to eliminte heap escape analysis
- [x] Implement 'sync.pool' to reuse transient buffer slices for incoming connections
- [x] Replace high-overhead string conversions with 'bytes' stack operations
- [x] Implement Graceful shutdown (3 step teardown)
- [x] Benchmark pre-sharded lock
- [x] Benchmark sharded lock
- [] Compare and create a mini-study of improved lock contention
