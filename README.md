### Brief
Initially, this project was for the MVCC chapter as a part of a series of projects i was working on per-chapter while reading the "Database Internals" book. then I took a look at [etcd](https://github.com/etcd-io/etcd) and thoguht why not make it a kv-store. Later on it changed into a big dive about concurrency.

### Now, What?
I wanna reimplement a lot of stuff here from scratch rather than using libraries, and just experiment more with the storage component itself.

### The Benchmark
arc-swap vs a RwLock, same BTreeMap backend behind both. Speedup = arc-swap ops/s divided by locked ops/s. Bigger than 1 means arc-swap wins, smaller means locked wins.

| workload            | 1 worker | 2 workers | 4 workers |
|---------------------|----------|-----------|-----------|
| read-only           | 0.86x    | 0.84x     | 0.94x     |
| 95% read            | 0.87x    | 2.35x     | 2.42x     |
| 80% read            | 0.74x    | 3.57x     | 3.48x     |
| 50/50               | 0.61x    | 3.37x     | 4.11x     |
| 90% write           | 0.48x    | 3.14x     | 3.40x     |
| 80% read, hot key   | 0.50x    | 2.40x     | 1.96x     |


With 1 worker, since arc-swap has more overhead, it takes more time. And another issue with it is that with a hot key, contention is high, and sync between cpu cores would affect the performance, which we can see more clearly from a `50/50` case, being a 4x, to halving to 2x in a hot key workload. the `read-only` and `95% read` are here to show how little writes need to see benefits from that arc-swap solution.
