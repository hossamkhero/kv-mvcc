use kv_mvcc::store::{
    ArcSwapBTree,
    LockedBTree,
    Store,
};

use std::{
    sync::Arc,
    time::Duration,
};

use tokio::{
    sync::{watch, Barrier},
    task::JoinSet,
    time::{sleep_until, Instant},
};

/* -------------------------------------------------------------------------- */
/*                              Benchmark config                              */
/* -------------------------------------------------------------------------- */

const KEY_COUNT: usize = 16_384;
const VALUE_COUNT: usize = 1_024;
const VALUE_BYTES: usize = 64;

// Number of pre-generated operations per worker.
const TRACE_LENGTH: usize = 65_536;

const WARMUP_SECONDS: u64 = 1;
const TRIAL_SECONDS: u64 = 5;
const REPETITIONS: usize = 5;

// Profiling uses one long, isolated measurement so the sampled stacks are not
// mixed with the other workloads and backends from the benchmark matrix.
const PROFILE_WORKERS: usize = 4;
const PROFILE_SECONDS: u64 = 30;

// Check the clock once per this many operations.
const TIME_CHECK_INTERVAL: usize = 64;

/* -------------------------------------------------------------------------- */
/*                                  Workload                                  */
/* -------------------------------------------------------------------------- */

#[derive(Clone, Copy)]
enum OpKind {
    Get,
    Update,
}

#[derive(Clone, Copy)]
struct BenchOp {
    kind: OpKind,
    key_index: u32,
    value_index: u32,
}

#[derive(Clone, Copy)]
enum KeyDistribution {
    Uniform,
    SingleHotKey,
}

#[derive(Clone, Copy)]
struct WorkloadSpec {
    name: &'static str,
    get_percent: u8,
    distribution: KeyDistribution,
}

struct Workload {
    keys: Vec<String>,
    values: Vec<String>,

    // One operation trace per worker.
    traces: Vec<Vec<BenchOp>>,
}

const WORKLOADS: &[WorkloadSpec] = &[
    WorkloadSpec {
        name: "read_only_uniform",
        get_percent: 100,
        distribution: KeyDistribution::Uniform,
    },
    WorkloadSpec {
        name: "read_heavy_uniform",
        get_percent: 95,
        distribution: KeyDistribution::Uniform,
    },
    WorkloadSpec {
        name: "mixed_uniform",
        get_percent: 50,
        distribution: KeyDistribution::Uniform,
    },
    WorkloadSpec {
        name: "update_heavy_uniform",
        get_percent: 10,
        distribution: KeyDistribution::Uniform,
    },
    WorkloadSpec {
        name: "uniform_80_read_20_update",
        get_percent: 80,
        distribution: KeyDistribution::Uniform,
    },
    WorkloadSpec {
        name: "hot_key_80_read_20_update",
        get_percent: 80,
        distribution: KeyDistribution::SingleHotKey,
    },
];

/* -------------------------------------------------------------------------- */
/*                         Deterministic random numbers                        */
/* -------------------------------------------------------------------------- */

/// A small deterministic generator.
///
/// It is used only while constructing traces, never in the measured region.
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_add(0x9e37_79b9_7f4a_7c15);

        let mut value = self.state;

        value = (value ^ (value >> 30))
            .wrapping_mul(0xbf58_476d_1ce4_e5b9);

        value = (value ^ (value >> 27))
            .wrapping_mul(0x94d0_49bb_1331_11eb);

        value ^ (value >> 31)
    }
}

/* -------------------------------------------------------------------------- */
/*                         Workload trace generation                          */
/* -------------------------------------------------------------------------- */

fn make_value(index: usize) -> String {
    let prefix = format!("{index:016x}");
    prefix.repeat(VALUE_BYTES.div_ceil(prefix.len()))[..VALUE_BYTES].to_owned()
}

fn generate_workload(
    spec: WorkloadSpec,
    worker_count: usize,
) -> Workload {
    let keys = (0..KEY_COUNT)
        .map(|index| format!("key-{index:08}"))
        .collect();

    let values = (0..VALUE_COUNT)
        .map(make_value)
        .collect();

    let traces = (0..worker_count)
        .map(|worker_id| {
            generate_trace(
                spec,
                0x1234_5678_u64.wrapping_add(worker_id as u64),
            )
        })
        .collect();

    Workload {
        keys,
        values,
        traces,
    }
}

fn generate_trace(
    spec: WorkloadSpec,
    seed: u64,
) -> Vec<BenchOp> {
    let mut random = SplitMix64::new(seed);
    let mut trace = Vec::with_capacity(TRACE_LENGTH);

    for _ in 0..TRACE_LENGTH {
        let kind = if random.next() % 100 < spec.get_percent as u64 {
            OpKind::Get
        } else {
            OpKind::Update
        };

        let key_index = match spec.distribution {
            KeyDistribution::Uniform => {
                random.next() as usize % KEY_COUNT
            }

            KeyDistribution::SingleHotKey => 0,
        };

        let value_index = random.next() as usize % VALUE_COUNT;

        trace.push(BenchOp {
            kind,
            key_index: key_index as u32,
            value_index: value_index as u32,
        });
    }

    trace
}

/* -------------------------------------------------------------------------- */
/*                          Operation execution path                          */
/* -------------------------------------------------------------------------- */

async fn apply<S: Store>(
    store: &S,
    workload: &Workload,
    operation: BenchOp,
) -> u64 {
    let key = &workload.keys[operation.key_index as usize];
    let value = &workload.values[operation.value_index as usize];

    match operation.kind {
        OpKind::Get => {
            store
                .get(key)
                .await
                .map_or(0, |value| value.len() as u64)
        }

        OpKind::Update => {
            store.update(key, value).await as u64
        }
    }
}

/* -------------------------------------------------------------------------- */
/*                              Store preparation                             */
/* -------------------------------------------------------------------------- */

async fn prefill<S: Store>(
    store: &S,
    workload: &Workload,
) {
    let initial_value = &workload.values[0];

    for key in &workload.keys {
        assert!(store.add(key, initial_value).await);
    }
}

/* -------------------------------------------------------------------------- */
/*                                Trial runner                                */
/* -------------------------------------------------------------------------- */

struct WorkerResult {
    completed: u64,
    checksum: u64,
    finished: Instant,
}

struct TrialResult {
    operations: u64,
    elapsed: Duration,
    checksum: u64,
}

impl TrialResult {
    fn operations_per_second(&self) -> f64 {
        self.operations as f64 / self.elapsed.as_secs_f64()
    }
}

async fn run_trial<S>(
    store: Arc<S>,
    workload: Arc<Workload>,
    worker_count: usize,
    duration: Duration,
) -> TrialResult
  where
    S: Store + 'static,
{
    let barrier = Arc::new(Barrier::new(worker_count + 1));

    // Workers wait for the benchmark controller to publish a start time.
    let (start_sender, start_receiver) =
    watch::channel(None::<Instant>);

    let mut workers = JoinSet::new();

    for worker_id in 0..worker_count {
        let store = Arc::clone(&store);
        let workload = Arc::clone(&workload);
        let barrier = Arc::clone(&barrier);
        let mut start_receiver = start_receiver.clone();

        workers.spawn(async move {
            let trace = &workload.traces[worker_id];

            let mut trace_position = 0;
            let mut completed = 0_u64;
            let mut checksum = 0_u64;

            barrier.wait().await;

            start_receiver.changed().await.unwrap();

            let start = start_receiver
                .borrow()
                .expect("controller must publish start time");

            sleep_until(start).await;

            let deadline = start + duration;

            loop {
                for _ in 0..TIME_CHECK_INTERVAL {
                    let operation = trace[trace_position];

                    trace_position += 1;

                    if trace_position == trace.len() {
                        trace_position = 0;
                    }

                    checksum ^= apply(
                        &*store,
                        &workload,
                        operation,
                    )
                    .await;

                    completed += 1;
                }

                if Instant::now() >= deadline {
                    break;
                }
            }

            WorkerResult {
                completed,
                checksum,
                finished: Instant::now(),
            }
        });
    }

    // Ensure every worker was spawned and reached the barrier.
    barrier.wait().await;

    let start = Instant::now() + Duration::from_millis(50);

    start_sender.send(Some(start)).unwrap();

    let mut total_operations = 0_u64;
    let mut combined_checksum = 0_u64;
    let mut latest_finish = start;

    while let Some(result) = workers.join_next().await {
        let result = result.expect("benchmark worker panicked");

        total_operations += result.completed;
        combined_checksum ^= result.checksum;
        latest_finish = latest_finish.max(result.finished);
    }

    // Make the accumulated read results externally observable.
    std::hint::black_box(combined_checksum);

    TrialResult {
        operations: total_operations,
        elapsed: latest_finish - start,
        checksum: combined_checksum,
    }
}

/* -------------------------------------------------------------------------- */
/*                            Backend trial wrapper                           */
/* -------------------------------------------------------------------------- */

async fn benchmark_backend<S>(
    workload: Arc<Workload>,
    worker_count: usize,
    duration: Duration,
) -> TrialResult
  where
    S: Store + Default + 'static,
{
    let store = Arc::new(S::default());

    prefill(&*store, &workload).await;

    run_trial(
        store,
        workload,
        worker_count,
        duration,
    )
    .await
}

fn print_result(
    backend: &str,
    workload: &str,
    workers: usize,
    trial: usize,
    result: &TrialResult,
) {
    println!(
        "{backend},{workload},{workers},{KEY_COUNT},{VALUE_BYTES},\
            {trial},{},{},{:.2},{}",
        result.operations,
        result.elapsed.as_nanos(),
        result.operations_per_second(),
        result.checksum,
    );
}

/* -------------------------------------------------------------------------- */
/*                            Benchmark experiment                            */
/* -------------------------------------------------------------------------- */

async fn run_benchmarks() {
    let available_cpus = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1);

    let mut worker_counts = Vec::new();
    let mut count = 1;

    while count <= available_cpus {
        worker_counts.push(count);
        count *= 2;
    }

    if !worker_counts.contains(&available_cpus) {
        worker_counts.push(available_cpus);
    }

    println!(
    "backend,workload,workers,keys,value_bytes,trial,\
        operations,elapsed_ns,ops_per_sec,checksum"
);

    for spec in WORKLOADS {
        for &worker_count in &worker_counts {
            let workload = Arc::new(
                generate_workload(*spec, worker_count)
            );

            // Warm up each backend/configuration without reporting it.
            let _ = benchmark_backend::<LockedBTree>(
                Arc::clone(&workload),
                worker_count,
                Duration::from_secs(WARMUP_SECONDS),
            )
            .await;

            let _ = benchmark_backend::<ArcSwapBTree>(
                Arc::clone(&workload),
                worker_count,
                Duration::from_secs(WARMUP_SECONDS),
            )
            .await;

            for trial in 0..REPETITIONS {
                // Alternate order to reduce temperature/frequency bias.
                if trial % 2 == 0 {
                    let plain = benchmark_backend::<LockedBTree>(
                        Arc::clone(&workload),
                        worker_count,
                        Duration::from_secs(TRIAL_SECONDS),
                    )
                    .await;

                    print_result(
                        "locked_btree",
                        spec.name,
                        worker_count,
                        trial,
                        &plain,
                    );

                    let arc_swap = benchmark_backend::<ArcSwapBTree>(
                        Arc::clone(&workload),
                        worker_count,
                        Duration::from_secs(TRIAL_SECONDS),
                    )
                    .await;

                    print_result(
                        "arc_swap_btree",
                        spec.name,
                        worker_count,
                        trial,
                        &arc_swap,
                    );
                } else {
                    let arc_swap = benchmark_backend::<ArcSwapBTree>(
                        Arc::clone(&workload),
                        worker_count,
                        Duration::from_secs(TRIAL_SECONDS),
                    )
                    .await;

                    print_result(
                        "arc_swap_btree",
                        spec.name,
                        worker_count,
                        trial,
                        &arc_swap,
                    );

                    let plain = benchmark_backend::<LockedBTree>(
                        Arc::clone(&workload),
                        worker_count,
                        Duration::from_secs(TRIAL_SECONDS),
                    )
                    .await;

                    print_result(
                        "locked_btree",
                        spec.name,
                        worker_count,
                        trial,
                        &plain,
                    );
                }
            }
        }
    }
}

async fn run_profile<S>()
where
    S: Store + Default + 'static,
{
    let available_cpus = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1);

    let worker_count = PROFILE_WORKERS.min(available_cpus);
    let spec = WORKLOADS
        .iter()
        .copied()
        .find(|spec| spec.name == "read_only_uniform")
        .expect("read_only_uniform workload must exist");

    let workload = Arc::new(generate_workload(spec, worker_count));
    let store = Arc::new(S::default());

    prefill(store.as_ref(), workload.as_ref()).await;

    // Activate the runtime workers and warm the same read-only store before
    // collecting the longer profiling run.
    let _ = run_trial(
        Arc::clone(&store),
        Arc::clone(&workload),
        worker_count,
        Duration::from_secs(WARMUP_SECONDS),
    )
    .await;

    eprintln!(
        "profiling read_only_uniform with {worker_count} workers for {PROFILE_SECONDS}s"
    );

    let result = run_trial(
        store,
        workload,
        worker_count,
        Duration::from_secs(PROFILE_SECONDS),
    )
    .await;

    eprintln!(
        "profile run completed: {:.2} ops/s",
        result.operations_per_second()
    );
}

fn main() {
    let runtime_workers = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(runtime_workers)
        .enable_time()
        .build()
        .unwrap();

    let arguments = std::env::args().skip(1).collect::<Vec<_>>();

    match arguments.as_slice() {
        [] => runtime.block_on(run_benchmarks()),
        [mode, backend] if mode == "profile" && backend == "locked" => {
            runtime.block_on(run_profile::<LockedBTree>());
        }
        [mode, backend] if mode == "profile" && backend == "arc-swap" => {
            runtime.block_on(run_profile::<ArcSwapBTree>());
        }
        _ => {
            eprintln!("usage:");
            eprintln!("  cargo bench --bench bench");
            eprintln!("  cargo bench --bench bench -- profile locked");
            eprintln!("  cargo bench --bench bench -- profile arc-swap");
            std::process::exit(2);
        }
    }
}
