use crate::value_generator::{
    BenchMapValue, BenchValue, MapInsertionKind, ValueGenerator, QUERY_COUNT, RANGE_LEN, SEED,
};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

pub const INSERT_BATCH_COUNT: usize = 1_024;
pub const INSERT_ONE_BATCH_SIZE: usize = 128;
pub const PARALLEL_OPERATION_COUNT: usize = 60_000;
const RANGE_QUERY_COUNT: usize = QUERY_COUNT;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scenario {
    InsertNew,
    InsertDuplicateHeavy,
    ContainsHit,
    ContainsMiss,
    RemoveHit,
    Range128,
    MixedReadHeavy,
    MixedBalanced,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MapScenario {
    InsertNew,
    InsertUpdateHeavy,
    GetHit,
    GetMiss,
    RemoveHit,
    Range128,
    MixedReadHeavy,
    MixedBalanced,
}

impl MapScenario {
    pub const ALL: [Self; 8] = [
        Self::InsertNew,
        Self::InsertUpdateHeavy,
        Self::GetHit,
        Self::GetMiss,
        Self::RemoveHit,
        Self::Range128,
        Self::MixedReadHeavy,
        Self::MixedBalanced,
    ];

    pub const CAPACITY_SWEEP: [Self; 3] = [Self::GetHit, Self::InsertNew, Self::MixedBalanced];

    pub fn id(self) -> &'static str {
        match self {
            Self::InsertNew => "insert/new",
            Self::InsertUpdateHeavy => "insert/update90",
            Self::GetHit => "get/hit",
            Self::GetMiss => "get/miss",
            Self::RemoveHit => "remove/hit",
            Self::Range128 => "range/128",
            Self::MixedReadHeavy => "mixed/random_read90",
            Self::MixedBalanced => "mixed/random_read50",
        }
    }

    pub fn operation_count(self) -> usize {
        match self {
            Self::InsertNew | Self::InsertUpdateHeavy => INSERT_BATCH_COUNT,
            Self::GetHit | Self::GetMiss | Self::MixedReadHeavy | Self::MixedBalanced => PARALLEL_OPERATION_COUNT,
            Self::RemoveHit | Self::Range128 => QUERY_COUNT,
        }
    }

    pub fn expected_successes(self) -> usize {
        match self {
            Self::InsertNew => INSERT_BATCH_COUNT,
            Self::InsertUpdateHeavy => INSERT_BATCH_COUNT / 10,
            Self::GetHit => PARALLEL_OPERATION_COUNT,
            Self::GetMiss => 0,
            Self::RemoveHit | Self::Range128 => QUERY_COUNT,
            Self::MixedReadHeavy => 49_200,
            Self::MixedBalanced => 54_000,
        }
    }

    pub fn expected_updates(self) -> usize {
        match self {
            Self::InsertUpdateHeavy => INSERT_BATCH_COUNT - INSERT_BATCH_COUNT / 10,
            Self::InsertNew
            | Self::GetHit
            | Self::GetMiss
            | Self::RemoveHit
            | Self::Range128
            | Self::MixedReadHeavy
            | Self::MixedBalanced => 0,
        }
    }

    pub fn throughput_elements(self) -> usize {
        match self {
            Self::Range128 => RANGE_QUERY_COUNT * RANGE_LEN,
            _ => self.operation_count(),
        }
    }

    pub fn expected_len(self, base_len: usize) -> usize {
        match self {
            Self::InsertNew => base_len + INSERT_BATCH_COUNT,
            Self::InsertUpdateHeavy => base_len + INSERT_BATCH_COUNT / 10,
            Self::RemoveHit => base_len - QUERY_COUNT,
            Self::GetHit | Self::GetMiss | Self::Range128 | Self::MixedReadHeavy | Self::MixedBalanced => base_len,
        }
    }

    pub fn needs_fresh_map(self) -> bool {
        matches!(self, Self::InsertNew | Self::InsertUpdateHeavy | Self::RemoveHit)
    }
}

impl Scenario {
    pub const ALL: [Self; 8] = [
        Self::InsertNew,
        Self::InsertDuplicateHeavy,
        Self::ContainsHit,
        Self::ContainsMiss,
        Self::RemoveHit,
        Self::Range128,
        Self::MixedReadHeavy,
        Self::MixedBalanced,
    ];

    pub const CAPACITY_SWEEP: [Self; 3] = [Self::ContainsHit, Self::InsertNew, Self::MixedBalanced];

    pub fn id(self) -> &'static str {
        match self {
            Self::InsertNew => "insert/new",
            Self::InsertDuplicateHeavy => "insert/dup90",
            Self::ContainsHit => "contains/hit",
            Self::ContainsMiss => "contains/miss",
            Self::RemoveHit => "remove/hit",
            Self::Range128 => "range/128",
            Self::MixedReadHeavy => "mixed/random_read90",
            Self::MixedBalanced => "mixed/random_read50",
        }
    }

    pub fn operation_count(self) -> usize {
        match self {
            Self::InsertNew | Self::InsertDuplicateHeavy => INSERT_BATCH_COUNT,
            Self::ContainsHit | Self::ContainsMiss | Self::MixedReadHeavy | Self::MixedBalanced => {
                PARALLEL_OPERATION_COUNT
            }
            Self::RemoveHit | Self::Range128 => QUERY_COUNT,
        }
    }

    pub fn expected_successes(self) -> usize {
        match self {
            Self::InsertNew => INSERT_BATCH_COUNT,
            Self::InsertDuplicateHeavy => INSERT_BATCH_COUNT / 10,
            Self::ContainsHit => PARALLEL_OPERATION_COUNT,
            Self::ContainsMiss => 0,
            Self::RemoveHit | Self::Range128 => QUERY_COUNT,
            Self::MixedReadHeavy => 49_200,
            Self::MixedBalanced => 54_000,
        }
    }

    pub fn throughput_elements(self) -> usize {
        match self {
            Self::Range128 => RANGE_QUERY_COUNT * RANGE_LEN,
            _ => self.operation_count(),
        }
    }

    pub fn expected_len(self, base_len: usize) -> usize {
        match self {
            Self::InsertNew => base_len + INSERT_BATCH_COUNT,
            Self::InsertDuplicateHeavy => base_len + INSERT_BATCH_COUNT / 10,
            Self::RemoveHit => base_len - QUERY_COUNT,
            Self::ContainsHit | Self::ContainsMiss | Self::Range128 | Self::MixedReadHeavy | Self::MixedBalanced => {
                base_len
            }
        }
    }

    pub fn needs_fresh_set(self) -> bool {
        matches!(self, Self::InsertNew | Self::InsertDuplicateHeavy | Self::RemoveHit)
    }
}

#[derive(Clone)]
pub enum Operation<T> {
    Contains(u64),
    Insert(T),
    Remove(u64),
    Range { start: u64, end: u64 },
}

#[derive(Clone)]
pub enum MapOperation<V> {
    Get(u64),
    Insert(u64, V),
    Remove(u64),
    Range { start: u64, end: u64 },
}

#[derive(Default)]
pub struct WorkerStats {
    pub operations: usize,
    pub successes: usize,
    pub updates: usize,
    pub validations: usize,
    pub checksum: u64,
}

impl WorkerStats {
    fn merge(&mut self, other: Self) {
        self.operations += other.operations;
        self.successes += other.successes;
        self.updates += other.updates;
        self.validations += other.validations;
        self.checksum ^= other.checksum;
    }
}

pub fn maximum_thread_count() -> usize {
    thread::available_parallelism().map_or(1, usize::from)
}

pub fn thread_counts() -> Vec<usize> {
    let maximum = maximum_thread_count();
    let mut counts = vec![1, maximum.min(4), maximum];
    counts.sort_unstable();
    counts.dedup();
    counts
}

pub fn operations<T: BenchValue>(scenario: Scenario, set_size: usize, thread_count: usize) -> Vec<Vec<Operation<T>>> {
    match scenario {
        Scenario::InsertNew => shard_operations(
            ValueGenerator::new(set_size)
                .regular_insertion_batch(INSERT_BATCH_COUNT)
                .into_iter()
                .map(Operation::Insert)
                .collect(),
            thread_count,
        ),
        Scenario::InsertDuplicateHeavy => shard_operations(
            ValueGenerator::new(set_size)
                .duplicate_heavy_insertion_batch(INSERT_BATCH_COUNT)
                .into_iter()
                .map(Operation::Insert)
                .collect(),
            thread_count,
        ),
        Scenario::ContainsHit => point_queries(set_size, true, PARALLEL_OPERATION_COUNT, thread_count),
        Scenario::ContainsMiss => point_queries(set_size, false, PARALLEL_OPERATION_COUNT, thread_count),
        Scenario::RemoveHit => shard_operations(
            ValueGenerator::new(set_size)
                .hit_keys()
                .into_iter()
                .map(Operation::Remove)
                .collect(),
            thread_count,
        ),
        Scenario::Range128 => range_queries(set_size, thread_count),
        Scenario::MixedReadHeavy => mixed_operations(set_size, thread_count, 18),
        Scenario::MixedBalanced => mixed_operations(set_size, thread_count, 2),
    }
}

pub fn map_operations<V: BenchMapValue>(
    scenario: MapScenario,
    map_size: usize,
    thread_count: usize,
) -> Vec<Vec<MapOperation<V>>> {
    match scenario {
        MapScenario::InsertNew => shard_operations(
            ValueGenerator::new(map_size)
                .map_insertions(INSERT_BATCH_COUNT, MapInsertionKind::New)
                .into_iter()
                .map(|(key, value)| MapOperation::Insert(key, value))
                .collect(),
            thread_count,
        ),
        MapScenario::InsertUpdateHeavy => shard_operations(
            ValueGenerator::new(map_size)
                .map_insertions(INSERT_BATCH_COUNT, MapInsertionKind::UpdateHeavy)
                .into_iter()
                .map(|(key, value)| MapOperation::Insert(key, value))
                .collect(),
            thread_count,
        ),
        MapScenario::GetHit => map_point_queries(map_size, true, PARALLEL_OPERATION_COUNT, thread_count),
        MapScenario::GetMiss => map_point_queries(map_size, false, PARALLEL_OPERATION_COUNT, thread_count),
        MapScenario::RemoveHit => shard_operations(
            ValueGenerator::new(map_size)
                .hit_keys()
                .into_iter()
                .map(MapOperation::Remove)
                .collect(),
            thread_count,
        ),
        MapScenario::Range128 => map_range_queries(map_size, thread_count),
        MapScenario::MixedReadHeavy => map_mixed_operations(map_size, thread_count, 18),
        MapScenario::MixedBalanced => map_mixed_operations(map_size, thread_count, 2),
    }
}

pub fn run_parallel<C, O, F>(collection: Arc<C>, operation_shards: Vec<Vec<O>>, apply: F) -> (Duration, WorkerStats)
where
    C: Send + Sync + 'static,
    O: Send + 'static,
    F: Fn(&C, O, &mut WorkerStats) + Send + Sync + 'static,
{
    let worker_count = operation_shards.len();
    let ready_barrier = Arc::new(Barrier::new(worker_count + 1));
    let start_barrier = Arc::new(Barrier::new(worker_count + 1));
    let finish_barrier = Arc::new(Barrier::new(worker_count + 1));
    let apply = Arc::new(apply);
    let mut handles = Vec::with_capacity(worker_count);

    for operations in operation_shards {
        let collection = Arc::clone(&collection);
        let ready_barrier = Arc::clone(&ready_barrier);
        let start_barrier = Arc::clone(&start_barrier);
        let finish_barrier = Arc::clone(&finish_barrier);
        let apply = Arc::clone(&apply);
        handles.push(thread::spawn(move || {
            let mut operations = operations.into_iter();
            ready_barrier.wait();
            start_barrier.wait();
            let result = catch_unwind(AssertUnwindSafe(|| {
                let mut stats = WorkerStats::default();
                for operation in operations.by_ref() {
                    apply(collection.as_ref(), operation, &mut stats);
                }
                stats
            }));
            finish_barrier.wait();
            // Return the iterator so its allocation is freed after the timer stops.
            (result, operations)
        }));
    }

    ready_barrier.wait();
    let start = Instant::now();
    start_barrier.wait();
    finish_barrier.wait();
    let elapsed = start.elapsed();

    let mut stats = WorkerStats::default();
    for handle in handles {
        let (result, operations) = handle.join().expect("benchmark worker should join");
        match result {
            Ok(worker_stats) => stats.merge(worker_stats),
            Err(payload) => resume_unwind(payload),
        }
        drop(operations);
    }

    (elapsed, stats)
}

#[allow(dead_code)]
pub fn run_parallel_with_context<C, O, W, I, F>(
    collection: Arc<C>,
    operation_shards: Vec<Vec<O>>,
    initialize: I,
    apply: F,
) -> (Duration, WorkerStats)
where
    C: Send + Sync + 'static,
    O: Send + 'static,
    I: Fn(&C) -> W + Send + Sync + 'static,
    F: Fn(&C, &mut W, O, &mut WorkerStats) + Send + Sync + 'static,
{
    let worker_count = operation_shards.len();
    let ready_barrier = Arc::new(Barrier::new(worker_count + 1));
    let start_barrier = Arc::new(Barrier::new(worker_count + 1));
    let finish_barrier = Arc::new(Barrier::new(worker_count + 1));
    let release_barrier = Arc::new(Barrier::new(worker_count + 1));
    let initialize = Arc::new(initialize);
    let apply = Arc::new(apply);
    let mut handles = Vec::with_capacity(worker_count);

    for operations in operation_shards {
        let collection = Arc::clone(&collection);
        let ready_barrier = Arc::clone(&ready_barrier);
        let start_barrier = Arc::clone(&start_barrier);
        let finish_barrier = Arc::clone(&finish_barrier);
        let release_barrier = Arc::clone(&release_barrier);
        let initialize = Arc::clone(&initialize);
        let apply = Arc::clone(&apply);
        handles.push(thread::spawn(move || {
            let mut operations = operations.into_iter();
            let mut context = initialize(collection.as_ref());
            ready_barrier.wait();
            start_barrier.wait();
            let result = catch_unwind(AssertUnwindSafe(|| {
                let mut stats = WorkerStats::default();
                for operation in operations.by_ref() {
                    apply(collection.as_ref(), &mut context, operation, &mut stats);
                }
                stats
            }));
            finish_barrier.wait();
            // Worker context teardown can unpin epochs or free buffers, so release it after timing.
            release_barrier.wait();
            drop(context);
            (result, operations)
        }));
    }

    ready_barrier.wait();
    let start = Instant::now();
    start_barrier.wait();
    finish_barrier.wait();
    let elapsed = start.elapsed();
    release_barrier.wait();

    let mut stats = WorkerStats::default();
    for handle in handles {
        let (result, operations) = handle.join().expect("benchmark worker should join");
        match result {
            Ok(worker_stats) => stats.merge(worker_stats),
            Err(payload) => resume_unwind(payload),
        }
        drop(operations);
    }

    (elapsed, stats)
}

fn point_queries<T>(set_size: usize, hit: bool, operation_count: usize, thread_count: usize) -> Vec<Vec<Operation<T>>> {
    let base_keys = ValueGenerator::new(set_size).base_values::<u64>();
    let operations = (0..operation_count)
        .map(|index| {
            let key = base_keys[index % base_keys.len()] + u64::from(!hit);
            Operation::Contains(key)
        })
        .collect();
    shard_operations(operations, thread_count)
}

fn map_point_queries<V>(
    map_size: usize,
    hit: bool,
    operation_count: usize,
    thread_count: usize,
) -> Vec<Vec<MapOperation<V>>> {
    let base_keys = ValueGenerator::new(map_size).base_values::<u64>();
    let operations = (0..operation_count)
        .map(|index| {
            let key = base_keys[index % base_keys.len()] + u64::from(!hit);
            MapOperation::Get(key)
        })
        .collect();
    shard_operations(operations, thread_count)
}

fn range_queries<T>(set_size: usize, thread_count: usize) -> Vec<Vec<Operation<T>>> {
    let mut rng = StdRng::seed_from_u64(SEED ^ 0x00A1_1CE5);
    let operations = (0..RANGE_QUERY_COUNT)
        .map(|_| {
            let start_index = rng.random_range(0..=(set_size - RANGE_LEN));
            Operation::Range {
                start: start_index as u64 * 2,
                end: (start_index + RANGE_LEN) as u64 * 2,
            }
        })
        .collect();
    shard_operations(operations, thread_count)
}

fn map_range_queries<V>(map_size: usize, thread_count: usize) -> Vec<Vec<MapOperation<V>>> {
    let mut rng = StdRng::seed_from_u64(SEED ^ 0x00A1_1CE5);
    let operations = (0..RANGE_QUERY_COUNT)
        .map(|_| {
            let start_index = rng.random_range(0..=(map_size - RANGE_LEN));
            MapOperation::Range {
                start: start_index as u64 * 2,
                end: (start_index + RANGE_LEN) as u64 * 2,
            }
        })
        .collect();
    shard_operations(operations, thread_count)
}

fn mixed_operations<T: BenchValue>(
    set_size: usize,
    thread_count: usize,
    reads_per_block: usize,
) -> Vec<Vec<Operation<T>>> {
    let block_len = reads_per_block + 2;
    let block_count = PARALLEL_OPERATION_COUNT / block_len;
    let mut base_keys = ValueGenerator::new(set_size).base_values::<u64>();
    let mutation_key_count = block_count.min(set_size / 2);
    let read_keys = base_keys.split_off(mutation_key_count);
    let mutation_keys = base_keys;
    let mut shards = empty_shards(thread_count);
    let mut rng = StdRng::seed_from_u64(SEED ^ reads_per_block as u64);
    let mut read_index = 0;

    for block in 0..block_count {
        let mutation_index = block % mutation_keys.len();
        let mutation_key = mutation_keys[mutation_index];
        let shard = mutation_index % thread_count;
        let mutation_position = rng.random_range(0..=reads_per_block);

        for position in 0..=reads_per_block {
            if position == mutation_position {
                shards[shard].push(Operation::Remove(mutation_key));
                shards[shard].push(Operation::Insert(T::from_key(mutation_key)));
            }
            if position < reads_per_block {
                let hit = read_index % 5 != 4;
                let key = read_keys[rng.random_range(0..read_keys.len())] + u64::from(!hit);
                shards[shard].push(Operation::Contains(key));
                read_index += 1;
            }
        }
    }

    shards
}

fn map_mixed_operations<V: BenchMapValue>(
    map_size: usize,
    thread_count: usize,
    reads_per_block: usize,
) -> Vec<Vec<MapOperation<V>>> {
    let block_len = reads_per_block + 2;
    let block_count = PARALLEL_OPERATION_COUNT / block_len;
    let mut base_keys = ValueGenerator::new(map_size).base_values::<u64>();
    let mutation_key_count = block_count.min(map_size / 2);
    let read_keys = base_keys.split_off(mutation_key_count);
    let mutation_keys = base_keys;
    let mut shards = empty_shards(thread_count);
    let mut rng = StdRng::seed_from_u64(SEED ^ reads_per_block as u64);
    let mut read_index = 0;

    for block in 0..block_count {
        let mutation_index = block % mutation_keys.len();
        let mutation_key = mutation_keys[mutation_index];
        let shard = mutation_index % thread_count;
        let mutation_position = rng.random_range(0..=reads_per_block);

        for position in 0..=reads_per_block {
            if position == mutation_position {
                shards[shard].push(MapOperation::Remove(mutation_key));
                shards[shard].push(MapOperation::Insert(mutation_key, V::from_key(mutation_key)));
            }
            if position < reads_per_block {
                let hit = read_index % 5 != 4;
                let key = read_keys[rng.random_range(0..read_keys.len())] + u64::from(!hit);
                shards[shard].push(MapOperation::Get(key));
                read_index += 1;
            }
        }
    }

    shards
}

fn shard_operations<T>(operations: Vec<T>, thread_count: usize) -> Vec<Vec<T>> {
    let mut shards = empty_shards(thread_count);
    for (index, operation) in operations.into_iter().enumerate() {
        shards[index % thread_count].push(operation);
    }
    shards
}

fn empty_shards<T>(thread_count: usize) -> Vec<Vec<T>> {
    (0..thread_count).map(|_| Vec::new()).collect()
}
