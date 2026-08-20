use crate::value_generator::{BenchMapValue, BenchValue, MapInsertionKind, ValueGenerator, SEED};
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::panic::{catch_unwind, resume_unwind, AssertUnwindSafe};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

pub const INSERT_BATCH_COUNT: usize = 1_024;
pub const MIXED_OPERATION_COUNT: usize = 10_000;
const MIXED_READ_HIT_PERCENT: usize = 90;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Scenario {
    InsertBatch,
    InsertBatch90Duplicates,
    MixedUsual,
    MixedHot,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MapScenario {
    InsertBatchNew,
    InsertBatchUpdate,
    MixedRead90Write10,
    MixedRead50Write50,
}

impl MapScenario {
    pub const ALL: [Self; 4] = [
        Self::InsertBatchNew,
        Self::InsertBatchUpdate,
        Self::MixedRead90Write10,
        Self::MixedRead50Write50,
    ];

    pub fn id(self) -> &'static str {
        match self {
            Self::InsertBatchNew => "insert_batch/new",
            Self::InsertBatchUpdate => "insert_batch/update",
            Self::MixedRead90Write10 => "mixed/read90_write10",
            Self::MixedRead50Write50 => "mixed/read50_write50",
        }
    }

    pub fn operation_count(self) -> usize {
        match self {
            Self::InsertBatchNew | Self::InsertBatchUpdate => INSERT_BATCH_COUNT,
            Self::MixedRead90Write10 | Self::MixedRead50Write50 => MIXED_OPERATION_COUNT,
        }
    }

    pub fn expected_successes(self) -> usize {
        match self {
            Self::InsertBatchNew => INSERT_BATCH_COUNT,
            Self::InsertBatchUpdate => 0,
            Self::MixedRead90Write10 | Self::MixedRead50Write50 => {
                let (read_count, write_pair_count) = self.mixed_operation_counts();
                read_count * MIXED_READ_HIT_PERCENT / 100 + write_pair_count * 2
            }
        }
    }

    pub fn expected_updates(self) -> usize {
        match self {
            Self::InsertBatchUpdate => INSERT_BATCH_COUNT,
            Self::InsertBatchNew | Self::MixedRead90Write10 | Self::MixedRead50Write50 => 0,
        }
    }

    pub fn expected_len(self, base_len: usize) -> usize {
        match self {
            Self::InsertBatchNew => base_len + INSERT_BATCH_COUNT,
            Self::InsertBatchUpdate | Self::MixedRead90Write10 | Self::MixedRead50Write50 => base_len,
        }
    }

    pub fn needs_fresh_map(self) -> bool {
        matches!(self, Self::InsertBatchNew | Self::InsertBatchUpdate)
    }

    fn reads_per_block(self) -> usize {
        match self {
            Self::MixedRead90Write10 => 18,
            Self::MixedRead50Write50 => 2,
            Self::InsertBatchNew | Self::InsertBatchUpdate => 0,
        }
    }

    fn mixed_operation_counts(self) -> (usize, usize) {
        let reads_per_block = self.reads_per_block();
        let block_count = MIXED_OPERATION_COUNT / (reads_per_block + 2);
        (block_count * reads_per_block, block_count)
    }

    pub fn validate_operations<V: BenchMapValue>(self, shards: &[Vec<MapOperation<V>>], thread_count: usize) {
        assert_eq!(shards.len(), thread_count);
        assert!(shards.iter().all(|shard| !shard.is_empty()));

        let actual = MapWorkloadShape::from_shards(shards);
        let expected = match self {
            Self::InsertBatchNew => MapWorkloadShape {
                inserts: INSERT_BATCH_COUNT,
                odd_inserts: INSERT_BATCH_COUNT,
                ..MapWorkloadShape::default()
            },
            Self::InsertBatchUpdate => MapWorkloadShape {
                inserts: INSERT_BATCH_COUNT,
                ..MapWorkloadShape::default()
            },
            Self::MixedRead90Write10 | Self::MixedRead50Write50 => {
                let (gets, write_pair_count) = self.mixed_operation_counts();
                MapWorkloadShape {
                    gets,
                    get_hits: gets * MIXED_READ_HIT_PERCENT / 100,
                    inserts: write_pair_count,
                    removes: write_pair_count,
                    ..MapWorkloadShape::default()
                }
            }
        };

        assert_eq!(actual, expected);
        assert_eq!(actual.operation_count(), self.operation_count());
    }
}

impl Scenario {
    pub const ALL: [Self; 4] = [
        Self::InsertBatch,
        Self::InsertBatch90Duplicates,
        Self::MixedUsual,
        Self::MixedHot,
    ];

    pub fn id(self) -> &'static str {
        match self {
            Self::InsertBatch => "insert_batch",
            Self::InsertBatch90Duplicates => "insert_batch_90_duplicates",
            Self::MixedUsual => "mixed/usual",
            Self::MixedHot => "mixed/hot",
        }
    }

    pub fn operation_count(self) -> usize {
        match self {
            Self::InsertBatch | Self::InsertBatch90Duplicates => INSERT_BATCH_COUNT,
            Self::MixedUsual | Self::MixedHot => MIXED_OPERATION_COUNT,
        }
    }

    pub fn expected_successes(self) -> usize {
        match self {
            Self::InsertBatch => INSERT_BATCH_COUNT,
            Self::InsertBatch90Duplicates => INSERT_BATCH_COUNT / 10,
            Self::MixedUsual | Self::MixedHot => {
                let (read_count, write_pair_count) = self.mixed_operation_counts();
                read_count * MIXED_READ_HIT_PERCENT / 100 + write_pair_count * 2
            }
        }
    }

    pub fn expected_len(self, base_len: usize) -> usize {
        match self {
            Self::InsertBatch => base_len + INSERT_BATCH_COUNT,
            Self::InsertBatch90Duplicates => base_len + INSERT_BATCH_COUNT / 10,
            Self::MixedUsual | Self::MixedHot => base_len,
        }
    }

    pub fn needs_fresh_set(self) -> bool {
        matches!(self, Self::InsertBatch | Self::InsertBatch90Duplicates)
    }

    fn reads_per_block(self) -> usize {
        match self {
            Self::MixedUsual => 18,
            Self::MixedHot => 2,
            Self::InsertBatch | Self::InsertBatch90Duplicates => 0,
        }
    }

    fn mixed_operation_counts(self) -> (usize, usize) {
        let reads_per_block = self.reads_per_block();
        let block_count = MIXED_OPERATION_COUNT / (reads_per_block + 2);
        (block_count * reads_per_block, block_count)
    }

    pub fn validate_operations<T: BenchValue>(self, shards: &[Vec<Operation<T>>], thread_count: usize) {
        assert_eq!(shards.len(), thread_count);
        assert!(shards.iter().all(|shard| !shard.is_empty()));

        let actual = WorkloadShape::from_shards(shards);
        let expected = match self {
            Self::InsertBatch => WorkloadShape {
                inserts: INSERT_BATCH_COUNT,
                odd_inserts: INSERT_BATCH_COUNT,
                ..WorkloadShape::default()
            },
            Self::InsertBatch90Duplicates => WorkloadShape {
                inserts: INSERT_BATCH_COUNT,
                odd_inserts: INSERT_BATCH_COUNT / 10,
                ..WorkloadShape::default()
            },
            Self::MixedUsual | Self::MixedHot => {
                let (contains, write_pair_count) = self.mixed_operation_counts();
                WorkloadShape {
                    contains,
                    contains_hits: contains * MIXED_READ_HIT_PERCENT / 100,
                    inserts: write_pair_count,
                    removes: write_pair_count,
                    ..WorkloadShape::default()
                }
            }
        };

        assert_eq!(actual, expected);
        assert_eq!(actual.operation_count(), self.operation_count());
    }
}

#[derive(Clone)]
pub enum Operation<T> {
    Contains(u64),
    Insert(T),
    Remove(u64),
}

#[derive(Debug, Default, Eq, PartialEq)]
struct WorkloadShape {
    contains: usize,
    contains_hits: usize,
    inserts: usize,
    odd_inserts: usize,
    removes: usize,
}

impl WorkloadShape {
    fn from_shards<T: BenchValue>(shards: &[Vec<Operation<T>>]) -> Self {
        let mut shape = Self::default();

        for operation in shards.iter().flatten() {
            match operation {
                Operation::Contains(key) => {
                    shape.contains += 1;
                    shape.contains_hits += usize::from(key % 2 == 0);
                }
                Operation::Insert(value) => {
                    shape.inserts += 1;
                    shape.odd_inserts += usize::from(value.key() % 2 == 1);
                }
                Operation::Remove(_) => shape.removes += 1,
            }
        }

        shape
    }

    fn operation_count(&self) -> usize {
        self.contains + self.inserts + self.removes
    }
}

#[derive(Clone)]
pub enum MapOperation<V> {
    Get(u64),
    Insert(u64, V),
    Remove(u64),
}

#[derive(Debug, Default, Eq, PartialEq)]
struct MapWorkloadShape {
    gets: usize,
    get_hits: usize,
    inserts: usize,
    odd_inserts: usize,
    removes: usize,
}

impl MapWorkloadShape {
    fn from_shards<V>(shards: &[Vec<MapOperation<V>>]) -> Self {
        let mut shape = Self::default();

        for operation in shards.iter().flatten() {
            match operation {
                MapOperation::Get(key) => {
                    shape.gets += 1;
                    shape.get_hits += usize::from(key % 2 == 0);
                }
                MapOperation::Insert(key, _) => {
                    shape.inserts += 1;
                    shape.odd_inserts += usize::from(key % 2 == 1);
                }
                MapOperation::Remove(_) => shape.removes += 1,
            }
        }

        shape
    }

    fn operation_count(&self) -> usize {
        self.gets + self.inserts + self.removes
    }
}

#[derive(Default)]
pub struct WorkerStats {
    pub operations: usize,
    pub successes: usize,
    pub updates: usize,
    pub checksum: u64,
}

impl WorkerStats {
    fn merge(&mut self, other: Self) {
        self.operations += other.operations;
        self.successes += other.successes;
        self.updates += other.updates;
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
        Scenario::InsertBatch => shard_operations(
            ValueGenerator::new(set_size)
                .regular_insertion_batch(INSERT_BATCH_COUNT)
                .into_iter()
                .map(Operation::Insert)
                .collect(),
            thread_count,
        ),
        Scenario::InsertBatch90Duplicates => shard_operations(
            ValueGenerator::new(set_size)
                .duplicate_heavy_insertion_batch(INSERT_BATCH_COUNT)
                .into_iter()
                .map(Operation::Insert)
                .collect(),
            thread_count,
        ),
        Scenario::MixedUsual | Scenario::MixedHot => {
            mixed_operations(set_size, thread_count, scenario.reads_per_block())
        }
    }
}

pub fn map_operations<V: BenchMapValue>(
    scenario: MapScenario,
    map_size: usize,
    thread_count: usize,
) -> Vec<Vec<MapOperation<V>>> {
    match scenario {
        MapScenario::InsertBatchNew => shard_operations(
            ValueGenerator::new(map_size)
                .map_insertions(INSERT_BATCH_COUNT, MapInsertionKind::New)
                .into_iter()
                .map(|(key, value)| MapOperation::Insert(key, value))
                .collect(),
            thread_count,
        ),
        MapScenario::InsertBatchUpdate => shard_operations(
            ValueGenerator::new(map_size)
                .map_insertions(INSERT_BATCH_COUNT, MapInsertionKind::Update)
                .into_iter()
                .map(|(key, value)| MapOperation::Insert(key, value))
                .collect(),
            thread_count,
        ),
        MapScenario::MixedRead90Write10 | MapScenario::MixedRead50Write50 => {
            map_mixed_operations(map_size, thread_count, scenario.reads_per_block())
        }
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
        let (result, _operations) = handle.join().expect("benchmark worker should join");
        match result {
            Ok(worker_stats) => stats.merge(worker_stats),
            Err(payload) => resume_unwind(payload),
        }
    }

    (elapsed, stats)
}

fn mixed_operations<T: BenchValue>(
    set_size: usize,
    thread_count: usize,
    reads_per_block: usize,
) -> Vec<Vec<Operation<T>>> {
    let block_len = reads_per_block + 2;
    let block_count = MIXED_OPERATION_COUNT / block_len;
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
                let hit = read_index % 10 != 9;
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
    let block_count = MIXED_OPERATION_COUNT / block_len;
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
                let hit = read_index % 10 != 9;
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
