use super::workload::{map_operations, run_parallel_with_context, MapOperation, MapScenario, WorkerStats};
use crate::value_generator::{BenchMapValue, MapInsertionKind, ValueGenerator, SINGLE_OPERATION_BATCH_SIZE};
use criterion::{black_box, measurement::WallTime, BenchmarkGroup, BenchmarkId};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub type IndexsetMap = indexset::concurrent::map::BTreeMap<u64, u64>;
pub type WorkTablesIndexMap = WorkTablesIndex::concurrent::map::BTreeMap<u64, u64>;
pub type ArcticMap = arctic::ConcurrentMap<u64, u64>;
pub type CongeeMap = congee::CongeeRaw<usize, usize>;

pub trait ComparisonMap: Send + Sync + Sized + 'static {
    const ID: &'static str;
    type WorkerContext;

    fn build(entries: &[(u64, u64)], node_capacity: usize) -> Self;
    fn worker_context(&self) -> Self::WorkerContext;
    fn insert(&self, context: &mut Self::WorkerContext, key: u64, value: u64) -> Option<u64>;
    fn get(&self, context: &mut Self::WorkerContext, key: u64) -> Option<u64>;
    fn remove(&self, context: &mut Self::WorkerContext, key: u64) -> Option<u64>;
}

impl ComparisonMap for IndexsetMap {
    const ID: &'static str = "indexset";
    type WorkerContext = ();

    fn build(entries: &[(u64, u64)], node_capacity: usize) -> Self {
        let map = Self::with_maximum_node_size(node_capacity);
        for (key, value) in entries {
            assert!(map.insert(*key, *value).is_none());
        }
        map
    }

    fn worker_context(&self) -> Self::WorkerContext {}

    fn insert(&self, _context: &mut Self::WorkerContext, key: u64, value: u64) -> Option<u64> {
        self.insert(key, value)
    }

    fn get(&self, _context: &mut Self::WorkerContext, key: u64) -> Option<u64> {
        self.get(&key).map(|entry| entry.get().value)
    }

    fn remove(&self, _context: &mut Self::WorkerContext, key: u64) -> Option<u64> {
        self.remove(&key).map(|(_, value)| value)
    }
}

impl ComparisonMap for WorkTablesIndexMap {
    const ID: &'static str = "worktables_index";
    type WorkerContext = ();

    fn build(entries: &[(u64, u64)], node_capacity: usize) -> Self {
        let map = Self::with_maximum_node_size(node_capacity);
        for (key, value) in entries {
            assert!(map.insert(*key, *value).is_none());
        }
        map
    }

    fn worker_context(&self) -> Self::WorkerContext {}

    fn insert(&self, _context: &mut Self::WorkerContext, key: u64, value: u64) -> Option<u64> {
        self.insert(key, value)
    }

    fn get(&self, _context: &mut Self::WorkerContext, key: u64) -> Option<u64> {
        self.get(&key).map(|entry| entry.get().value)
    }

    fn remove(&self, _context: &mut Self::WorkerContext, key: u64) -> Option<u64> {
        self.remove(&key).map(|(_, value)| value)
    }
}

impl ComparisonMap for ArcticMap {
    const ID: &'static str = "arctic";
    type WorkerContext = ();

    fn build(entries: &[(u64, u64)], _node_capacity: usize) -> Self {
        let map = Self::new();
        for (key, value) in entries {
            assert!(map.insert(*key, *value).is_ok());
        }
        map
    }

    fn worker_context(&self) -> Self::WorkerContext {}

    fn insert(&self, _context: &mut Self::WorkerContext, key: u64, value: u64) -> Option<u64> {
        self.upsert(key, value).old().copied()
    }

    fn get(&self, _context: &mut Self::WorkerContext, key: u64) -> Option<u64> {
        self.get(&key).map(|value| *value)
    }

    fn remove(&self, _context: &mut Self::WorkerContext, key: u64) -> Option<u64> {
        self.remove(&key).map(|value| *value)
    }
}

pub struct CongeeWorkerContext {
    guard: congee::epoch::Guard,
}

impl ComparisonMap for CongeeMap {
    const ID: &'static str = "congee_raw";
    type WorkerContext = CongeeWorkerContext;

    fn build(entries: &[(u64, u64)], _node_capacity: usize) -> Self {
        let map = Self::default();
        let guard = map.pin();
        for (key, value) in entries {
            assert!(map
                .insert(*key as usize, *value as usize, &guard)
                .expect("CongeeRaw allocation should succeed")
                .is_none());
        }
        drop(guard);
        map
    }

    fn worker_context(&self) -> Self::WorkerContext {
        CongeeWorkerContext { guard: self.pin() }
    }

    fn insert(&self, context: &mut Self::WorkerContext, key: u64, value: u64) -> Option<u64> {
        self.insert(key as usize, value as usize, &context.guard)
            .expect("CongeeRaw allocation should succeed")
            .map(|value| value as u64)
    }

    fn get(&self, context: &mut Self::WorkerContext, key: u64) -> Option<u64> {
        self.get(&(key as usize), &context.guard).map(|value| value as u64)
    }

    fn remove(&self, context: &mut Self::WorkerContext, key: u64) -> Option<u64> {
        self.remove(&(key as usize), &context.guard).map(|value| value as u64)
    }
}

struct Fixture<M: ComparisonMap> {
    base_entries: Vec<(u64, u64)>,
    operations: Vec<Vec<MapOperation<u64>>>,
    stable_map: Option<Arc<M>>,
}

impl<M: ComparisonMap> Fixture<M> {
    fn new(scenario: MapScenario, map_size: usize, node_capacity: usize, thread_count: usize) -> Self {
        let base_entries = ValueGenerator::new(map_size).map_base_entries();
        let operations = comparison_operations(scenario, map_size, thread_count);
        scenario.validate_operations(&operations, thread_count);
        let stable_map = (!scenario.needs_fresh_map()).then(|| Arc::new(M::build(&base_entries, node_capacity)));

        Self {
            base_entries,
            operations,
            stable_map,
        }
    }
}

pub fn bench_parallel_case<M: ComparisonMap>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    scenario: MapScenario,
    map_size: usize,
    node_capacity: usize,
    thread_count: usize,
) {
    let id = BenchmarkId::new(format!("{}_t{thread_count}", M::ID), format!("n{map_size}"));
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let fixture = fixture.get_or_insert_with(|| Fixture::<M>::new(scenario, map_size, node_capacity, thread_count));

        b.iter_custom(|iterations| {
            let mut total_elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let map = if scenario.needs_fresh_map() {
                    Arc::new(M::build(&fixture.base_entries, node_capacity))
                } else {
                    Arc::clone(fixture.stable_map.as_ref().expect("stable scenario should have a map"))
                };
                let (elapsed, stats) = run_parallel_with_context(
                    Arc::clone(&map),
                    fixture.operations.clone(),
                    M::worker_context,
                    apply_operation::<M>,
                );
                total_elapsed += elapsed;

                black_box(stats.checksum);
                assert_eq!(stats.operations, scenario.operation_count());
                assert_eq!(stats.successes, scenario.expected_successes());
                assert_eq!(stats.updates, scenario.expected_updates());
                validate_mutations(map.as_ref(), scenario, &fixture.operations);
            }

            total_elapsed
        });
    });
}

pub fn bench_insert_one<M: ComparisonMap>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    map_size: usize,
    kind: MapInsertionKind,
) {
    let id = BenchmarkId::new(M::ID, format!("n{map_size}"));
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let (base_entries, insertions) = fixture.get_or_insert_with(|| {
            let generator = ValueGenerator::new(map_size);
            (
                generator.map_base_entries::<u64>(),
                comparison_insertions(&generator, kind),
            )
        });

        b.iter_custom(|iterations| {
            let mut total_elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let insertion_batch = insertions.clone();
                let map = M::build(base_entries, crate::value_generator::DEFAULT_NODE_CAPACITY);
                let mut context = map.worker_context();
                let mut updates = 0;

                let start = Instant::now();
                for (key, value) in insertion_batch {
                    updates +=
                        usize::from(black_box(map.insert(&mut context, black_box(key), black_box(value))).is_some());
                }
                total_elapsed += start.elapsed();

                let expected_updates = match kind {
                    MapInsertionKind::New => 0,
                    MapInsertionKind::Update => SINGLE_OPERATION_BATCH_SIZE,
                };
                assert_eq!(updates, expected_updates);
                for (key, value) in insertions.iter() {
                    assert_eq!(map.get(&mut context, *key), Some(*value));
                }
            }

            total_elapsed / SINGLE_OPERATION_BATCH_SIZE as u32
        });
    });
}

fn comparison_operations(scenario: MapScenario, map_size: usize, thread_count: usize) -> Vec<Vec<MapOperation<u64>>> {
    let mut operation_shards = map_operations(scenario, map_size, thread_count);
    for operation in operation_shards.iter_mut().flatten() {
        if let MapOperation::Insert(key, value) = operation {
            *value = comparison_value(*key, *value);
        }
    }
    operation_shards
}

fn comparison_insertions(generator: &ValueGenerator, kind: MapInsertionKind) -> Vec<(u64, u64)> {
    generator
        .map_insertions::<u64>(SINGLE_OPERATION_BATCH_SIZE, kind)
        .into_iter()
        .map(|(key, value)| (key, comparison_value(key, value)))
        .collect()
}

fn comparison_value(key: u64, value: u64) -> u64 {
    // CongeeRaw uses the high payload bit as a node tag, so every implementation gets the same low-bit update.
    let value = if value == <u64 as BenchMapValue>::updated_from_key(key) {
        key + 1
    } else {
        value
    };
    debug_assert_eq!(value >> 63, 0);
    value
}

fn validate_mutations<M: ComparisonMap>(map: &M, scenario: MapScenario, operation_shards: &[Vec<MapOperation<u64>>]) {
    let mut context = map.worker_context();
    match scenario {
        MapScenario::InsertBatchNew
        | MapScenario::InsertBatchUpdate
        | MapScenario::MixedRead90Write10
        | MapScenario::MixedRead50Write50 => {
            for operation in operation_shards.iter().flatten() {
                if let MapOperation::Insert(key, value) = operation {
                    assert_eq!(map.get(&mut context, *key), Some(*value));
                }
            }
        }
    }
}

fn apply_operation<M: ComparisonMap>(
    map: &M,
    context: &mut M::WorkerContext,
    operation: MapOperation<u64>,
    stats: &mut WorkerStats,
) {
    match operation {
        MapOperation::Get(key) => {
            let value = black_box(map.get(context, black_box(key)));
            stats.operations += 1;
            if let Some(value) = value {
                stats.successes += 1;
                stats.checksum ^= key ^ value.checksum();
            }
        }
        MapOperation::Insert(key, value) => {
            let checksum = value.checksum();
            let previous = black_box(map.insert(context, black_box(key), black_box(value)));
            stats.operations += 1;
            if let Some(previous) = previous {
                stats.updates += 1;
                stats.checksum ^= key ^ previous.checksum();
            } else {
                stats.successes += 1;
                stats.checksum ^= key ^ checksum;
            }
        }
        MapOperation::Remove(key) => {
            let removed = black_box(map.remove(context, black_box(key)));
            stats.operations += 1;
            if let Some(value) = removed {
                stats.successes += 1;
                stats.checksum ^= key ^ value.checksum();
            }
        }
    }
}
