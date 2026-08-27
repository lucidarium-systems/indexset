use super::workload::{map_operations, run_parallel, MapOperation, MapScenario, WorkerStats};
use crate::value_generator::{BenchMapValue, MapInsertionKind, ValueGenerator, RANGE_LEN, SINGLE_OPERATION_BATCH_SIZE};
use criterion::{black_box, measurement::WallTime, BenchmarkGroup, BenchmarkId};
use parking_lot::Mutex;
use std::collections::BTreeMap as StdBTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub type ConcurrentMap<V> = indexset::concurrent::map::BTreeMap<u64, V>;
pub type MutexIndexMap<V> = Mutex<indexset::BTreeMap<u64, V>>;
pub type MutexStdBTreeMap<V> = Mutex<StdBTreeMap<u64, V>>;

pub trait MapImplementation<V>: Send + Sync + Sized + 'static
where
    V: BenchMapValue + Send + Sync,
{
    const ID: &'static str;
    const USES_NODE_CAPACITY: bool;

    fn build(entries: &[(u64, V)], node_capacity: usize) -> Self;
    fn insert(&self, key: u64, value: V) -> Option<V>;
    fn get_checksum(&self, key: &u64) -> Option<u64>;
    fn remove(&self, key: &u64) -> Option<V>;
    fn range_count(&self, start: u64, end: u64) -> usize;
    fn range_checksum(&self, start: u64, end: u64) -> u64;
    fn len(&self) -> usize;

    fn benchmark_id(node_capacity: usize, thread_count: Option<usize>) -> String {
        let mut id = if Self::USES_NODE_CAPACITY {
            format!("{}_cap{node_capacity}", Self::ID)
        } else {
            Self::ID.to_owned()
        };
        if let Some(thread_count) = thread_count {
            id.push_str(&format!("_t{thread_count}"));
        }
        id
    }
}

impl<V> MapImplementation<V> for ConcurrentMap<V>
where
    V: BenchMapValue + Send + Sync,
{
    const ID: &'static str = "concurrent";
    const USES_NODE_CAPACITY: bool = true;

    fn build(entries: &[(u64, V)], node_capacity: usize) -> Self {
        let map = Self::with_maximum_node_size(node_capacity);
        for (key, value) in entries {
            assert!(map.insert(*key, value.clone()).is_none());
        }
        map
    }

    fn insert(&self, key: u64, value: V) -> Option<V> {
        self.insert(key, value)
    }

    fn get_checksum(&self, key: &u64) -> Option<u64> {
        self.get(key).map(|entry| entry.get().value.checksum())
    }

    fn remove(&self, key: &u64) -> Option<V> {
        self.remove(key).map(|(_, value)| value)
    }

    fn range_count(&self, start: u64, end: u64) -> usize {
        self.range(start..end).count()
    }

    fn range_checksum(&self, start: u64, end: u64) -> u64 {
        self.range(start..end).fold(0_u64, |checksum, (key, value)| {
            checksum.wrapping_add(*key).wrapping_add(value.checksum())
        })
    }

    fn len(&self) -> usize {
        self.len()
    }
}

impl<V> MapImplementation<V> for MutexIndexMap<V>
where
    V: BenchMapValue + Send + Sync,
{
    const ID: &'static str = "indexset_mutex";
    const USES_NODE_CAPACITY: bool = true;

    fn build(entries: &[(u64, V)], node_capacity: usize) -> Self {
        let mut map = indexset::BTreeMap::with_maximum_node_size(node_capacity);
        for (key, value) in entries {
            assert!(map.insert(*key, value.clone()).is_none());
        }
        Self::new(map)
    }

    fn insert(&self, key: u64, value: V) -> Option<V> {
        self.lock().insert(key, value)
    }

    fn get_checksum(&self, key: &u64) -> Option<u64> {
        self.lock().get(key).map(BenchMapValue::checksum)
    }

    fn remove(&self, key: &u64) -> Option<V> {
        self.lock().remove(key)
    }

    fn range_count(&self, start: u64, end: u64) -> usize {
        self.lock().range(start..end).count()
    }

    fn range_checksum(&self, start: u64, end: u64) -> u64 {
        self.lock().range(start..end).fold(0_u64, |checksum, (key, value)| {
            checksum.wrapping_add(*key).wrapping_add(value.checksum())
        })
    }

    fn len(&self) -> usize {
        self.lock().len()
    }
}

impl<V> MapImplementation<V> for MutexStdBTreeMap<V>
where
    V: BenchMapValue + Send + Sync,
{
    const ID: &'static str = "std_btree_map_mutex";
    const USES_NODE_CAPACITY: bool = false;

    fn build(entries: &[(u64, V)], _node_capacity: usize) -> Self {
        Self::new(entries.iter().cloned().collect())
    }

    fn insert(&self, key: u64, value: V) -> Option<V> {
        self.lock().insert(key, value)
    }

    fn get_checksum(&self, key: &u64) -> Option<u64> {
        self.lock().get(key).map(BenchMapValue::checksum)
    }

    fn remove(&self, key: &u64) -> Option<V> {
        self.lock().remove(key)
    }

    fn range_count(&self, start: u64, end: u64) -> usize {
        self.lock().range(start..end).count()
    }

    fn range_checksum(&self, start: u64, end: u64) -> u64 {
        self.lock().range(start..end).fold(0_u64, |checksum, (key, value)| {
            checksum.wrapping_add(*key).wrapping_add(value.checksum())
        })
    }

    fn len(&self) -> usize {
        self.lock().len()
    }
}

struct Fixture<V, M>
where
    V: BenchMapValue + Send + Sync,
    M: MapImplementation<V>,
{
    base_entries: Vec<(u64, V)>,
    operations: Vec<Vec<MapOperation<V>>>,
    stable_map: Option<Arc<M>>,
}

impl<V, M> Fixture<V, M>
where
    V: BenchMapValue + Send + Sync,
    M: MapImplementation<V>,
{
    fn new(scenario: MapScenario, map_size: usize, node_capacity: usize, thread_count: usize) -> Self {
        let base_entries = ValueGenerator::new(map_size).map_base_entries();
        let operations = map_operations(scenario, map_size, thread_count);
        scenario.validate_operations(&operations, thread_count);
        let stable_map = (!scenario.needs_fresh_map()).then(|| Arc::new(M::build(&base_entries, node_capacity)));

        Self {
            base_entries,
            operations,
            stable_map,
        }
    }
}

pub fn bench_multithreaded_case<V, M>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    scenario: MapScenario,
    map_size: usize,
    node_capacity: usize,
    thread_count: usize,
) where
    V: BenchMapValue + Send + Sync,
    M: MapImplementation<V>,
{
    let id = BenchmarkId::new(
        M::benchmark_id(node_capacity, Some(thread_count)),
        format!("n{map_size}"),
    );
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let fixture =
            fixture.get_or_insert_with(|| Fixture::<V, M>::new(scenario, map_size, node_capacity, thread_count));

        b.iter_custom(|iterations| {
            let mut total_elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let map = if scenario.needs_fresh_map() {
                    Arc::new(M::build(&fixture.base_entries, node_capacity))
                } else {
                    Arc::clone(fixture.stable_map.as_ref().expect("stable scenario should have a map"))
                };
                let (elapsed, stats) =
                    run_parallel(Arc::clone(&map), fixture.operations.clone(), apply_operation::<V, M>);
                total_elapsed += elapsed;

                black_box(stats.checksum);
                assert_eq!(stats.operations, scenario.operation_count());
                assert_eq!(stats.successes, scenario.expected_successes());
                assert_eq!(stats.updates, scenario.expected_updates());
                assert_eq!(map.len(), scenario.expected_len(map_size));
                if matches!(scenario, MapScenario::InsertBatchNew | MapScenario::InsertBatchUpdate) {
                    for operation in fixture.operations.iter().flatten() {
                        if let MapOperation::Insert(key, value) = operation {
                            assert_eq!(map.get_checksum(key), Some(value.checksum()));
                        }
                    }
                }
            }

            total_elapsed
        });
    });
}

pub fn bench_insert_one<V, M>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    map_size: usize,
    node_capacity: usize,
    kind: MapInsertionKind,
) where
    V: BenchMapValue + Send + Sync,
    M: MapImplementation<V>,
{
    let id = BenchmarkId::new(M::benchmark_id(node_capacity, None), format!("n{map_size}"));
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let (base_entries, insertions) = fixture.get_or_insert_with(|| {
            let generator = ValueGenerator::new(map_size);
            (
                generator.map_base_entries::<V>(),
                generator.map_insertions::<V>(SINGLE_OPERATION_BATCH_SIZE, kind),
            )
        });

        b.iter_custom(|iterations| {
            let mut total_elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let mut insertion_batch = insertions.clone();
                let map = M::build(base_entries, node_capacity);

                let start = Instant::now();
                for (key, value) in insertion_batch.drain(..) {
                    black_box(map.insert(black_box(key), black_box(value)));
                }
                total_elapsed += start.elapsed();

                let inserted_count = match kind {
                    MapInsertionKind::New => SINGLE_OPERATION_BATCH_SIZE,
                    MapInsertionKind::Update => 0,
                };
                assert_eq!(map.len(), map_size + inserted_count);
                for (key, value) in insertions.iter() {
                    assert_eq!(map.get_checksum(key), Some(value.checksum()));
                }
            }

            total_elapsed / insertions.len() as u32
        });
    });
}

pub fn bench_get<V, M>(group: &mut BenchmarkGroup<'_, WallTime>, map_size: usize, node_capacity: usize, hit: bool)
where
    V: BenchMapValue + Send + Sync,
    M: MapImplementation<V>,
{
    let id = BenchmarkId::new(M::benchmark_id(node_capacity, None), format!("n{map_size}"));
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let (map, queries) = fixture.get_or_insert_with(|| {
            let generator = ValueGenerator::new(map_size);
            let queries = if hit {
                generator.hit_keys()
            } else {
                generator.miss_keys()
            };
            let map = M::build(&generator.map_base_entries::<V>(), node_capacity);
            assert!(queries.iter().all(|key| map.get_checksum(key).is_some() == hit));
            (map, queries)
        });

        b.iter_custom(|iterations| {
            let mut total_elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let start = Instant::now();
                for key in black_box(queries.as_slice()) {
                    black_box(map.get_checksum(black_box(key)));
                }
                total_elapsed += start.elapsed();
            }

            total_elapsed / queries.len() as u32
        });
    });
}

pub fn bench_remove<V, M>(group: &mut BenchmarkGroup<'_, WallTime>, map_size: usize, node_capacity: usize, hit: bool)
where
    V: BenchMapValue + Send + Sync,
    M: MapImplementation<V>,
{
    let id = BenchmarkId::new(M::benchmark_id(node_capacity, None), format!("n{map_size}"));
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let (base_entries, keys) = fixture.get_or_insert_with(|| {
            let generator = ValueGenerator::new(map_size);
            let base_entries = generator.map_base_entries::<V>();
            let keys = if hit {
                generator.hit_keys()
            } else {
                generator.miss_keys()
            };
            let validation_map = M::build(&base_entries, node_capacity);
            assert!(keys.iter().all(|key| validation_map.remove(key).is_some() == hit));
            (base_entries, keys)
        });

        b.iter_custom(|iterations| {
            let mut total_elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let map = M::build(base_entries, node_capacity);

                let start = Instant::now();
                for key in black_box(keys.as_slice()) {
                    black_box(map.remove(black_box(key)));
                }
                total_elapsed += start.elapsed();

                let expected_len = map_size - usize::from(hit) * keys.len();
                assert_eq!(map.len(), expected_len);
            }

            total_elapsed / keys.len() as u32
        });
    });
}

pub fn bench_range<V, M>(group: &mut BenchmarkGroup<'_, WallTime>, map_size: usize, node_capacity: usize)
where
    V: BenchMapValue + Send + Sync,
    M: MapImplementation<V>,
{
    let start = map_size as u64;
    let end = start + (RANGE_LEN * 2) as u64;
    let id = BenchmarkId::new(M::benchmark_id(node_capacity, None), format!("n{map_size}"));
    let mut map = None;

    group.bench_function(id, move |b| {
        let map = map.get_or_insert_with(|| {
            let generator = ValueGenerator::new(map_size);
            let map = M::build(&generator.map_base_entries::<V>(), node_capacity);
            assert_eq!(map.range_count(start, end), RANGE_LEN);
            map
        });
        b.iter(|| black_box(map.range_checksum(start, end)));
    });
}

fn apply_operation<V, M>(map: &M, operation: MapOperation<V>, stats: &mut WorkerStats)
where
    V: BenchMapValue + Send + Sync,
    M: MapImplementation<V>,
{
    match operation {
        MapOperation::Get(key) => {
            let checksum = black_box(map.get_checksum(black_box(&key)));
            stats.operations += 1;
            if let Some(checksum) = checksum {
                stats.successes += 1;
                stats.checksum ^= key ^ checksum;
            }
        }
        MapOperation::Insert(key, value) => {
            let checksum = value.checksum();
            let previous = black_box(map.insert(black_box(key), black_box(value)));
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
            let removed = black_box(map.remove(black_box(&key)));
            stats.operations += 1;
            if let Some(value) = removed {
                stats.successes += 1;
                stats.checksum ^= key ^ value.checksum();
            }
        }
    }
}
