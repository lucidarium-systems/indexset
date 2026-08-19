use super::workload::{run_parallel, Operation, Scenario, WorkerStats};
use crate::value_generator::{BenchValue, ValueGenerator, RANGE_LEN};
use criterion::{black_box, measurement::WallTime, BenchmarkGroup, BenchmarkId};
use parking_lot::Mutex;
use std::collections::BTreeSet as StdBTreeSet;
use std::fmt::Debug;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub type ConcurrentSet<T> = indexset::concurrent::set::BTreeSet<T>;
pub type MutexIndexSet<T> = Mutex<indexset::BTreeSet<T>>;
pub type MutexStdBTreeSet<T> = Mutex<StdBTreeSet<T>>;

pub trait SetImplementation<T>: Send + Sync + Sized + 'static
where
    T: BenchValue + Debug + Send + Sync,
{
    const ID: &'static str;
    const USES_NODE_CAPACITY: bool;

    fn build(values: &[T], node_capacity: usize) -> Self;
    fn insert(&self, value: T) -> bool;
    fn contains(&self, key: &u64) -> bool;
    fn remove(&self, key: &u64) -> bool;
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

impl<T> SetImplementation<T> for ConcurrentSet<T>
where
    T: BenchValue + Debug + Send + Sync,
{
    const ID: &'static str = "concurrent";
    const USES_NODE_CAPACITY: bool = true;

    fn build(values: &[T], node_capacity: usize) -> Self {
        let set = Self::with_maximum_node_size(node_capacity);
        for value in values {
            assert!(set.insert(value.clone()));
        }
        set
    }

    fn insert(&self, value: T) -> bool {
        self.insert(value)
    }

    fn contains(&self, key: &u64) -> bool {
        self.contains(key)
    }

    fn remove(&self, key: &u64) -> bool {
        self.remove(key).is_some()
    }

    fn range_count(&self, start: u64, end: u64) -> usize {
        self.range(start..end).count()
    }

    fn range_checksum(&self, start: u64, end: u64) -> u64 {
        self.range(start..end)
            .fold(0_u64, |checksum, value| checksum.wrapping_add(value.key()))
    }

    fn len(&self) -> usize {
        self.len()
    }
}

impl<T> SetImplementation<T> for MutexIndexSet<T>
where
    T: BenchValue + Debug + Send + Sync,
{
    const ID: &'static str = "indexset_mutex";
    const USES_NODE_CAPACITY: bool = true;

    fn build(values: &[T], node_capacity: usize) -> Self {
        let mut set = indexset::BTreeSet::with_maximum_node_size(node_capacity);
        for value in values {
            assert!(set.insert(value.clone()));
        }
        Self::new(set)
    }

    fn insert(&self, value: T) -> bool {
        self.lock().insert(value)
    }

    fn contains(&self, key: &u64) -> bool {
        self.lock().contains(key)
    }

    fn remove(&self, key: &u64) -> bool {
        self.lock().remove(key)
    }

    fn range_count(&self, start: u64, end: u64) -> usize {
        self.lock().range(start..end).count()
    }

    fn range_checksum(&self, start: u64, end: u64) -> u64 {
        self.lock()
            .range(start..end)
            .fold(0_u64, |checksum, value| checksum.wrapping_add(value.key()))
    }

    fn len(&self) -> usize {
        self.lock().len()
    }
}

impl<T> SetImplementation<T> for MutexStdBTreeSet<T>
where
    T: BenchValue + Debug + Send + Sync,
{
    const ID: &'static str = "std_btree_set_mutex";
    const USES_NODE_CAPACITY: bool = false;

    fn build(values: &[T], _node_capacity: usize) -> Self {
        Self::new(values.iter().cloned().collect())
    }

    fn insert(&self, value: T) -> bool {
        self.lock().insert(value)
    }

    fn contains(&self, key: &u64) -> bool {
        self.lock().contains(key)
    }

    fn remove(&self, key: &u64) -> bool {
        self.lock().take(key).is_some()
    }

    fn range_count(&self, start: u64, end: u64) -> usize {
        self.lock().range(start..end).count()
    }

    fn range_checksum(&self, start: u64, end: u64) -> u64 {
        self.lock()
            .range(start..end)
            .fold(0_u64, |checksum, value| checksum.wrapping_add(value.key()))
    }

    fn len(&self) -> usize {
        self.lock().len()
    }
}

struct Fixture<T, S>
where
    T: BenchValue + Debug + Send + Sync,
    S: SetImplementation<T>,
{
    base_values: Vec<T>,
    operations: Vec<Vec<Operation<T>>>,
    stable_set: Option<Arc<S>>,
}

impl<T, S> Fixture<T, S>
where
    T: BenchValue + Debug + Send + Sync,
    S: SetImplementation<T>,
{
    fn new(scenario: Scenario, set_size: usize, node_capacity: usize, thread_count: usize) -> Self {
        let base_values = ValueGenerator::new(set_size).base_values();
        let operations = super::workload::operations(scenario, set_size, thread_count);
        scenario.validate_operations(&operations, thread_count);
        let stable_set = (!scenario.needs_fresh_set()).then(|| Arc::new(S::build(&base_values, node_capacity)));

        Self {
            base_values,
            operations,
            stable_set,
        }
    }
}

pub fn bench_multithreaded_case<T, S>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    scenario: Scenario,
    set_size: usize,
    node_capacity: usize,
    thread_count: usize,
) where
    T: BenchValue + Debug + Send + Sync,
    S: SetImplementation<T>,
{
    let id = BenchmarkId::new(
        S::benchmark_id(node_capacity, Some(thread_count)),
        format!("n{set_size}"),
    );
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let fixture =
            fixture.get_or_insert_with(|| Fixture::<T, S>::new(scenario, set_size, node_capacity, thread_count));

        b.iter_custom(|iterations| {
            let mut total_elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let set = if scenario.needs_fresh_set() {
                    Arc::new(S::build(&fixture.base_values, node_capacity))
                } else {
                    Arc::clone(fixture.stable_set.as_ref().expect("stable scenario should have a set"))
                };
                let (elapsed, stats) =
                    run_parallel(Arc::clone(&set), fixture.operations.clone(), apply_operation::<T, S>);
                total_elapsed += elapsed;

                black_box(stats.checksum);
                assert_eq!(stats.operations, scenario.operation_count());
                assert_eq!(stats.successes, scenario.expected_successes());
                assert_eq!(set.len(), scenario.expected_len(set_size));
            }

            total_elapsed
        });
    });
}

pub fn bench_insert_one<T, S, F>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    set_size: usize,
    node_capacity: usize,
    expected_new_count: usize,
    make_insertions: F,
) where
    T: BenchValue + Debug + Send + Sync,
    S: SetImplementation<T>,
    F: Fn(&ValueGenerator) -> Vec<T> + 'static,
{
    let id = BenchmarkId::new(S::benchmark_id(node_capacity, None), format!("n{set_size}"));
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let (base_values, insertions) = fixture.get_or_insert_with(|| {
            let generator = ValueGenerator::new(set_size);
            (generator.base_values::<T>(), make_insertions(&generator))
        });

        b.iter_custom(|iterations| {
            let mut total_elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let mut insertion_batch = insertions.clone();
                let set = S::build(base_values, node_capacity);

                let start = Instant::now();
                for value in insertion_batch.drain(..) {
                    black_box(set.insert(black_box(value)));
                }
                total_elapsed += start.elapsed();

                assert_eq!(set.len(), set_size + expected_new_count);
            }

            total_elapsed / insertions.len() as u32
        });
    });
}

pub fn bench_contains<T, S>(group: &mut BenchmarkGroup<'_, WallTime>, set_size: usize, node_capacity: usize, hit: bool)
where
    T: BenchValue + Debug + Send + Sync,
    S: SetImplementation<T>,
{
    let id = BenchmarkId::new(S::benchmark_id(node_capacity, None), format!("n{set_size}"));
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let (set, queries) = fixture.get_or_insert_with(|| {
            let generator = ValueGenerator::new(set_size);
            let queries = if hit {
                generator.hit_keys()
            } else {
                generator.miss_keys()
            };
            let set = S::build(&generator.base_values::<T>(), node_capacity);
            assert!(queries.iter().all(|key| set.contains(key) == hit));
            (set, queries)
        });

        b.iter_custom(|iterations| {
            let mut total_elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let start = Instant::now();
                for key in black_box(queries.as_slice()) {
                    black_box(set.contains(key));
                }
                total_elapsed += start.elapsed();
            }

            total_elapsed / queries.len() as u32
        });
    });
}

pub fn bench_remove<T, S>(group: &mut BenchmarkGroup<'_, WallTime>, set_size: usize, node_capacity: usize, hit: bool)
where
    T: BenchValue + Debug + Send + Sync,
    S: SetImplementation<T>,
{
    let id = BenchmarkId::new(S::benchmark_id(node_capacity, None), format!("n{set_size}"));
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let (base_values, keys) = fixture.get_or_insert_with(|| {
            let generator = ValueGenerator::new(set_size);
            let base_values = generator.base_values::<T>();
            let keys = if hit {
                generator.hit_keys()
            } else {
                generator.miss_keys()
            };
            let validation_set = S::build(&base_values, node_capacity);
            assert!(keys.iter().all(|key| validation_set.remove(key) == hit));
            (base_values, keys)
        });

        b.iter_custom(|iterations| {
            let mut total_elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let set = S::build(base_values, node_capacity);

                let start = Instant::now();
                for key in black_box(keys.as_slice()) {
                    black_box(set.remove(black_box(key)));
                }
                total_elapsed += start.elapsed();

                let expected_len = set_size - usize::from(hit) * keys.len();
                assert_eq!(set.len(), expected_len);
            }

            total_elapsed / keys.len() as u32
        });
    });
}

pub fn bench_range<T, S>(group: &mut BenchmarkGroup<'_, WallTime>, set_size: usize, node_capacity: usize)
where
    T: BenchValue + Debug + Send + Sync,
    S: SetImplementation<T>,
{
    let start = set_size as u64;
    let end = start + (RANGE_LEN * 2) as u64;
    let id = BenchmarkId::new(S::benchmark_id(node_capacity, None), format!("n{set_size}"));
    let mut set = None;

    group.bench_function(id, move |b| {
        let set = set.get_or_insert_with(|| {
            let generator = ValueGenerator::new(set_size);
            let set = S::build(&generator.base_values::<T>(), node_capacity);
            assert_eq!(set.range_count(start, end), RANGE_LEN);
            set
        });
        b.iter(|| black_box(set.range_checksum(start, end)));
    });
}

fn apply_operation<T, S>(set: &S, operation: Operation<T>, stats: &mut WorkerStats)
where
    T: BenchValue + Debug + Send + Sync,
    S: SetImplementation<T>,
{
    match operation {
        Operation::Contains(key) => {
            let found = black_box(set.contains(black_box(&key)));
            stats.operations += 1;
            stats.successes += usize::from(found);
            stats.checksum ^= key.wrapping_mul(u64::from(found));
        }
        Operation::Insert(value) => {
            let key = value.key();
            let inserted = black_box(set.insert(black_box(value)));
            stats.operations += 1;
            stats.successes += usize::from(inserted);
            stats.checksum ^= key.wrapping_mul(u64::from(inserted));
        }
        Operation::Remove(key) => {
            let removed = black_box(set.remove(black_box(&key)));
            stats.operations += 1;
            stats.successes += usize::from(removed);
            if removed {
                stats.checksum ^= key;
            }
        }
    }
}
