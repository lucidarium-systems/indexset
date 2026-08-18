#[path = "multimap/dataset.rs"]
mod dataset;
#[path = "multimap/workload.rs"]
mod workload;

use self::dataset::{BenchMultiMapValue, LargeValue, MultiMapDataset, MultiMapFanout};
use self::workload::{
    insertions, operations, MultiMapInsertionKind, MultiMapOperation, MultiMapScenario, INSERT_ONE_BATCH_SIZE,
};
use super::workload::{maximum_thread_count, run_parallel, thread_counts, WorkerStats};
use crate::value_generator::{DEFAULT_NODE_CAPACITY, NODE_CAPACITIES, SET_SIZES};
use criterion::{black_box, measurement::WallTime, BenchmarkGroup, BenchmarkId, Criterion, SamplingMode, Throughput};
use indexset::concurrent::multimap::BTreeMultiMap;
use indexset::core::multipair::OrdMultiPair;
use std::sync::Arc;
use std::time::{Duration, Instant};

const CAPACITY_SWEEP_SIZES: [usize; 2] = [100_000, 1_000_000];

pub type RandomMultiMap<V> = BTreeMultiMap<u64, V>;
pub type OrderedMultiMap<V> = BTreeMultiMap<u64, V, Vec<OrdMultiPair<u64, V>>, OrdMultiPair<u64, V>>;

pub trait MultiMapImplementation<V>: Send + Sync + Sized + 'static
where
    V: BenchMultiMapValue + Send + Sync,
{
    const ID: &'static str;

    fn build(entries: &[(u64, V)], node_capacity: usize) -> Self;
    fn insert(&self, key: u64, value: V) -> Option<V>;
    fn get_checksum(&self, key: &u64) -> (usize, u64);
    fn contains_pair(&self, key: &u64, value: &V) -> bool;
    fn remove_exact(&self, key: &u64, value: &V) -> Option<(u64, V)>;
    fn range_checksum(&self, start: u64, end: u64) -> (usize, u64);
    fn len(&self) -> usize;

    fn benchmark_id(node_capacity: usize, thread_count: Option<usize>) -> String {
        let mut id = format!("{}_cap{node_capacity}", Self::ID);
        if let Some(thread_count) = thread_count {
            id.push_str(&format!("_t{thread_count}"));
        }
        id
    }
}

macro_rules! impl_multimap {
    ($map:ty, $id:literal) => {
        impl<V> MultiMapImplementation<V> for $map
        where
            V: BenchMultiMapValue + Send + Sync,
        {
            const ID: &'static str = $id;

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

            fn get_checksum(&self, key: &u64) -> (usize, u64) {
                self.get(key).fold((0, 0_u64), |(count, checksum), (key, value)| {
                    (
                        count + 1,
                        checksum.wrapping_add(*key).wrapping_add(value.checksum()),
                    )
                })
            }

            fn contains_pair(&self, key: &u64, value: &V) -> bool {
                self.get(key).any(|(_, candidate)| candidate == value)
            }

            fn remove_exact(&self, key: &u64, value: &V) -> Option<(u64, V)> {
                self.remove(key, value)
            }

            fn range_checksum(&self, start: u64, end: u64) -> (usize, u64) {
                self.range(start..end)
                    .fold((0, 0_u64), |(count, checksum), (key, value)| {
                        (
                            count + 1,
                            checksum.wrapping_add(*key).wrapping_add(value.checksum()),
                        )
                    })
            }

            fn len(&self) -> usize {
                self.len()
            }
        }
    };
}

impl_multimap!(RandomMultiMap<V>, "random");
impl_multimap!(OrderedMultiMap<V>, "ord");

struct Fixture<V, M>
where
    V: BenchMultiMapValue + Send + Sync,
    M: MultiMapImplementation<V>,
{
    dataset: MultiMapDataset<V>,
    operations: Vec<Vec<MultiMapOperation<V>>>,
    stable_map: Option<Arc<M>>,
}

impl<V, M> Fixture<V, M>
where
    V: BenchMultiMapValue + Send + Sync,
    M: MultiMapImplementation<V>,
{
    fn new(
        scenario: MultiMapScenario,
        pair_count: usize,
        fanout: MultiMapFanout,
        node_capacity: usize,
        thread_count: usize,
    ) -> Self {
        let dataset = MultiMapDataset::new(pair_count, fanout);
        let operations = operations(scenario, &dataset, fanout, thread_count);
        let stable_map = (!scenario.needs_fresh_map()).then(|| Arc::new(M::build(&dataset.entries, node_capacity)));

        Self {
            dataset,
            operations,
            stable_map,
        }
    }
}

pub fn bench_parallel_case<V, M>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    scenario: MultiMapScenario,
    pair_count: usize,
    fanout: MultiMapFanout,
    node_capacity: usize,
    thread_count: usize,
) where
    V: BenchMultiMapValue + Send + Sync,
    M: MultiMapImplementation<V>,
{
    let id = BenchmarkId::new(
        M::benchmark_id(node_capacity, Some(thread_count)),
        format!("pairs{pair_count}"),
    );
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let fixture = fixture
            .get_or_insert_with(|| Fixture::<V, M>::new(scenario, pair_count, fanout, node_capacity, thread_count));

        b.iter_custom(|iterations| {
            let mut total_elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let map = if scenario.needs_fresh_map() {
                    Arc::new(M::build(&fixture.dataset.entries, node_capacity))
                } else {
                    Arc::clone(fixture.stable_map.as_ref().expect("stable scenario should have a map"))
                };
                let (elapsed, stats) =
                    run_parallel(Arc::clone(&map), fixture.operations.clone(), apply_operation::<V, M>);
                total_elapsed += elapsed;

                black_box(stats.checksum);
                assert_eq!(stats.operations, scenario.operation_count(fanout));
                assert_eq!(stats.successes, scenario.expected_successes(fanout));
                assert_eq!(stats.validations, scenario.operation_count(fanout));
                assert_eq!(stats.updates, 0);
                assert_eq!(map.len(), scenario.expected_len(pair_count));
                verify_mutation_results(map.as_ref(), scenario, &fixture.operations);
            }

            total_elapsed
        });
    });
}

fn bench_insert_one_case<V, M>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    pair_count: usize,
    fanout: MultiMapFanout,
    node_capacity: usize,
    kind: MultiMapInsertionKind,
) where
    V: BenchMultiMapValue + Send + Sync,
    M: MultiMapImplementation<V>,
{
    let id = BenchmarkId::new(M::benchmark_id(node_capacity, None), format!("pairs{pair_count}"));
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let (dataset, insertions) = fixture.get_or_insert_with(|| {
            let dataset = MultiMapDataset::new(pair_count, fanout);
            let insertions = insertions(&dataset, INSERT_ONE_BATCH_SIZE, kind);
            (dataset, insertions)
        });

        b.iter_custom(|iterations| {
            let mut total_elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let insertion_batch = insertions.clone();
                let map = M::build(&dataset.entries, node_capacity);

                let start = Instant::now();
                for (key, value) in insertion_batch {
                    black_box(map.insert(black_box(key), black_box(value)));
                }
                total_elapsed += start.elapsed();

                assert_eq!(map.len(), pair_count + INSERT_ONE_BATCH_SIZE);
                for (key, value) in insertions.iter() {
                    assert!(map.contains_pair(key, value));
                }
            }

            total_elapsed / INSERT_ONE_BATCH_SIZE as u32
        });
    });
}

fn apply_operation<V, M>(map: &M, operation: MultiMapOperation<V>, stats: &mut WorkerStats)
where
    V: BenchMultiMapValue + Send + Sync,
    M: MultiMapImplementation<V>,
{
    match operation {
        MultiMapOperation::Get { key, expected } => {
            let actual = black_box(map.get_checksum(black_box(&key)));
            stats.operations += 1;
            stats.successes += usize::from(actual.0 > 0);
            stats.validations += usize::from(expected.map_or(actual.0 > 0, |expected| actual == expected));
            stats.checksum ^= key ^ actual.0 as u64 ^ actual.1;
        }
        MultiMapOperation::Insert(key, value) => {
            let checksum = value.checksum();
            let previous = black_box(map.insert(black_box(key), black_box(value)));
            stats.operations += 1;
            stats.successes += usize::from(previous.is_none());
            stats.validations += usize::from(previous.is_none());
            stats.checksum ^= key ^ checksum ^ previous.map_or(0, |value| value.checksum());
        }
        MultiMapOperation::RemoveExact(key, value) => {
            let removed = black_box(map.remove_exact(black_box(&key), black_box(&value)));
            let valid = removed
                .as_ref()
                .is_some_and(|(removed_key, removed_value)| *removed_key == key && removed_value == &value);
            stats.operations += 1;
            stats.successes += usize::from(removed.is_some());
            stats.validations += usize::from(valid);
            if let Some((removed_key, removed_value)) = removed {
                stats.checksum ^= removed_key ^ removed_value.checksum();
            }
        }
        MultiMapOperation::Range { start, end, expected } => {
            let actual = black_box(map.range_checksum(black_box(start), black_box(end)));
            let valid = actual == expected;
            stats.operations += 1;
            stats.successes += usize::from(valid);
            stats.validations += usize::from(valid);
            stats.checksum ^= start ^ end ^ actual.0 as u64 ^ actual.1;
        }
    }
}

fn verify_mutation_results<V, M>(map: &M, scenario: MultiMapScenario, operations: &[Vec<MultiMapOperation<V>>])
where
    V: BenchMultiMapValue + Send + Sync,
    M: MultiMapImplementation<V>,
{
    match scenario {
        MultiMapScenario::InsertNewKey | MultiMapScenario::InsertExistingKey => {
            for operation in operations.iter().flatten() {
                if let MultiMapOperation::Insert(key, value) = operation {
                    assert!(map.contains_pair(key, value));
                }
            }
        }
        MultiMapScenario::RemoveExactHit => {
            for operation in operations.iter().flatten() {
                if let MultiMapOperation::RemoveExact(key, value) = operation {
                    assert!(!map.contains_pair(key, value));
                }
            }
        }
        MultiMapScenario::GetHit
        | MultiMapScenario::GetMiss
        | MultiMapScenario::Range128Keys
        | MultiMapScenario::MixedReadHeavy
        | MultiMapScenario::MixedBalanced => {}
    }
}

fn bench_insert_one_scenario_for<V>(c: &mut Criterion, fanout: MultiMapFanout, kind: MultiMapInsertionKind)
where
    V: BenchMultiMapValue + Send + Sync,
{
    let mut group = c.benchmark_group(format!(
        "concurrent_multimap_v1/insert_one/{}/{}/{}",
        V::ID,
        fanout.id(),
        kind.id()
    ));
    group.throughput(Throughput::Elements(1));
    group.sample_size(20);

    for pair_count in SET_SIZES {
        for node_capacity in NODE_CAPACITIES {
            bench_insert_one_case::<V, RandomMultiMap<V>>(&mut group, pair_count, fanout, node_capacity, kind);
            bench_insert_one_case::<V, OrderedMultiMap<V>>(&mut group, pair_count, fanout, node_capacity, kind);
        }
    }

    group.finish();
}

fn bench_insert_one_for<V>(c: &mut Criterion)
where
    V: BenchMultiMapValue + Send + Sync,
{
    for fanout in MultiMapFanout::ALL {
        bench_insert_one_scenario_for::<V>(c, fanout, MultiMapInsertionKind::NewKey);
        bench_insert_one_scenario_for::<V>(c, fanout, MultiMapInsertionKind::ExistingKey);
    }
}

pub fn bench_insert_one(c: &mut Criterion) {
    bench_insert_one_for::<u64>(c);
    bench_insert_one_for::<LargeValue>(c);
}

fn bench_scenario_for<V>(c: &mut Criterion, scenario: MultiMapScenario, fanout: MultiMapFanout)
where
    V: BenchMultiMapValue + Send + Sync,
{
    let mut group = c.benchmark_group(format!(
        "concurrent_multimap_v1/{}/{}/{}",
        scenario.id(),
        V::ID,
        fanout.id()
    ));
    group.sampling_mode(SamplingMode::Flat);

    for pair_count in SET_SIZES {
        group.throughput(Throughput::Elements(
            scenario.throughput_elements(pair_count, fanout) as u64
        ));
        for thread_count in thread_counts() {
            bench_parallel_case::<V, RandomMultiMap<V>>(
                &mut group,
                scenario,
                pair_count,
                fanout,
                DEFAULT_NODE_CAPACITY,
                thread_count,
            );
            bench_parallel_case::<V, OrderedMultiMap<V>>(
                &mut group,
                scenario,
                pair_count,
                fanout,
                DEFAULT_NODE_CAPACITY,
                thread_count,
            );
        }
    }

    group.finish();
}

pub fn bench_parallel(c: &mut Criterion) {
    for scenario in MultiMapScenario::ALL {
        for fanout in MultiMapFanout::ALL {
            bench_scenario_for::<u64>(c, scenario, fanout);
            bench_scenario_for::<LargeValue>(c, scenario, fanout);
        }
    }
}

fn bench_capacity_scenario_for<V>(c: &mut Criterion, scenario: MultiMapScenario, fanout: MultiMapFanout)
where
    V: BenchMultiMapValue + Send + Sync,
{
    let mut group = c.benchmark_group(format!(
        "concurrent_multimap_v1/cap/{}/{}/{}",
        scenario.id(),
        V::ID,
        fanout.id()
    ));
    group.sampling_mode(SamplingMode::Flat);

    let thread_count = maximum_thread_count();
    for pair_count in CAPACITY_SWEEP_SIZES {
        group.throughput(Throughput::Elements(
            scenario.throughput_elements(pair_count, fanout) as u64
        ));
        for node_capacity in NODE_CAPACITIES {
            if node_capacity == DEFAULT_NODE_CAPACITY {
                continue;
            }

            bench_parallel_case::<V, RandomMultiMap<V>>(
                &mut group,
                scenario,
                pair_count,
                fanout,
                node_capacity,
                thread_count,
            );
            bench_parallel_case::<V, OrderedMultiMap<V>>(
                &mut group,
                scenario,
                pair_count,
                fanout,
                node_capacity,
                thread_count,
            );
        }
    }

    group.finish();
}

pub fn bench_capacity_sweep(c: &mut Criterion) {
    for scenario in MultiMapScenario::CAPACITY_SWEEP {
        for fanout in MultiMapFanout::ALL {
            bench_capacity_scenario_for::<u64>(c, scenario, fanout);
            bench_capacity_scenario_for::<LargeValue>(c, scenario, fanout);
        }
    }
}
