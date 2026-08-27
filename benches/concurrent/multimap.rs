#[path = "multimap/dataset.rs"]
mod dataset;
#[path = "multimap/workload.rs"]
mod workload;

use self::dataset::{BenchMultiMapValue, LargeValue, MultiMapDataset, MultiMapFanout};
use self::workload::{
    insertions, operations, point_queries, removal_pairs, MultiMapInsertionKind, MultiMapOperation, MultiMapScenario,
};
use super::workload::{run_parallel, thread_counts, WorkerStats};
use crate::value_generator::{NODE_CAPACITIES, SET_SIZES, SINGLE_OPERATION_BATCH_SIZE};
use criterion::{black_box, measurement::WallTime, BenchmarkGroup, BenchmarkId, Criterion, SamplingMode, Throughput};
use indexset::concurrent::multimap::BTreeMultiMap;
use indexset::core::multipair::OrdMultiPair;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    fn remove_pair(&self, key: &u64, value: &V) -> Option<(u64, V)>;
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
                assert_eq!(map.len(), entries.len());
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

            fn remove_pair(&self, key: &u64, value: &V) -> Option<(u64, V)> {
                self.remove(key, value)
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
        let operations = operations(scenario, &dataset, thread_count);
        scenario.validate_operations(&operations, thread_count);
        let stable_map = (!scenario.needs_fresh_map()).then(|| Arc::new(M::build(&dataset.entries, node_capacity)));

        Self {
            dataset,
            operations,
            stable_map,
        }
    }
}

fn bench_multithreaded_case<V, M>(
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
        format!("n{pair_count}"),
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
                assert_eq!(stats.operations, scenario.operation_count());
                assert_eq!(stats.successes, scenario.expected_successes());
                assert_eq!(stats.validations, scenario.operation_count());
                assert_eq!(stats.updates, 0);
                assert_eq!(map.len(), scenario.expected_len(pair_count));
                verify_insertions(map.as_ref(), scenario, &fixture.operations);
            }

            total_elapsed
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
            let valid = expected.map_or(actual.0 > 0, |expected| actual == expected);
            stats.operations += 1;
            stats.successes += usize::from(actual.0 > 0);
            stats.validations += usize::from(valid);
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
        MultiMapOperation::RemovePair(key, value) => {
            let removed = black_box(map.remove_pair(black_box(&key), black_box(&value)));
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
    }
}

fn verify_insertions<V, M>(map: &M, scenario: MultiMapScenario, operations: &[Vec<MultiMapOperation<V>>])
where
    V: BenchMultiMapValue + Send + Sync,
    M: MultiMapImplementation<V>,
{
    if matches!(
        scenario,
        MultiMapScenario::InsertBatchNew | MultiMapScenario::InsertBatchExisting
    ) {
        for operation in operations.iter().flatten() {
            if let MultiMapOperation::Insert(key, value) = operation {
                assert!(map.contains_pair(key, value));
            }
        }
    }
}

fn bench_insert_case<V, M>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    pair_count: usize,
    fanout: MultiMapFanout,
    node_capacity: usize,
    kind: MultiMapInsertionKind,
) where
    V: BenchMultiMapValue + Send + Sync,
    M: MultiMapImplementation<V>,
{
    let id = BenchmarkId::new(M::benchmark_id(node_capacity, None), format!("n{pair_count}"));
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let (dataset, insertions) = fixture.get_or_insert_with(|| {
            let dataset = MultiMapDataset::new(pair_count, fanout);
            let insertions = insertions(&dataset, SINGLE_OPERATION_BATCH_SIZE, kind);
            (dataset, insertions)
        });

        b.iter_custom(|iterations| {
            let mut total_elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let mut insertion_batch = insertions.clone();
                let map = M::build(&dataset.entries, node_capacity);

                let start = Instant::now();
                for (key, value) in insertion_batch.drain(..) {
                    black_box(map.insert(black_box(key), black_box(value)));
                }
                total_elapsed += start.elapsed();

                assert_eq!(
                    insertions
                        .iter()
                        .filter(|(key, value)| map.contains_pair(key, value))
                        .count(),
                    SINGLE_OPERATION_BATCH_SIZE
                );
                assert_eq!(map.len(), pair_count + SINGLE_OPERATION_BATCH_SIZE);
            }

            total_elapsed / insertions.len() as u32
        });
    });
}

fn bench_get_case<V, M>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    pair_count: usize,
    fanout: MultiMapFanout,
    node_capacity: usize,
    hit: bool,
) where
    V: BenchMultiMapValue + Send + Sync,
    M: MultiMapImplementation<V>,
{
    let id = BenchmarkId::new(M::benchmark_id(node_capacity, None), format!("n{pair_count}"));
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let (map, queries) = fixture.get_or_insert_with(|| {
            let dataset = MultiMapDataset::<V>::new(pair_count, fanout);
            let queries = point_queries(&dataset, hit);
            let map = M::build(&dataset.entries, node_capacity);
            assert!(queries
                .iter()
                .all(|query| map.get_checksum(&query.key) == query.expected));
            (map, queries)
        });

        b.iter_custom(|iterations| {
            let mut total_elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let start = Instant::now();
                for query in black_box(queries.as_slice()) {
                    black_box(map.get_checksum(black_box(&query.key)));
                }
                total_elapsed += start.elapsed();
            }

            total_elapsed / queries.len() as u32
        });
    });
}

fn bench_remove_pair_case<V, M>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    pair_count: usize,
    fanout: MultiMapFanout,
    node_capacity: usize,
    hit: bool,
) where
    V: BenchMultiMapValue + Send + Sync,
    M: MultiMapImplementation<V>,
{
    let id = BenchmarkId::new(M::benchmark_id(node_capacity, None), format!("n{pair_count}"));
    let mut fixture = None;

    group.bench_function(id, move |b| {
        let (dataset, removals) = fixture.get_or_insert_with(|| {
            let dataset = MultiMapDataset::new(pair_count, fanout);
            let removals = removal_pairs(&dataset, hit);
            let validation_map = M::build(&dataset.entries, node_capacity);
            assert!(removals
                .iter()
                .all(|(key, value)| validation_map.remove_pair(key, value).is_some() == hit));
            (dataset, removals)
        });

        b.iter_custom(|iterations| {
            let mut total_elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let map = M::build(&dataset.entries, node_capacity);

                let start = Instant::now();
                for (key, value) in black_box(removals.as_slice()) {
                    black_box(map.remove_pair(black_box(key), black_box(value)));
                }
                total_elapsed += start.elapsed();

                let expected_len = pair_count - usize::from(hit) * removals.len();
                assert_eq!(map.len(), expected_len);
                assert!(removals.iter().all(|(key, value)| !map.contains_pair(key, value)));
            }

            total_elapsed / removals.len() as u32
        });
    });
}

fn bench_insert_scenario_for<V>(c: &mut Criterion, fanout: MultiMapFanout, kind: MultiMapInsertionKind)
where
    V: BenchMultiMapValue + Send + Sync,
{
    let mut group = c.benchmark_group(format!(
        "concurrent_multimap_v2/insert/{}/{}/{}",
        kind.id(),
        V::ID,
        fanout.id()
    ));
    group.throughput(Throughput::Elements(1));

    for pair_count in SET_SIZES {
        for node_capacity in NODE_CAPACITIES {
            bench_insert_case::<V, RandomMultiMap<V>>(&mut group, pair_count, fanout, node_capacity, kind);
            bench_insert_case::<V, OrderedMultiMap<V>>(&mut group, pair_count, fanout, node_capacity, kind);
        }
    }

    group.finish();
}

pub fn bench_insert(c: &mut Criterion) {
    for fanout in MultiMapFanout::ALL {
        for kind in [MultiMapInsertionKind::New, MultiMapInsertionKind::Existing] {
            bench_insert_scenario_for::<u64>(c, fanout, kind);
            bench_insert_scenario_for::<LargeValue>(c, fanout, kind);
        }
    }
}

fn bench_get_scenario_for<V>(c: &mut Criterion, fanout: MultiMapFanout, hit: bool)
where
    V: BenchMultiMapValue + Send + Sync,
{
    let outcome = if hit { "hit" } else { "miss" };
    let mut group = c.benchmark_group(format!(
        "concurrent_multimap_v2/get/{outcome}/{}/{}",
        V::ID,
        fanout.id()
    ));
    group.throughput(Throughput::Elements(1));

    for pair_count in SET_SIZES {
        for node_capacity in NODE_CAPACITIES {
            bench_get_case::<V, RandomMultiMap<V>>(&mut group, pair_count, fanout, node_capacity, hit);
            bench_get_case::<V, OrderedMultiMap<V>>(&mut group, pair_count, fanout, node_capacity, hit);
        }
    }

    group.finish();
}

pub fn bench_get(c: &mut Criterion) {
    for fanout in MultiMapFanout::ALL {
        for hit in [true, false] {
            bench_get_scenario_for::<u64>(c, fanout, hit);
            bench_get_scenario_for::<LargeValue>(c, fanout, hit);
        }
    }
}

fn bench_remove_pair_scenario_for<V>(c: &mut Criterion, fanout: MultiMapFanout, hit: bool)
where
    V: BenchMultiMapValue + Send + Sync,
{
    let outcome = if hit { "hit" } else { "miss" };
    let mut group = c.benchmark_group(format!(
        "concurrent_multimap_v2/remove_pair/{outcome}/{}/{}",
        V::ID,
        fanout.id()
    ));
    group.throughput(Throughput::Elements(1));

    for pair_count in SET_SIZES {
        for node_capacity in NODE_CAPACITIES {
            bench_remove_pair_case::<V, RandomMultiMap<V>>(&mut group, pair_count, fanout, node_capacity, hit);
            bench_remove_pair_case::<V, OrderedMultiMap<V>>(&mut group, pair_count, fanout, node_capacity, hit);
        }
    }

    group.finish();
}

pub fn bench_remove_pair(c: &mut Criterion) {
    for fanout in MultiMapFanout::ALL {
        for hit in [true, false] {
            bench_remove_pair_scenario_for::<u64>(c, fanout, hit);
            bench_remove_pair_scenario_for::<LargeValue>(c, fanout, hit);
        }
    }
}

fn bench_multithreaded_scenario_for<V>(c: &mut Criterion, scenario: MultiMapScenario, fanout: MultiMapFanout)
where
    V: BenchMultiMapValue + Send + Sync,
{
    let mut group = c.benchmark_group(format!(
        "concurrent_multimap_v2/multithreaded/{}/{}/{}",
        scenario.id(),
        V::ID,
        fanout.id()
    ));
    group.throughput(Throughput::Elements(scenario.operation_count() as u64));
    group.sampling_mode(SamplingMode::Flat);

    for pair_count in SET_SIZES {
        for thread_count in thread_counts() {
            for node_capacity in NODE_CAPACITIES {
                bench_multithreaded_case::<V, RandomMultiMap<V>>(
                    &mut group,
                    scenario,
                    pair_count,
                    fanout,
                    node_capacity,
                    thread_count,
                );
                // TODO: Re-enable dense ordered mixed benchmarks after
                // https://github.com/lucidarium-systems/indexset/issues/68 is fixed.
                if fanout != MultiMapFanout::Dense
                    || !matches!(
                        scenario,
                        MultiMapScenario::MixedRead90Write10 | MultiMapScenario::MixedRead50Write50
                    )
                {
                    bench_multithreaded_case::<V, OrderedMultiMap<V>>(
                        &mut group,
                        scenario,
                        pair_count,
                        fanout,
                        node_capacity,
                        thread_count,
                    );
                }
            }
        }
    }

    group.finish();
}

pub fn bench_multithreaded(c: &mut Criterion) {
    for scenario in MultiMapScenario::ALL {
        for fanout in MultiMapFanout::ALL {
            bench_multithreaded_scenario_for::<u64>(c, scenario, fanout);
            bench_multithreaded_scenario_for::<LargeValue>(c, scenario, fanout);
        }
    }
}
