#[path = "concurrent/map.rs"]
mod map;
#[cfg(feature = "multimap")]
#[path = "concurrent/multimap.rs"]
mod multimap;
#[path = "concurrent/set.rs"]
mod set;
#[allow(dead_code)]
#[path = "value_generator.rs"]
mod value_generator;
#[path = "concurrent/workload.rs"]
mod workload;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use map::{ConcurrentMap, MutexIndexMap, MutexStdBTreeMap};
use set::{ConcurrentSet, MutexIndexSet, MutexStdBTreeSet};
use std::fmt::Debug;
use std::time::Duration;
use value_generator::{
    BenchMapValue, BenchValue, LargeMapValue, LargeRecord, MapInsertionKind, DEFAULT_NODE_CAPACITY, NODE_CAPACITIES,
    SET_SIZES,
};
use workload::{maximum_thread_count, thread_counts, MapScenario, Scenario};

const CAPACITY_SWEEP_SIZES: [usize; 2] = [100_000, 1_000_000];

fn bench_insert_one_for<T>(c: &mut Criterion)
where
    T: BenchValue + Debug + Send + Sync,
{
    let mut group = c.benchmark_group(format!("concurrent_set_v1/insert_one/{}", T::ID));
    group.throughput(Throughput::Elements(1));
    group.sample_size(20);

    for set_size in SET_SIZES {
        for node_capacity in NODE_CAPACITIES {
            set::bench_insert_one::<T, ConcurrentSet<T>>(&mut group, set_size, node_capacity);
            set::bench_insert_one::<T, MutexIndexSet<T>>(&mut group, set_size, node_capacity);
        }
        set::bench_insert_one::<T, MutexStdBTreeSet<T>>(&mut group, set_size, DEFAULT_NODE_CAPACITY);
    }

    group.finish();
}

fn bench_insert_one(c: &mut Criterion) {
    bench_insert_one_for::<u64>(c);
    bench_insert_one_for::<LargeRecord>(c);
}

fn bench_scenario_for<T>(c: &mut Criterion, scenario: Scenario)
where
    T: BenchValue + Debug + Send + Sync,
{
    let mut group = c.benchmark_group(format!("concurrent_set_v2/{}/{}", scenario.id(), T::ID));
    group.throughput(Throughput::Elements(scenario.throughput_elements() as u64));
    group.sampling_mode(SamplingMode::Flat);

    for set_size in SET_SIZES {
        for thread_count in thread_counts() {
            set::bench_parallel_case::<T, ConcurrentSet<T>>(
                &mut group,
                scenario,
                set_size,
                DEFAULT_NODE_CAPACITY,
                thread_count,
            );
            set::bench_parallel_case::<T, MutexIndexSet<T>>(
                &mut group,
                scenario,
                set_size,
                DEFAULT_NODE_CAPACITY,
                thread_count,
            );
            set::bench_parallel_case::<T, MutexStdBTreeSet<T>>(
                &mut group,
                scenario,
                set_size,
                DEFAULT_NODE_CAPACITY,
                thread_count,
            );
        }
    }

    group.finish();
}

fn bench_parallel(c: &mut Criterion) {
    for scenario in Scenario::ALL {
        bench_scenario_for::<u64>(c, scenario);
        bench_scenario_for::<LargeRecord>(c, scenario);
    }
}

fn bench_capacity_scenario_for<T>(c: &mut Criterion, scenario: Scenario)
where
    T: BenchValue + Debug + Send + Sync,
{
    let mut group = c.benchmark_group(format!("concurrent_set_v2/capacity/{}/{}", scenario.id(), T::ID));
    group.throughput(Throughput::Elements(scenario.throughput_elements() as u64));
    group.sampling_mode(SamplingMode::Flat);

    let thread_count = maximum_thread_count();
    for set_size in CAPACITY_SWEEP_SIZES {
        for node_capacity in NODE_CAPACITIES {
            if node_capacity == DEFAULT_NODE_CAPACITY {
                continue;
            }

            set::bench_parallel_case::<T, ConcurrentSet<T>>(
                &mut group,
                scenario,
                set_size,
                node_capacity,
                thread_count,
            );
            set::bench_parallel_case::<T, MutexIndexSet<T>>(
                &mut group,
                scenario,
                set_size,
                node_capacity,
                thread_count,
            );
        }
    }

    group.finish();
}

fn bench_capacity_sweep(c: &mut Criterion) {
    for scenario in Scenario::CAPACITY_SWEEP {
        bench_capacity_scenario_for::<u64>(c, scenario);
        bench_capacity_scenario_for::<LargeRecord>(c, scenario);
    }
}

fn bench_map_insert_one_scenario_for<V: BenchMapValue + Send + Sync>(
    c: &mut Criterion,
    scenario: &str,
    kind: MapInsertionKind,
) {
    let mut group = c.benchmark_group(format!("concurrent_map_v1/insert_one/{}/{scenario}", V::ID));
    group.throughput(Throughput::Elements(1));
    group.sample_size(20);

    for map_size in SET_SIZES {
        for node_capacity in NODE_CAPACITIES {
            map::bench_insert_one::<V, ConcurrentMap<V>>(&mut group, map_size, node_capacity, kind);
            map::bench_insert_one::<V, MutexIndexMap<V>>(&mut group, map_size, node_capacity, kind);
        }
        map::bench_insert_one::<V, MutexStdBTreeMap<V>>(&mut group, map_size, DEFAULT_NODE_CAPACITY, kind);
    }

    group.finish();
}

fn bench_map_insert_one(c: &mut Criterion) {
    bench_map_insert_one_scenario_for::<u64>(c, "new", MapInsertionKind::New);
    bench_map_insert_one_scenario_for::<u64>(c, "update", MapInsertionKind::Update);
    bench_map_insert_one_scenario_for::<LargeMapValue>(c, "new", MapInsertionKind::New);
    bench_map_insert_one_scenario_for::<LargeMapValue>(c, "update", MapInsertionKind::Update);
}

fn bench_map_scenario_for<V>(c: &mut Criterion, scenario: MapScenario)
where
    V: BenchMapValue + Send + Sync,
{
    let mut group = c.benchmark_group(format!("concurrent_map_v2/{}/{}", scenario.id(), V::ID));
    group.throughput(Throughput::Elements(scenario.throughput_elements() as u64));
    group.sampling_mode(SamplingMode::Flat);

    for map_size in SET_SIZES {
        for thread_count in thread_counts() {
            map::bench_parallel_case::<V, ConcurrentMap<V>>(
                &mut group,
                scenario,
                map_size,
                DEFAULT_NODE_CAPACITY,
                thread_count,
            );
            map::bench_parallel_case::<V, MutexIndexMap<V>>(
                &mut group,
                scenario,
                map_size,
                DEFAULT_NODE_CAPACITY,
                thread_count,
            );
            map::bench_parallel_case::<V, MutexStdBTreeMap<V>>(
                &mut group,
                scenario,
                map_size,
                DEFAULT_NODE_CAPACITY,
                thread_count,
            );
        }
    }

    group.finish();
}

fn bench_map_parallel(c: &mut Criterion) {
    for scenario in MapScenario::ALL {
        bench_map_scenario_for::<u64>(c, scenario);
        bench_map_scenario_for::<LargeMapValue>(c, scenario);
    }
}

fn bench_map_capacity_scenario_for<V>(c: &mut Criterion, scenario: MapScenario)
where
    V: BenchMapValue + Send + Sync,
{
    let mut group = c.benchmark_group(format!("concurrent_map_v2/capacity/{}/{}", scenario.id(), V::ID));
    group.throughput(Throughput::Elements(scenario.throughput_elements() as u64));
    group.sampling_mode(SamplingMode::Flat);

    let thread_count = maximum_thread_count();
    for map_size in CAPACITY_SWEEP_SIZES {
        for node_capacity in NODE_CAPACITIES {
            if node_capacity == DEFAULT_NODE_CAPACITY {
                continue;
            }

            map::bench_parallel_case::<V, ConcurrentMap<V>>(
                &mut group,
                scenario,
                map_size,
                node_capacity,
                thread_count,
            );
            map::bench_parallel_case::<V, MutexIndexMap<V>>(
                &mut group,
                scenario,
                map_size,
                node_capacity,
                thread_count,
            );
        }
    }

    group.finish();
}

fn bench_map_capacity_sweep(c: &mut Criterion) {
    for scenario in MapScenario::CAPACITY_SWEEP {
        bench_map_capacity_scenario_for::<u64>(c, scenario);
        bench_map_capacity_scenario_for::<LargeMapValue>(c, scenario);
    }
}

fn benchmark_config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_millis(300))
        .measurement_time(Duration::from_secs(1))
        .sample_size(10)
}

criterion_group! {
    name = benches;
    config = benchmark_config();
    targets = bench_insert_one, bench_parallel, bench_capacity_sweep, bench_map_insert_one, bench_map_parallel,
        bench_map_capacity_sweep
}

#[cfg(feature = "multimap")]
criterion_group! {
    name = multimap_benches;
    config = benchmark_config();
    targets = multimap::bench_insert_one, multimap::bench_parallel, multimap::bench_capacity_sweep
}

#[cfg(feature = "multimap")]
criterion_main!(benches, multimap_benches);
#[cfg(not(feature = "multimap"))]
criterion_main!(benches);
