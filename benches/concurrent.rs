#[path = "concurrent/set.rs"]
mod set;
#[allow(dead_code)]
#[path = "value_generator.rs"]
mod value_generator;
#[path = "concurrent/workload.rs"]
mod workload;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use set::{ConcurrentSet, MutexIndexSet, MutexStdBTreeSet};
use std::fmt::Debug;
use std::time::Duration;
use value_generator::{
    BenchValue, LargeRecord, ValueGenerator, DEFAULT_NODE_CAPACITY, NODE_CAPACITIES, RANGE_LEN, SET_SIZES,
    SINGLE_OPERATION_BATCH_SIZE,
};
use workload::{thread_counts, Scenario};

fn bench_insert_one_scenario_for<T>(
    c: &mut Criterion,
    scenario: &str,
    expected_new_count: usize,
    make_insertions: impl Fn(&ValueGenerator) -> Vec<T> + Copy + 'static,
) where
    T: BenchValue + Debug + Send + Sync,
{
    let mut group = c.benchmark_group(format!("concurrent_set_v3/{scenario}/{}", T::ID));
    group.throughput(Throughput::Elements(1));

    for set_size in SET_SIZES {
        for node_capacity in NODE_CAPACITIES {
            set::bench_insert_one::<T, ConcurrentSet<T>, _>(
                &mut group,
                set_size,
                node_capacity,
                expected_new_count,
                make_insertions,
            );
            set::bench_insert_one::<T, MutexIndexSet<T>, _>(
                &mut group,
                set_size,
                node_capacity,
                expected_new_count,
                make_insertions,
            );
        }
        set::bench_insert_one::<T, MutexStdBTreeSet<T>, _>(
            &mut group,
            set_size,
            DEFAULT_NODE_CAPACITY,
            expected_new_count,
            make_insertions,
        );
    }

    group.finish();
}

fn bench_insert_one(c: &mut Criterion) {
    bench_insert_one_scenario_for::<u64>(c, "insert_one", SINGLE_OPERATION_BATCH_SIZE, |generator| {
        generator.regular_insertion_batch(SINGLE_OPERATION_BATCH_SIZE)
    });
    bench_insert_one_scenario_for::<LargeRecord>(c, "insert_one", SINGLE_OPERATION_BATCH_SIZE, |generator| {
        generator.regular_insertion_batch(SINGLE_OPERATION_BATCH_SIZE)
    });
}

fn bench_insert_one_90_duplicates(c: &mut Criterion) {
    let expected_new_count = SINGLE_OPERATION_BATCH_SIZE / 10;
    bench_insert_one_scenario_for::<u64>(c, "insert_one_90_duplicates", expected_new_count, |generator| {
        generator.duplicate_heavy_insertion_batch(SINGLE_OPERATION_BATCH_SIZE)
    });
    bench_insert_one_scenario_for::<LargeRecord>(c, "insert_one_90_duplicates", expected_new_count, |generator| {
        generator.duplicate_heavy_insertion_batch(SINGLE_OPERATION_BATCH_SIZE)
    });
}

fn bench_contains_scenario_for<T>(c: &mut Criterion, hit: bool)
where
    T: BenchValue + Debug + Send + Sync,
{
    let outcome = if hit { "hit" } else { "miss" };
    let mut group = c.benchmark_group(format!("concurrent_set_v3/contains/{outcome}/{}", T::ID));
    group.throughput(Throughput::Elements(1));

    for set_size in SET_SIZES {
        for node_capacity in NODE_CAPACITIES {
            set::bench_contains::<T, ConcurrentSet<T>>(&mut group, set_size, node_capacity, hit);
            set::bench_contains::<T, MutexIndexSet<T>>(&mut group, set_size, node_capacity, hit);
        }
        set::bench_contains::<T, MutexStdBTreeSet<T>>(&mut group, set_size, DEFAULT_NODE_CAPACITY, hit);
    }

    group.finish();
}

fn bench_contains(c: &mut Criterion) {
    for hit in [true, false] {
        bench_contains_scenario_for::<u64>(c, hit);
        bench_contains_scenario_for::<LargeRecord>(c, hit);
    }
}

fn bench_remove_scenario_for<T>(c: &mut Criterion, hit: bool)
where
    T: BenchValue + Debug + Send + Sync,
{
    let outcome = if hit { "hit" } else { "miss" };
    let mut group = c.benchmark_group(format!("concurrent_set_v3/remove/{outcome}/{}", T::ID));
    group.throughput(Throughput::Elements(1));

    for set_size in SET_SIZES {
        for node_capacity in NODE_CAPACITIES {
            set::bench_remove::<T, ConcurrentSet<T>>(&mut group, set_size, node_capacity, hit);
            set::bench_remove::<T, MutexIndexSet<T>>(&mut group, set_size, node_capacity, hit);
        }
        set::bench_remove::<T, MutexStdBTreeSet<T>>(&mut group, set_size, DEFAULT_NODE_CAPACITY, hit);
    }

    group.finish();
}

fn bench_remove(c: &mut Criterion) {
    for hit in [true, false] {
        bench_remove_scenario_for::<u64>(c, hit);
        bench_remove_scenario_for::<LargeRecord>(c, hit);
    }
}

fn bench_range_for<T>(c: &mut Criterion)
where
    T: BenchValue + Debug + Send + Sync,
{
    let mut group = c.benchmark_group(format!("concurrent_set_v3/range/128/{}", T::ID));
    group.throughput(Throughput::Elements(RANGE_LEN as u64));

    for set_size in SET_SIZES {
        for node_capacity in NODE_CAPACITIES {
            set::bench_range::<T, ConcurrentSet<T>>(&mut group, set_size, node_capacity);
            set::bench_range::<T, MutexIndexSet<T>>(&mut group, set_size, node_capacity);
        }
        set::bench_range::<T, MutexStdBTreeSet<T>>(&mut group, set_size, DEFAULT_NODE_CAPACITY);
    }

    group.finish();
}

fn bench_range(c: &mut Criterion) {
    bench_range_for::<u64>(c);
    bench_range_for::<LargeRecord>(c);
}

fn bench_multithreaded_scenario_for<T>(c: &mut Criterion, scenario: Scenario)
where
    T: BenchValue + Debug + Send + Sync,
{
    let mut group = c.benchmark_group(format!("concurrent_set_v3/multithreaded/{}/{}", scenario.id(), T::ID));
    group.throughput(Throughput::Elements(scenario.operation_count() as u64));
    group.sampling_mode(SamplingMode::Flat);

    for set_size in SET_SIZES {
        for thread_count in thread_counts() {
            for node_capacity in NODE_CAPACITIES {
                set::bench_multithreaded_case::<T, ConcurrentSet<T>>(
                    &mut group,
                    scenario,
                    set_size,
                    node_capacity,
                    thread_count,
                );
                set::bench_multithreaded_case::<T, MutexIndexSet<T>>(
                    &mut group,
                    scenario,
                    set_size,
                    node_capacity,
                    thread_count,
                );
            }
            set::bench_multithreaded_case::<T, MutexStdBTreeSet<T>>(
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

fn bench_multithreaded(c: &mut Criterion) {
    for scenario in Scenario::ALL {
        bench_multithreaded_scenario_for::<u64>(c, scenario);
        bench_multithreaded_scenario_for::<LargeRecord>(c, scenario);
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
    targets = bench_insert_one, bench_insert_one_90_duplicates, bench_contains, bench_remove, bench_range,
        bench_multithreaded
}
criterion_main!(benches);
