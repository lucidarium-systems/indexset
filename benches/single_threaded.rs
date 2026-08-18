#[path = "single_threaded/map.rs"]
mod map;
#[path = "single_threaded/set.rs"]
mod set;
#[path = "value_generator.rs"]
mod value_generator;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use map::{IndexMap, StdMap};
use set::{IndexSet, StdSet};
use std::time::Duration;
use value_generator::{
    BenchMapValue, BenchValue, LargeMapValue, LargeRecord, MapInsertionKind, ValueGenerator, DEFAULT_NODE_CAPACITY,
    NODE_CAPACITIES, RANGE_LEN, SET_SIZES, SINGLE_OPERATION_BATCH_SIZE,
};

const INSERT_BATCH_COUNT: usize = 1_024;

fn bench_insert_one_scenario_for<T: BenchValue>(
    c: &mut Criterion,
    scenario: &str,
    make_insertions: impl Fn(&ValueGenerator) -> Vec<T> + Copy + 'static,
) {
    let mut group = c.benchmark_group(format!("single_set_v2/{scenario}/{}", T::ID));
    group.throughput(Throughput::Elements(1));

    for set_size in SET_SIZES {
        set::bench_insert_one::<T, StdSet<T>, _>(&mut group, set_size, DEFAULT_NODE_CAPACITY, make_insertions);
        for node_capacity in NODE_CAPACITIES {
            set::bench_insert_one::<T, IndexSet<T>, _>(&mut group, set_size, node_capacity, make_insertions);
        }
    }

    group.finish();
}

fn bench_insert_one(c: &mut Criterion) {
    bench_insert_one_scenario_for::<u64>(c, "insert_one", |generator| {
        generator.regular_insertion_batch(SINGLE_OPERATION_BATCH_SIZE)
    });
    bench_insert_one_scenario_for::<LargeRecord>(c, "insert_one", |generator| {
        generator.regular_insertion_batch(SINGLE_OPERATION_BATCH_SIZE)
    });
}

fn bench_insert_one_90_percent_duplicates(c: &mut Criterion) {
    bench_insert_one_scenario_for::<u64>(c, "insert_one_90_percent_duplicates", |generator| {
        generator.duplicate_heavy_insertion_batch(SINGLE_OPERATION_BATCH_SIZE)
    });
    bench_insert_one_scenario_for::<LargeRecord>(c, "insert_one_90_percent_duplicates", |generator| {
        generator.duplicate_heavy_insertion_batch(SINGLE_OPERATION_BATCH_SIZE)
    });
}

fn bench_contains_for<T: BenchValue>(c: &mut Criterion) {
    for hit in [true, false] {
        let outcome = if hit { "hit" } else { "miss" };
        let mut group = c.benchmark_group(format!("single_set/contains/{}/{outcome}", T::ID));
        group.throughput(Throughput::Elements(1));

        for set_size in SET_SIZES {
            set::bench_contains::<T, StdSet<T>>(&mut group, set_size, DEFAULT_NODE_CAPACITY, hit);
            for node_capacity in NODE_CAPACITIES {
                set::bench_contains::<T, IndexSet<T>>(&mut group, set_size, node_capacity, hit);
            }
        }

        group.finish();
    }
}

fn bench_contains(c: &mut Criterion) {
    bench_contains_for::<u64>(c);
    bench_contains_for::<LargeRecord>(c);
}

fn bench_remove_for<T: BenchValue>(c: &mut Criterion) {
    let mut group = c.benchmark_group(format!("single_set/remove/{}/hit", T::ID));
    group.throughput(Throughput::Elements(1));

    for set_size in SET_SIZES {
        set::bench_remove::<T, StdSet<T>>(&mut group, set_size, DEFAULT_NODE_CAPACITY);
        for node_capacity in NODE_CAPACITIES {
            set::bench_remove::<T, IndexSet<T>>(&mut group, set_size, node_capacity);
        }
    }

    group.finish();
}

fn bench_remove(c: &mut Criterion) {
    bench_remove_for::<u64>(c);
    bench_remove_for::<LargeRecord>(c);
}

fn bench_get_index_for<T: BenchValue>(c: &mut Criterion) {
    let mut group = c.benchmark_group(format!("single_set/get_index/{}", T::ID));
    group.throughput(Throughput::Elements(1));

    for set_size in SET_SIZES {
        for node_capacity in NODE_CAPACITIES {
            set::bench_get_index::<T>(&mut group, set_size, node_capacity);
        }
    }

    group.finish();
}

fn bench_get_index(c: &mut Criterion) {
    bench_get_index_for::<u64>(c);
    bench_get_index_for::<LargeRecord>(c);
}

fn bench_traversal_for<T: BenchValue>(c: &mut Criterion) {
    let mut full_group = c.benchmark_group(format!("single_set/traversal/{}/full", T::ID));
    for set_size in SET_SIZES {
        full_group.throughput(Throughput::Elements(set_size as u64));
        set::bench_traversal::<T, StdSet<T>>(&mut full_group, set_size, DEFAULT_NODE_CAPACITY);
        for node_capacity in NODE_CAPACITIES {
            set::bench_traversal::<T, IndexSet<T>>(&mut full_group, set_size, node_capacity);
        }
    }
    full_group.finish();

    let mut range_group = c.benchmark_group(format!("single_set/traversal/{}/range_128", T::ID));
    range_group.throughput(Throughput::Elements(RANGE_LEN as u64));
    for set_size in SET_SIZES {
        set::bench_range::<T, StdSet<T>>(&mut range_group, set_size, DEFAULT_NODE_CAPACITY);
        for node_capacity in NODE_CAPACITIES {
            set::bench_range::<T, IndexSet<T>>(&mut range_group, set_size, node_capacity);
        }
    }
    range_group.finish();
}

fn bench_traversal(c: &mut Criterion) {
    bench_traversal_for::<u64>(c);
    bench_traversal_for::<LargeRecord>(c);
}

fn bench_map_insert_batch_scenario_for<V: BenchMapValue>(c: &mut Criterion, scenario: &str, kind: MapInsertionKind) {
    let mut group = c.benchmark_group(format!("single_map/insert_batch/{}/{scenario}", V::ID));
    group.throughput(Throughput::Elements(INSERT_BATCH_COUNT as u64));

    for map_size in SET_SIZES {
        map::bench_insert_batch::<V, StdMap<V>>(&mut group, map_size, DEFAULT_NODE_CAPACITY, INSERT_BATCH_COUNT, kind);
        for node_capacity in NODE_CAPACITIES {
            map::bench_insert_batch::<V, IndexMap<V>>(&mut group, map_size, node_capacity, INSERT_BATCH_COUNT, kind);
        }
    }

    group.finish();
}

fn bench_map_insert_batch(c: &mut Criterion) {
    bench_map_insert_batch_scenario_for::<u64>(c, "new", MapInsertionKind::New);
    bench_map_insert_batch_scenario_for::<u64>(c, "90_percent_updates", MapInsertionKind::UpdateHeavy);
    bench_map_insert_batch_scenario_for::<LargeMapValue>(c, "new", MapInsertionKind::New);
    bench_map_insert_batch_scenario_for::<LargeMapValue>(c, "90_percent_updates", MapInsertionKind::UpdateHeavy);
}

fn bench_map_insert_one_scenario_for<V: BenchMapValue>(c: &mut Criterion, scenario: &str, kind: MapInsertionKind) {
    let mut group = c.benchmark_group(format!("single_map/insert_one/{}/{scenario}", V::ID));
    group.throughput(Throughput::Elements(1));

    for map_size in SET_SIZES {
        map::bench_insert_one::<V, StdMap<V>>(
            &mut group,
            map_size,
            DEFAULT_NODE_CAPACITY,
            SINGLE_OPERATION_BATCH_SIZE,
            kind,
        );
        for node_capacity in NODE_CAPACITIES {
            map::bench_insert_one::<V, IndexMap<V>>(
                &mut group,
                map_size,
                node_capacity,
                SINGLE_OPERATION_BATCH_SIZE,
                kind,
            );
        }
    }

    group.finish();
}

fn bench_map_insert_one(c: &mut Criterion) {
    bench_map_insert_one_scenario_for::<u64>(c, "new", MapInsertionKind::New);
    bench_map_insert_one_scenario_for::<u64>(c, "update", MapInsertionKind::Update);
    bench_map_insert_one_scenario_for::<LargeMapValue>(c, "new", MapInsertionKind::New);
    bench_map_insert_one_scenario_for::<LargeMapValue>(c, "update", MapInsertionKind::Update);
}

fn bench_map_get_for<V: BenchMapValue>(c: &mut Criterion) {
    for hit in [true, false] {
        let outcome = if hit { "hit" } else { "miss" };
        let mut group = c.benchmark_group(format!("single_map/get/{}/{outcome}", V::ID));
        group.throughput(Throughput::Elements(1));

        for map_size in SET_SIZES {
            map::bench_get::<V, StdMap<V>>(&mut group, map_size, DEFAULT_NODE_CAPACITY, hit);
            for node_capacity in NODE_CAPACITIES {
                map::bench_get::<V, IndexMap<V>>(&mut group, map_size, node_capacity, hit);
            }
        }

        group.finish();
    }
}

fn bench_map_get(c: &mut Criterion) {
    bench_map_get_for::<u64>(c);
    bench_map_get_for::<LargeMapValue>(c);
}

fn bench_map_remove_for<V: BenchMapValue>(c: &mut Criterion) {
    let mut group = c.benchmark_group(format!("single_map/remove/{}/hit", V::ID));
    group.throughput(Throughput::Elements(1));

    for map_size in SET_SIZES {
        map::bench_remove::<V, StdMap<V>>(&mut group, map_size, DEFAULT_NODE_CAPACITY);
        for node_capacity in NODE_CAPACITIES {
            map::bench_remove::<V, IndexMap<V>>(&mut group, map_size, node_capacity);
        }
    }

    group.finish();
}

fn bench_map_remove(c: &mut Criterion) {
    bench_map_remove_for::<u64>(c);
    bench_map_remove_for::<LargeMapValue>(c);
}

fn bench_map_traversal_for<V: BenchMapValue>(c: &mut Criterion) {
    let mut full_group = c.benchmark_group(format!("single_map/traversal/{}/full", V::ID));
    for map_size in SET_SIZES {
        full_group.throughput(Throughput::Elements(map_size as u64));
        map::bench_traversal::<V, StdMap<V>>(&mut full_group, map_size, DEFAULT_NODE_CAPACITY);
        for node_capacity in NODE_CAPACITIES {
            map::bench_traversal::<V, IndexMap<V>>(&mut full_group, map_size, node_capacity);
        }
    }
    full_group.finish();

    let mut range_group = c.benchmark_group(format!("single_map/traversal/{}/range_128", V::ID));
    range_group.throughput(Throughput::Elements(RANGE_LEN as u64));
    for map_size in SET_SIZES {
        map::bench_range::<V, StdMap<V>>(&mut range_group, map_size, DEFAULT_NODE_CAPACITY);
        for node_capacity in NODE_CAPACITIES {
            map::bench_range::<V, IndexMap<V>>(&mut range_group, map_size, node_capacity);
        }
    }
    range_group.finish();
}

fn bench_map_traversal(c: &mut Criterion) {
    bench_map_traversal_for::<u64>(c);
    bench_map_traversal_for::<LargeMapValue>(c);
}

fn benchmark_config() -> Criterion {
    Criterion::default()
        .warm_up_time(Duration::from_millis(300))
        .measurement_time(Duration::from_secs(1))
        .sample_size(20)
}

criterion_group! {
    name = benches;
    config = benchmark_config();
    targets = bench_insert_one, bench_insert_one_90_percent_duplicates, bench_contains, bench_remove, bench_get_index,
        bench_traversal, bench_map_insert_batch, bench_map_insert_one, bench_map_get, bench_map_remove,
        bench_map_traversal
}
criterion_main!(benches);
