#[path = "single_threaded/btree_map.rs"]
mod btree_map;
#[path = "single_threaded/btree_set.rs"]
mod btree_set;
#[path = "single_threaded/std_map.rs"]
mod std_map;
#[path = "single_threaded/std_set.rs"]
mod std_set;
#[path = "single_threaded/value_generator.rs"]
mod value_generator;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::time::Duration;
use value_generator::{
    BenchMapValue, BenchValue, LargeMapValue, LargeRecord, MapInsertionKind, ValueGenerator, NODE_CAPACITIES,
    RANGE_LEN, SET_SIZES,
};

const INSERT_ONE_BATCH_SIZE: usize = 128;
const INSERT_BATCH_COUNT: usize = 1_024;

fn bench_insert_batch_scenario_for<T: BenchValue>(
    c: &mut Criterion,
    scenario: &str,
    make_insertions: impl Fn(&ValueGenerator) -> Vec<T>,
) {
    let mut group = c.benchmark_group(format!("single_set/insert_batch/{}/{scenario}", T::ID));

    for set_size in SET_SIZES {
        let generator = ValueGenerator::new(set_size);
        let base_values = generator.base_values::<T>();
        let insertions = make_insertions(&generator);
        group.throughput(Throughput::Elements(INSERT_BATCH_COUNT as u64));
        std_set::bench_insert_batch(&mut group, set_size, INSERT_BATCH_COUNT, &base_values, &insertions);
        for node_capacity in NODE_CAPACITIES {
            btree_set::bench_insert_batch(&mut group, set_size, node_capacity, &base_values, &insertions);
        }
    }

    group.finish();
}

fn bench_insert_batch(c: &mut Criterion) {
    bench_insert_batch_scenario_for::<u64>(c, "regular", |generator| {
        generator.regular_insertion_batch(INSERT_BATCH_COUNT)
    });
    bench_insert_batch_scenario_for::<u64>(c, "90_percent_duplicates", |generator| {
        generator.duplicate_heavy_insertion_batch(INSERT_BATCH_COUNT)
    });
    bench_insert_batch_scenario_for::<LargeRecord>(c, "regular", |generator| {
        generator.regular_insertion_batch(INSERT_BATCH_COUNT)
    });
    bench_insert_batch_scenario_for::<LargeRecord>(c, "90_percent_duplicates", |generator| {
        generator.duplicate_heavy_insertion_batch(INSERT_BATCH_COUNT)
    });
}

fn bench_insert_one_for<T: BenchValue>(c: &mut Criterion) {
    let mut group = c.benchmark_group(format!("single_set/insert_one/{}", T::ID));
    group.throughput(Throughput::Elements(1));

    for set_size in SET_SIZES {
        let generator = ValueGenerator::new(set_size);
        let base_values = generator.base_values::<T>();
        let insertions = generator.regular_insertion_batch::<T>(INSERT_ONE_BATCH_SIZE);
        std_set::bench_insert_one(&mut group, set_size, &base_values, &insertions);
        for node_capacity in NODE_CAPACITIES {
            btree_set::bench_insert_one(&mut group, set_size, node_capacity, &base_values, &insertions);
        }
    }

    group.finish();
}

fn bench_insert_one(c: &mut Criterion) {
    bench_insert_one_for::<u64>(c);
    bench_insert_one_for::<LargeRecord>(c);
}

fn bench_contains_for<T: BenchValue>(c: &mut Criterion) {
    for hit in [true, false] {
        let outcome = if hit { "hit" } else { "miss" };
        let mut group = c.benchmark_group(format!("single_set/contains/{}/{outcome}", T::ID));
        group.throughput(Throughput::Elements(1));

        for set_size in SET_SIZES {
            let generator = ValueGenerator::new(set_size);
            let input = generator.base_values::<T>();
            let queries = if hit {
                generator.hit_keys()
            } else {
                generator.miss_keys()
            };
            std_set::bench_contains(&mut group, set_size, &input, &queries);
            for node_capacity in NODE_CAPACITIES {
                btree_set::bench_contains(&mut group, set_size, node_capacity, &input, &queries);
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
        let generator = ValueGenerator::new(set_size);
        let input = generator.base_values::<T>();
        let keys = generator.hit_keys();
        std_set::bench_remove(&mut group, set_size, &input, &keys);
        for node_capacity in NODE_CAPACITIES {
            btree_set::bench_remove(&mut group, set_size, node_capacity, &input, &keys);
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
        let generator = ValueGenerator::new(set_size);
        let input = generator.base_values::<T>();
        let indices = generator.random_indices();
        for node_capacity in NODE_CAPACITIES {
            btree_set::bench_get_index(&mut group, set_size, node_capacity, &input, &indices);
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
        let input = ValueGenerator::new(set_size).base_values::<T>();
        full_group.throughput(Throughput::Elements(set_size as u64));
        std_set::bench_traversal(&mut full_group, set_size, &input);
        for node_capacity in NODE_CAPACITIES {
            btree_set::bench_traversal(&mut full_group, set_size, node_capacity, &input);
        }
    }
    full_group.finish();

    let mut range_group = c.benchmark_group(format!("single_set/traversal/{}/range_128", T::ID));
    range_group.throughput(Throughput::Elements(RANGE_LEN as u64));
    for set_size in SET_SIZES {
        let input = ValueGenerator::new(set_size).base_values::<T>();
        std_set::bench_range(&mut range_group, set_size, &input);
        for node_capacity in NODE_CAPACITIES {
            btree_set::bench_range(&mut range_group, set_size, node_capacity, &input);
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
        std_map::bench_insert_batch::<V>(&mut group, map_size, INSERT_BATCH_COUNT, kind);
        for node_capacity in NODE_CAPACITIES {
            btree_map::bench_insert_batch::<V>(&mut group, map_size, node_capacity, INSERT_BATCH_COUNT, kind);
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
        std_map::bench_insert_one::<V>(&mut group, map_size, INSERT_ONE_BATCH_SIZE, kind);
        for node_capacity in NODE_CAPACITIES {
            btree_map::bench_insert_one::<V>(&mut group, map_size, node_capacity, INSERT_ONE_BATCH_SIZE, kind);
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
            std_map::bench_get::<V>(&mut group, map_size, hit);
            for node_capacity in NODE_CAPACITIES {
                btree_map::bench_get::<V>(&mut group, map_size, node_capacity, hit);
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
        std_map::bench_remove::<V>(&mut group, map_size);
        for node_capacity in NODE_CAPACITIES {
            btree_map::bench_remove::<V>(&mut group, map_size, node_capacity);
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
        std_map::bench_traversal::<V>(&mut full_group, map_size);
        for node_capacity in NODE_CAPACITIES {
            btree_map::bench_traversal::<V>(&mut full_group, map_size, node_capacity);
        }
    }
    full_group.finish();

    let mut range_group = c.benchmark_group(format!("single_map/traversal/{}/range_128", V::ID));
    range_group.throughput(Throughput::Elements(RANGE_LEN as u64));
    for map_size in SET_SIZES {
        std_map::bench_range::<V>(&mut range_group, map_size);
        for node_capacity in NODE_CAPACITIES {
            btree_map::bench_range::<V>(&mut range_group, map_size, node_capacity);
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
    targets = bench_insert_batch, bench_insert_one, bench_contains, bench_remove, bench_get_index, bench_traversal,
        bench_map_insert_batch, bench_map_insert_one, bench_map_get, bench_map_remove, bench_map_traversal
}
criterion_main!(benches);
