#[cfg(not(target_pointer_width = "64"))]
compile_error!("the comparison benchmark requires a 64-bit target for CongeeRaw<usize, usize>");

#[path = "comparison/map.rs"]
mod map;
#[allow(dead_code)]
#[path = "value_generator.rs"]
mod value_generator;
#[allow(dead_code)]
#[path = "concurrent/workload.rs"]
mod workload;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use map::{ArcticMap, CongeeMap, IndexsetMap, WorkTablesIndexMap};
use std::time::Duration;
use value_generator::{MapInsertionKind, DEFAULT_NODE_CAPACITY, SET_SIZES};
use workload::{thread_counts, MapScenario};

fn bench_insert_one_scenario(c: &mut Criterion, scenario: &str, kind: MapInsertionKind) {
    let mut group = c.benchmark_group(format!("comparison_map_v1/insert_one/{scenario}"));
    group.throughput(Throughput::Elements(1));
    group.sample_size(20);

    for map_size in SET_SIZES {
        map::bench_insert_one::<IndexsetMap>(&mut group, map_size, kind);
        map::bench_insert_one::<WorkTablesIndexMap>(&mut group, map_size, kind);
        map::bench_insert_one::<ArcticMap>(&mut group, map_size, kind);
        map::bench_insert_one::<CongeeMap>(&mut group, map_size, kind);
    }

    group.finish();
}

fn bench_insert_one(c: &mut Criterion) {
    bench_insert_one_scenario(c, "new", MapInsertionKind::New);
    bench_insert_one_scenario(c, "update", MapInsertionKind::Update);
}

fn bench_scenario(c: &mut Criterion, scenario: MapScenario) {
    let mut group = c.benchmark_group(format!("comparison_map_v1/{}", scenario.id()));
    group.throughput(Throughput::Elements(scenario.throughput_elements() as u64));
    group.sampling_mode(SamplingMode::Flat);

    for map_size in SET_SIZES {
        for thread_count in thread_counts() {
            map::bench_parallel_case::<IndexsetMap>(
                &mut group,
                scenario,
                map_size,
                DEFAULT_NODE_CAPACITY,
                thread_count,
            );
            map::bench_parallel_case::<WorkTablesIndexMap>(
                &mut group,
                scenario,
                map_size,
                DEFAULT_NODE_CAPACITY,
                thread_count,
            );
            map::bench_parallel_case::<ArcticMap>(&mut group, scenario, map_size, DEFAULT_NODE_CAPACITY, thread_count);
            map::bench_parallel_case::<CongeeMap>(&mut group, scenario, map_size, DEFAULT_NODE_CAPACITY, thread_count);
        }
    }

    group.finish();
}

fn bench_parallel(c: &mut Criterion) {
    for scenario in MapScenario::ALL {
        bench_scenario(c, scenario);
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
    targets = bench_insert_one, bench_parallel
}
criterion_main!(benches);
