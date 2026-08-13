use super::value_generator::{BenchMapValue, MapInsertionKind, ValueGenerator, RANGE_LEN};
use criterion::{black_box, measurement::WallTime, BatchSize, BenchmarkGroup, BenchmarkId};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

fn build<V: BenchMapValue>(entries: &[(u64, V)]) -> BTreeMap<u64, V> {
    entries.iter().cloned().collect()
}

pub fn bench_insert_batch<V: BenchMapValue>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    map_size: usize,
    insert_count: usize,
    kind: MapInsertionKind,
) {
    group.bench_function(BenchmarkId::new(format!("std_batch_{insert_count}"), map_size), |b| {
        let generator = ValueGenerator::new(map_size);
        let base_entries = generator.map_base_entries::<V>();
        let insertions = generator.map_insertions::<V>(insert_count, kind);

        b.iter_batched_ref(
            || (build(&base_entries), insertions.clone()),
            |(map, insertion_batch)| {
                for (key, value) in insertion_batch.drain(..) {
                    black_box(map.insert(black_box(key), black_box(value)));
                }
            },
            BatchSize::PerIteration,
        );
    });
}

pub fn bench_insert_one<V: BenchMapValue>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    map_size: usize,
    operation_count: usize,
    kind: MapInsertionKind,
) {
    group.bench_function(BenchmarkId::new("std", map_size), |b| {
        let generator = ValueGenerator::new(map_size);
        let base_entries = generator.map_base_entries::<V>();
        let insertions = generator.map_insertions::<V>(operation_count, kind);
        let mut next_insertion = 0;

        b.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let insertion_batch = (0..insertions.len())
                    .map(|_| {
                        let insertion = insertions[next_insertion % insertions.len()].clone();
                        next_insertion += 1;
                        insertion
                    })
                    .collect::<Vec<_>>();
                let mut map = build(&base_entries);

                let start = Instant::now();
                for (key, value) in insertion_batch {
                    black_box(map.insert(black_box(key), black_box(value)));
                }
                elapsed += start.elapsed();
            }

            elapsed / insertions.len() as u32
        });
    });
}

pub fn bench_get<V: BenchMapValue>(group: &mut BenchmarkGroup<'_, WallTime>, map_size: usize, hit: bool) {
    let mut fixture = None;
    group.bench_function(BenchmarkId::new("std", map_size), |b| {
        let (map, queries) = fixture.get_or_insert_with(|| {
            let generator = ValueGenerator::new(map_size);
            let map = build(&generator.map_base_entries::<V>());
            let queries = if hit {
                generator.hit_keys()
            } else {
                generator.miss_keys()
            };

            (map, queries)
        });

        b.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let start = Instant::now();
                for key in black_box(queries.as_slice()) {
                    black_box(map.get(black_box(key)));
                }
                elapsed += start.elapsed();
            }

            elapsed / queries.len() as u32
        });
    });
}

pub fn bench_remove<V: BenchMapValue>(group: &mut BenchmarkGroup<'_, WallTime>, map_size: usize) {
    group.bench_function(BenchmarkId::new("std", map_size), |b| {
        let generator = ValueGenerator::new(map_size);
        let base_entries = generator.map_base_entries::<V>();
        let keys = generator.hit_keys();

        b.iter_custom(|iterations| {
            let mut elapsed = Duration::ZERO;

            for _ in 0..iterations {
                let mut map = build(&base_entries);

                let start = Instant::now();
                for key in black_box(&keys) {
                    black_box(map.remove(black_box(key)));
                }
                elapsed += start.elapsed();
            }

            elapsed / keys.len() as u32
        });
    });
}

pub fn bench_traversal<V: BenchMapValue>(group: &mut BenchmarkGroup<'_, WallTime>, map_size: usize) {
    let mut map = None;
    group.bench_function(BenchmarkId::new("std", map_size), |b| {
        let map = map.get_or_insert_with(|| {
            let generator = ValueGenerator::new(map_size);
            build(&generator.map_base_entries::<V>())
        });

        b.iter(|| {
            black_box(map.iter().fold(0_u64, |checksum, (key, value)| {
                checksum.wrapping_add(*key).wrapping_add(value.checksum())
            }))
        });
    });
}

pub fn bench_range<V: BenchMapValue>(group: &mut BenchmarkGroup<'_, WallTime>, map_size: usize) {
    let start = map_size as u64;
    let end = start + (RANGE_LEN * 2) as u64;
    let mut map = None;
    group.bench_function(BenchmarkId::new("std", map_size), |b| {
        let map = map.get_or_insert_with(|| {
            let generator = ValueGenerator::new(map_size);
            build(&generator.map_base_entries::<V>())
        });

        b.iter(|| {
            black_box(map.range(start..end).fold(0_u64, |checksum, (key, value)| {
                checksum.wrapping_add(*key).wrapping_add(value.checksum())
            }))
        });
    });
}
