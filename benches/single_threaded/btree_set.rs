use super::value_generator::{BenchValue, RANGE_LEN};
use criterion::{black_box, measurement::WallTime, BatchSize, BenchmarkGroup, BenchmarkId};
use indexset::BTreeSet;
use std::time::{Duration, Instant};

fn build<T: BenchValue>(values: &[T], node_capacity: usize) -> BTreeSet<T> {
    let mut set = BTreeSet::with_maximum_node_size(node_capacity);
    for value in values {
        set.insert(value.clone());
    }
    set
}

fn id(node_capacity: usize) -> String {
    format!("indexset_cap_{node_capacity}")
}

pub fn bench_insert_batch<T: BenchValue>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    set_size: usize,
    node_capacity: usize,
    base_values: &[T],
    insertions: &[T],
) {
    group.bench_function(BenchmarkId::new(id(node_capacity), set_size), |b| {
        b.iter_batched_ref(
            || (build(base_values, node_capacity), insertions.to_vec()),
            |(set, insertion_batch)| {
                for value in insertion_batch.drain(..) {
                    black_box(set.insert(black_box(value)));
                }
            },
            BatchSize::PerIteration,
        );
    });
}

pub fn bench_insert_one<T: BenchValue>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    set_size: usize,
    node_capacity: usize,
    base_values: &[T],
    insertions: &[T],
) {
    let mut next_insertion = 0;
    group.bench_function(BenchmarkId::new(id(node_capacity), set_size), |b| {
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
                let mut set = build(base_values, node_capacity);

                let start = Instant::now();
                for insertion in insertion_batch {
                    black_box(set.insert(black_box(insertion)));
                }
                elapsed += start.elapsed();
            }

            elapsed / insertions.len() as u32
        });
    });
}

pub fn bench_contains<T: BenchValue>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    set_size: usize,
    node_capacity: usize,
    values: &[T],
    queries: &[u64],
) {
    let mut set = None;
    group.bench_function(BenchmarkId::new(id(node_capacity), set_size), |b| {
        let set = set.get_or_insert_with(|| build(values, node_capacity));
        b.iter(|| {
            for key in black_box(queries) {
                black_box(set.contains(key));
            }
        });
    });
}

pub fn bench_remove<T: BenchValue>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    set_size: usize,
    node_capacity: usize,
    values: &[T],
    keys: &[u64],
) {
    group.bench_function(BenchmarkId::new(id(node_capacity), set_size), |b| {
        b.iter_batched_ref(
            || build(values, node_capacity),
            |set| {
                for key in keys {
                    black_box(set.remove(black_box(key)));
                }
            },
            BatchSize::PerIteration,
        );
    });
}

pub fn bench_traversal<T: BenchValue>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    set_size: usize,
    node_capacity: usize,
    values: &[T],
) {
    let mut set = None;
    group.bench_function(BenchmarkId::new(id(node_capacity), set_size), |b| {
        let set = set.get_or_insert_with(|| build(values, node_capacity));
        b.iter(|| {
            black_box(
                set.iter()
                    .fold(0_u64, |checksum, value| checksum.wrapping_add(value.key())),
            )
        });
    });
}

pub fn bench_range<T: BenchValue>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    set_size: usize,
    node_capacity: usize,
    values: &[T],
) {
    let start = set_size as u64;
    let end = start + (RANGE_LEN * 2) as u64;
    let mut set = None;
    group.bench_function(BenchmarkId::new(id(node_capacity), set_size), |b| {
        let set = set.get_or_insert_with(|| build(values, node_capacity));
        b.iter(|| {
            black_box(
                set.range(start..end)
                    .fold(0_u64, |checksum, value| checksum.wrapping_add(value.key())),
            )
        });
    });
}
